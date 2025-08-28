#!/usr/bin/env python3
import argparse, json, logging, os, shlex, subprocess, sys
from dataclasses import dataclass
from pathlib import Path
try:
    import pexpect
except Exception:
    print("pip install pexpect boto3", file=sys.stderr); raise

@dataclass(frozen=True)
class EnvConfig:
    name: str
    autosys_source: str
    secret_name: str
    secret_key: str

ENV_MAP = {
    "lab":  EnvConfig("lab",
        "/opt/CA/WorkloadAutomationAE/autouser.LB1/autosys.sh.LB1",
        "/atsylab1/atsylab1-rds-schedsvc01/atsylab1_dbo", "password"),
    "dev":  EnvConfig("dev",
        "/opt/CA/WorkloadAutomationAE/autouser.DV1/autosys.sh.DV1",
        "/atsydev1/atsydev1-rds-schedsvc01/atsydev1_dbo", "password"),
    "test": EnvConfig("test",
        "/opt/CA/WorkloadAutomationAE/autouser.TST/autosys.sh.TST",
        "/atsytest1/atsytest1-rds-schedsvc01/atsytest1_dbo","password"),
    "acpt": EnvConfig("acpt",
        "/opt/CA/WorkloadAutomationAE/autouser.ACPT/autosys.sh.ACPT",
        "/atsyacpt1/atsyacpt1-rds-schedsvc01/atsyacpt1_dbo","password"),
}

def run(cmd, **kw):
    logging.debug("RUN %s", cmd)
    return subprocess.run(cmd, check=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, **kw)

def curl_download(url: str, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    run(["curl","-fL","-o",str(out_path),url])
    logging.info("downloaded %s", out_path)

def extract_tgz(tgz: Path, dest: Path):
    dest.mkdir(parents=True, exist_ok=True)
    run(["tar","-zxf",str(tgz),"-C",str(dest)])
    logging.info("extracted to %s", dest)

def get_secret(secret_name: str, key: str) -> str:
    try:
        import boto3
        c = boto3.client("secretsmanager")
        r = c.get_secret_value(SecretId=secret_name)
        s = r.get("SecretString") or __import__("base64").b64decode(r["SecretBinary"]).decode()
        try: return json.loads(s).get(key, s)
        except json.JSONDecodeError: return s
    except Exception:
        r = run(["aws","secretsmanager","get-secret-value","--secret-id",secret_name,
                 "--query","SecretString","--output","text"])
        raw = r.stdout.strip()
        try: return json.loads(raw).get(key, raw)
        except json.JSONDecodeError: return raw

def run_under_autosys(source_path: str, command: str, plan: list[tuple[str,str]], timeout: int):
    bash = f'bash -lc "source {shlex.quote(source_path)} && {command}"'
    child = pexpect.spawn(bash, encoding="utf-8", timeout=timeout)
    child.logfile = sys.stdout  # stream to our logs
    for pat, send in plan:
        child.expect(pat); child.sendline(send)
    child.expect(pexpect.EOF)
    if (child.exitstatus or 0) != 0:
        raise RuntimeError(f"command failed: {command}")

def main():
    ap = argparse.ArgumentParser("Autosys DB refresh driver")
    ap.add_argument("--env", required=True, choices=ENV_MAP.keys())
    ap.add_argument("--work-dir", default="/data")
    ap.add_argument("--tar-url",
        default="https://nexusrepository.fanniemae.com/nexus/repository/autosys_aws/Scheduler/Installation/autosys24.1.0.0.0.tgz")
    ap.add_argument("--tar-name", default="autosys24.1.0.0.0.tgz")
    ap.add_argument("--extract-to", default="/")
    ap.add_argument("--autosys-source")  # override
    ap.add_argument("--secret-name"); ap.add_argument("--secret-key")
    # ----- RefreshAEDB.pl prompts (now parameterized) -----
    ap.add_argument("--perl-cmd", default="perl RefreshAEDB.pl")
    ap.add_argument("--auth-mode", default="0")  # "Database authentication mode"
    ap.add_argument("--service-id", default="DQAUTSYS1.WORLD")  # your screenshot value
    ap.add_argument("--schema-owner", default="atsyslab1_dbo")
    ap.add_argument("--jre-dir", default="/opt/CA/WorkloadAutomationAE/JRE64_WA")
    ap.add_argument("--confirm", default="y")   # "Are you sure"
    ap.add_argument("--timeout", type=int, default=3600)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    cfg = ENV_MAP[args.env]
    autosys_source = args.autosys_source or cfg.autosys_source
    secret_name = args.secret_name or cfg.secret_name
    secret_key  = args.secret_key  or cfg.secret_key

    work = Path(args.work_dir); work.mkdir(parents=True, exist_ok=True); os.chdir(work)
    tar = work / args.tar_name
    if not tar.exists(): curl_download(args.tar_url, tar)
    extract_tgz(tar, Path(args.extract_to))

    password = get_secret(secret_name, secret_key)
    logging.info("secret retrieved (len=%d)", len(password))

    # Exact interaction from your continuation image:
    plan = [
        (r"Database authentication mode.*>", args.auth_mode),
        (r"Service Identifier.*>",           args.service_id),
        (r"Schema owner name.*>",            args.schema_owner),
        (rf"{args.schema_owner} user.?s password.*>", password),
        (r"JRE Directory.*>",                args.jre_dir),
        (r"Are you sure.*>",                 args.confirm),
    ]
    run_under_autosys(autosys_source, args.perl_cmd, plan, args.timeout)
    logging.info("RefreshAEDB.pl executed successfully.")

if __name__ == "__main__":
    try: main()
    except subprocess.CalledProcessError as e:
        logging.error("FAILED (rc=%s):\n%s", e.returncode, e.stdout); sys.exit(e.returncode or 1)
    except Exception as e:
        logging.exception("fatal: %s", e); sys.exit(1)