What setup.sh does (bullet notes)

Bootstrap & environment detection
	•	Enables strict bash (set -e) so the script stops on errors.
	•	Uses IMDSv2 to fetch:
	•	Token, instance ID, and the instance profile/role.
	•	The EC2 Name tag and other tags to derive environment flags.
	•	Sets $ENVIRONMENT (e.g., dev1, test, acpt, prod) from lifecycle/tag values.
	•	Pulls a shared commons bundle from S3 into a temp dir and sources utils.sh (helpers used later).

Per-environment config (LDAP/AD & hostnames)
	•	Switches on $ENVIRONMENT to set:
	•	Friendly instance label (e.g., atsyslab1, atsysdev1).
	•	LDAP bind username and LDAP group search filters.
	•	LDAP host:port (different in prod vs non-prod).
	•	Computes hostnames and updates /etc/hosts so DX/EEM components resolve consistently.

EEM paths, roles, and toolchain
	•	Defines S3 source for the EEM payload (cp-artifacts/.../eem).
	•	Reads the attached EEMRole from instance tags (used for auth/AWS calls).
	•	Sets working dirs:
	•	$EEM_IMAGE (e.g., EEMServer_12.0.4.0_linux.bin).
	•	$EEM_HOME, $IGAT_HOME, $EIAM_HOME locations.
	•	$JAVA_HOME path bundled with EEM.
	•	Prepares install status messages/logs to trace progress.

Helper functions
	•	setup_dsa_user(): creates/ensures the service account (e.g., dsa, fixed UID), sets shell/home; idempotent.
	•	install_eem_image(): runs the silent installer for the EEM image, tails/validates eiam-install.log, hard-fails on non-zero RC.
	•	configure_eem():
	•	Secures/patches iGateway TLS (igateway.conf: ciphers, protocol, bind addresses).
	•	Pulls secrets (bind/admin passwords) from AWS Secrets Manager and injects into EEM configs.
	•	Writes LDAP settings into server.xml, preserves backups (*.ORIG).
	•	eiam_cluster_setup(): builds a python venv, points pip to your Nexus indexes, installs boto3, and runs eiam_cluster_setup.py; fails loud on non-zero RC.

System packages & agents
	•	Installs yum prerequisites.
	•	Optionally installs/sets up sssd for directory integration.
	•	(You have a placeholder function name like install_cloudwatch_template in comments—currently not wiring monitoring; we’ll add that below.)

Systemd service deployment
	•	Removes legacy gateway/dxserver init bits as needed.
	•	Copies your unit file eem.service into /etc/systemd/system/eem.service.
	•	systemctl daemon-reload + enable eem (so it starts at boot).
	•	Orchestrates DX/iGateway restarts in the right order, runs your cluster refresh script, then final restart.
	•	Fixes ownership/permissions for log directories.

Script entrypoint
	•	Gathers commons from S3 again in main, then calls the functions in order:
	•	yum_requirements → setup_dsa_user → host/sssd → install EEM → place unit → configure EEM → start services.
	•	Writes install markers to logs so SSM/GitLab can parse success.