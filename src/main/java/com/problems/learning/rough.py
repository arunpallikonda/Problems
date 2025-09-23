svc_heartbeat.sh
#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Generic systemd heartbeat sidecar.
# Logs one line per minute to syslog for a *watched* unit (service).
#
# Usage:  svc_heartbeat.sh <unit-name>
# Example: svc_heartbeat.sh eem.service
#
# Why logs? Your CloudWatch Agent already ships syslog/journal to your
# CloudWatch Log Group, and your Terraform log->metric module builds alarms.
# ------------------------------------------------------------------------------
set -euo pipefail

UNIT="${1:?usage: svc_heartbeat.sh <unit-name>}"    # e.g., "eem.service"
TAG="AUTOSYS_HEALTH"                                 # TF FilterPattern matches this
HOST="$(hostname -s)"

# Exit cleanly when systemd stops us (no zombies/orphans).
trap 'exit 0' TERM INT

while true; do
  if systemctl is-active --quiet "${UNIT}"; then
    # OK heartbeat (this is what the alarm counts)
    logger -t autosys-health "${TAG} service=${UNIT} host=${HOST} status=OK"
  else
    # Optional FAIL breadcrumb for humans; alarm triggers on *missing OK*, not FAIL.
    logger -t autosys-health "${TAG} service=${UNIT} host=${HOST} status=FAIL"
  fi
  sleep 60
done


svc-hb@.service
# ------------------------------------------------------------------------------
# Templated sidecar: one instance per watched unit.
# Start with:   systemctl start svc-hb@eem.service
# Teams don't call this directly; their .service starts it via ExecStartPost.
# ------------------------------------------------------------------------------
[Unit]
Description=Heartbeat sidecar for %i (writes to syslog for CW log alarm)
# Keep lifecycle bound to the watched unit:
BindsTo=%i
PartOf=%i
After=%i

[Service]
Type=simple
# %i expands to the instance name passed (e.g., "eem.service")
ExecStart=/usr/local/bin/svc_heartbeat.sh %i
Restart=always
RestartSec=5s
User=root
Group=root

[Install]
WantedBy=multi-user.target



ExecStartPost=/bin/systemctl start svc-hb@%N.service

# --- install shared heartbeat assets (one time per host) ---
install -m 0755 "$EEM_DIR/monitoring/svc_heartbeat.sh" /usr/local/bin/svc_heartbeat.sh
install -m 0644 "$EEM_DIR/monitoring/svc-hb@.service"  /etc/systemd/system/svc-hb@.service

# make new unit visible to systemd
systemctl daemon-reload
# no need to enable globally; each main service starts its own instance







alarm

# exact log group name already receiving syslog/journal
variable "log_group_name" { type = string }

# list the services you want alarms for (unit filenames)
variable "services" {
  type    = list(string)
  default = ["eem.service", "autoweb.service", "netagt.service"]
}

module "svc_ok_heartbeat_missing" {
  for_each = toset(var.services)

  source  = "terraform.fanniemae-org/monitoring/aws//modules/mon_aws/mon_cloudwatch_log_alarm"
  version = ">=3.0.2"

  app_shortname = var.appshortname
  AlarmId       = "SvcHeartbeatMissing-${each.value}"
  Description   = "${var.appshortname}: OK heartbeat missing for ${each.value}"
  Severity      = "MINOR"

  LogGroups      = [var.log_group_name]
  # Must exactly match what the script writes:
  FilterPatterns = ["\"AUTOSYS_HEALTH service=${each.value} status=OK\""]

  MetricNamespace   = "AppLogMetrics/${var.appshortname}"
  MetricName        = "${var.appshortname}-svc-ok-${replace(each.value, ".service", "")}"

  Statistic          = "Sum"                 # count OK heartbeats
  Period             = 60
  Threshold          = 1                     # expect >=1 OK per minute
  DatapointsToAlarm  = 1
  EvaluationPeriods  = 2                     # 2 missing minutes => ALARM
  ComparisonOperator = "LessThanThreshold"
}
