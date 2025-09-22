emm_health.sh
#!/usr/bin/env bash
# EEM health probe (log-based heartbeat for CloudWatch log alarm)
set -euo pipefail
SVC_NAME="eem.service"
TAG="AUTOSYS_HEALTH"     # keep tokens stable; TF filter matches this
SERVICE_NAME="EEM"

log() { logger -t autosys-health "$*"; }

if systemctl is-active --quiet "${SVC_NAME}"; then
  log "${TAG} service=${SERVICE_NAME} status=OK"     # <-- alarm counts these
  exit 0
else
  log "${TAG} service=${SERVICE_NAME} status=FAIL"   # for humans; alarm is “missing OK”
  exit 2
fi





eem_health.service
[Unit]
Description=EEM health probe (writes heartbeat to syslog)
Wants=eem.service
After=eem.service
ConditionPathExists=/usr/local/bin/eem_health.sh

[Service]
Type=oneshot
ExecStart=/usr/local/bin/eem_health.sh
User=root
Group=root




eem_health.timer
[Unit]
Description=Run EEM health probe every minute

[Timer]
OnBootSec=30s
OnUnitActiveSec=60s
Unit=eem_health.service
AccuracySec=5s
Persistent=true

[Install]
WantedBy=timers.target






cw_alarm


# Point this to the group that contained your CW_TEST line
variable "log_group_name" {
  type    = string
  default = "atsyslab1-dev1-ess-System"  # <-- put the exact log group name here
}

module "eem_ok_heartbeat_missing" {
  source  = "terraform.fanniemae-org/monitoring/aws//modules/mon_aws/mon_cloudwatch_log_alarm"
  version = ">=3.0.2"   # or the version your registry requires

  # org-standard fields (match your other alarms)
  app_shortname = var.appshortname
  AlarmId       = "ServiceHeartbeatMissing-EEM"
  Description   = "${var.appshortname}-monitoring: EEM OK heartbeat missing"
  Severity      = "MINOR"

  # Log metric filter: COUNT the OK heartbeats from the script
  LogGroups      = [var.log_group_name]
  FilterPatterns = ["\"AUTOSYS_HEALTH service=EEM status=OK\""]

  # Metric+alarm config (module creates both)
  MetricNamespace   = "AppLogMetrics/${var.appshortname}"
  MetricName        = "${var.appshortname}-monitoring-service-EEM"
  Statistic          = "Sum"                 # count OKs
  Threshold          = 1                     # expect >=1 OK per minute
  DatapointsToAlarm  = 1
  EvaluationPeriods  = 2                     # two consecutive minutes missing => ALARM
  Period             = 60
  ComparisonOperator = "LessThanThreshold"
}



