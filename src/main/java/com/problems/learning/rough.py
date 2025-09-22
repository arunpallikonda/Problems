eem_health.sh
#!/usr/bin/env bash
#
# EEM health probe for systemd-based monitoring.
#
# What it does:
#   - Checks whether `eem.service` is active (via systemctl).
#   - Writes an audit line to syslog (so humans can trace checks).
#   - Publishes a CloudWatch custom metric:
#       Namespace = "Autosys/Systemd"
#       Metric    = "ServiceUp"
#       Dimensions: Service=EEM
#       Value     = 1 when active, 0 when NOT active
#
# Requirements:
#   - Instance role must allow cloudwatch:PutMetricData (Terraform below).
#   - AWS CLI available on the instance (EEM AMI typically has it; if not, install awscli).
#
# NOTE:
#   - We DO NOT touch eem.service or its restart policy.
#   - This script is idempotent and safe to run repeatedly.
#

set -euo pipefail

SVC_NAME="eem.service"
CW_NAMESPACE="Autosys/Systemd"
CW_METRIC="ServiceUp"
CW_DIM_SERVICE="EEM"

log() { logger -t autosys-health "$*"; }

if systemctl is-active --quiet "${SVC_NAME}"; then
  VALUE=1
  log "OK: ${SVC_NAME} active"
else
  VALUE=0
  log "FAIL: ${SVC_NAME} not active"
fi

# Publish metric (non-fatal if PutMetricData fails; we still exit non-zero when down)
aws cloudwatch put-metric-data \
  --namespace "${CW_NAMESPACE}" \
  --metric-data "MetricName=${CW_METRIC},Value=${VALUE},Unit=None,Dimensions=[{Name=Service,Value=${CW_DIM_SERVICE}}]" \
  >/dev/null 2>&1 || log "WARN: PutMetricData failed for ${SVC_NAME}"

# Exit code reflects health so the oneshot service shows success/failure in systemd
exit $((VALUE==1 ? 0 : 2))





eem_health.service
# Oneshoot systemd unit that executes the health probe script once.
# The timer below will trigger this unit every minute.
[Unit]
Description=EEM health probe (emits CloudWatch metric + syslog audit)
Wants=eem.service
After=eem.service
ConditionPathExists=/usr/local/bin/eem_health.sh

[Service]
Type=oneshot
ExecStart=/usr/local/bin/eem_health.sh
# Run as root to allow `systemctl` and AWS CLI without sudo
User=root
Group=root



eem_health.timer
# Schedules the health probe to run once per minute.
[Unit]
Description=Run EEM health probe every minute

[Timer]
# First run 30s after boot, then once per minute.
OnBootSec=30s
OnUnitActiveSec=60s
Unit=eem_health.service
AccuracySec=5s
Persistent=true   # run missed checks after downtime

[Install]
WantedBy=timers.target




setup.sh
install_eem_monitoring() {
  set -euo pipefail

  # Base path where SSM unpacked your artifact; adjust if you use a different root
  ART_ROOT="${EEM_SOURCE:-/cp-artifacts}/monitoring"

  # Install the probe script and the timer/service units
  install -m 0755 "${ART_ROOT}/eem_health.sh"      /usr/local/bin/eem_health.sh
  install -m 0644 "${ART_ROOT}/eem_health.service" /etc/systemd/system/eem_health.service
  install -m 0644 "${ART_ROOT}/eem_health.timer"   /etc/systemd/system/eem_health.timer

  # Make systemd aware & start the schedule
  systemctl daemon-reload
  systemctl enable --now eem_health.timer
}


# Your existing lines:
systemctl daemon-reload
systemctl enable eem

# NEW: add monitoring (no change to eem.service policy)
install_eem_monitoring


