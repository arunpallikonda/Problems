setup_eem_systemd_monitoring() {
  set -euo pipefail

  # 1) Ensure auto-restart without touching your base unit
  mkdir -p /etc/systemd/system/eem.service.d
  cat >/etc/systemd/system/eem.service.d/override.conf <<'EOF'
[Service]
Restart=always
RestartSec=5
StartLimitIntervalSec=0
EOF

  # 2) Healthcheck script writes to syslog and optional metric
  install -m 0755 /dev/stdin /usr/local/bin/eem_health.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
svc="eem.service"
if systemctl is-active --quiet "$svc"; then
  logger -t autosys-health "OK: $svc active"
  # Uncomment to publish a direct metric instead of procstat:
  # aws cloudwatch put-metric-data --namespace "Autosys/Systemd" --metric-name "ServiceUp" --value 1 --dimensions Service=EEM
  exit 0
else
  logger -t autosys-health "FAIL: $svc not active"
  # aws cloudwatch put-metric-data --namespace "Autosys/Systemd" --metric-name "ServiceUp" --value 0 --dimensions Service=EEM
  exit 2
fi
EOF

  # 3) systemd timer to run healthcheck every minute
  cat >/etc/systemd/system/eem-health.service <<'EOF'
[Unit]
Description=EEM health probe
Wants=eem.service
After=eem.service
ConditionPathExists=/usr/local/bin/eem_health.sh

[Service]
Type=oneshot
ExecStart=/usr/local/bin/eem_health.sh
EOF

  cat >/etc/systemd/system/eem-health.timer <<'EOF'
[Unit]
Description=Run EEM health probe every minute

[Timer]
OnBootSec=30s
OnUnitActiveSec=60s
Unit=eem-health.service
AccuracySec=5s
Persistent=true

[Install]
WantedBy=timers.target
EOF

  # 4) CloudWatch Agent config (procstat watches eem)
  mkdir -p /opt/aws/amazon-cloudwatch-agent/etc
  cat >/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json <<'EOF'
{
  "metrics": {
    "append_dimensions": { "AutoScalingGroupName": "${aws:AutoScalingGroupName}", "InstanceId": "${aws:InstanceId}" },
    "metrics_collected": {
      "procstat": [
        {
          "pattern": "eem",
          "measurement": ["pid_count"],
          "metrics_collection_interval": 60
        }
      ]
    }
  },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          { "file_path": "/var/log/messages", "log_group_name": "/autosys/health", "log_stream_name": "{instance_id}/messages", "timestamp_format": "%b %d %H:%M:%S" }
        ]
      }
    }
  }
}
EOF

  # 5) Reload systemd and (re)start timer
  systemctl daemon-reload
  systemctl enable --now eem-health.timer

  # 6) Start/refresh CloudWatch Agent if installed
  if command -v /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl >/dev/null; then
    /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a stop || true
    /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
      -a fetch-config -m ec2 -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json -s
  fi

  # 7) (Optional, requires IAM perms) create an alarm on procstat metric
  INSTANCE_ID="$(curl -s --fail --connect-timeout 2 http://169.254.169.254/latest/meta-data/instance-id || true)"
  if [[ -n "${INSTANCE_ID:-}" ]]; then
    aws cloudwatch put-metric-alarm \
      --alarm-name "eem-proc-down-${INSTANCE_ID}" \
      --alarm-description "EEM process count is 0 on ${INSTANCE_ID}" \
      --namespace "CWAgent" \
      --metric-name "procstat_pid_count" \
      --dimensions Name=InstanceId,Value="${INSTANCE_ID}" \
      --statistic Average --period 60 --evaluation-periods 2 \
      --threshold 1 --comparison-operator LessThanThreshold \
      --treat-missing-data breaching \
      --alarm-actions "${EEM_ALARM_SNS_ARN:-}" || true
  fi
}