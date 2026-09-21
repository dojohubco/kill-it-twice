#!/bin/bash
set -euo pipefail
umask 077
roles='source_admin pipeline_admin source_writer source_command source_reader source_capture source_bootstrap source_backfill source_operator pipeline_stager pipeline_capture pipeline_es pipeline_rabbit pipeline_receipts pipeline_backfill pipeline_operator consumer_runtime consumer_receipt_reader consumer_operator es_setup es_runtime rabbit_setup operator_token'
mkdir -p /private /source-secret /pipeline-secret /es /rabbit /ready
for role in $roles; do
  if [[ ! -s /private/$role ]]; then
    [[ ! -e /private/provisioned ]] || { echo 'Missing retained credential; refusing rotation' >&2; exit 1; }
    openssl rand -hex 24 > "/private/$role.tmp"
    mv "/private/$role.tmp" "/private/$role"
  fi
done
if [[ ! -s /private/installation-id ]]; then
  [[ ! -e /private/provisioned ]] || exit 1
  cat /proc/sys/kernel/random/uuid > /private/installation-id
fi
if [[ ! -e /private/provisioned ]]; then
  openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
    -keyout /private/server.key -out /private/server.crt \
    -subj '/CN=kill-it-twice-local' \
    -addext 'subjectAltName=DNS:localhost,DNS:elasticsearch,DNS:rabbitmq,DNS:toxiproxy,IP:127.0.0.1' >/dev/null 2>&1
  cp /private/source_admin /source-secret/password
  cp /private/pipeline_admin /pipeline-secret/password
  cp /private/server.key /private/server.crt /es/
  cp /private/server.key /private/server.crt /rabbit/
  cat > /es/elasticsearch.yml <<'YAML'
cluster.name: kill-it-twice-local
network.host: 0.0.0.0
discovery.type: single-node
xpack.security.enabled: true
xpack.security.autoconfiguration.enabled: false
xpack.security.http.ssl.enabled: true
xpack.security.http.ssl.key: /kit/server.key
xpack.security.http.ssl.certificate: /kit/server.crt
action.auto_create_index: false
ingest.geoip.downloader.enabled: false
YAML
  cat > /rabbit/rabbitmq.conf <<EOF
listeners.tcp = none
listeners.ssl.default = 5671
ssl_options.cacertfile = /kit/server.crt
ssl_options.certfile = /kit/server.crt
ssl_options.keyfile = /kit/server.key
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
management.ssl.port = 15671
management.ssl.cacertfile = /kit/server.crt
management.ssl.certfile = /kit/server.crt
management.ssl.keyfile = /kit/server.key
default_user = m4_setup
default_pass = $(cat /private/rabbit_setup)
max_message_size = 131072
heartbeat = 15
vm_memory_high_watermark.absolute = 512MiB
disk_free_limit.absolute = 1GB
EOF
  printf '[rabbitmq_management].\n' > /rabbit/enabled_plugins
  chown -R 1000:0 /es /rabbit
  chmod 700 /es /rabbit
  touch /private/provisioned
fi
openssl x509 -in /private/server.crt -checkend 86400 -noout >/dev/null
cmp /private/source_admin /source-secret/password
cmp /private/pipeline_admin /pipeline-secret/password
cmp /private/server.crt /es/server.crt
cmp /private/server.crt /rabbit/server.crt
for role in capture backfill elasticsearch publisher consumer observer control seed writer; do
  mkdir -p "/out/$role"
  chown 1000:0 "/out/$role"
  chmod 700 "/out/$role"
done
mkdir -p /rabbit-data
chown 1000:0 /rabbit-data /ready
chmod 700 /rabbit-data /ready
printf 'Local credentials and TLS material retained; no credentials printed.\n'
