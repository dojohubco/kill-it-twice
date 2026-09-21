#!/bin/bash
set -euo pipefail
umask 077
# ES_PATH_CONF is a retained directory, so retain the pinned image JVM defaults too.
if [[ ! -s /kit/jvm.options ]]; then
  cp /usr/share/elasticsearch/config/jvm.options /kit/jvm.options
  chown 1000:0 /kit/jvm.options
fi
mkdir -p /kit/jvm.options.d
chown 1000:0 /kit/jvm.options.d
if [[ ! -e /kit/realm-ready ]]; then
  cp /usr/share/elasticsearch/config/log4j2.properties /kit/log4j2.properties
  touch /kit/users /kit/users_roles
  printf "kit_runtime:\n  cluster: ['cluster:monitor/main']\n  indices: []\n" > /kit/roles.yml
  /usr/share/elasticsearch/bin/elasticsearch-users useradd kit_setup \
    -p "$(cat /private/es_setup)" -r superuser >/dev/null
  /usr/share/elasticsearch/bin/elasticsearch-users useradd kit_runtime \
    -p "$(cat /private/es_runtime)" -r kit_runtime >/dev/null
  touch /kit/realm-ready
  chown -R 1000:0 /kit
fi
for file in users users_roles roles.yml log4j2.properties server.key server.crt; do
  [[ -s /kit/$file ]] || { echo 'Incomplete retained ES realm' >&2; exit 1; }
done
printf 'Elasticsearch file realm retained.\n'
