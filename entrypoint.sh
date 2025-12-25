#!/usr/bin/env bash
set -euo pipefail
export PATH="/home/airflow/.local/bin:${PATH}"

airflow db migrate

if ! airflow users list | awk 'NR>2 {print $2}' | grep -qx "${AIRFLOW_USER:-airflow}"; then
  airflow users create \
    -u "${AIRFLOW_USER:-airflow}" \
    -p "${AIRFLOW_PASSWORD:-airflow}" \
    -r Admin \
    -f Admin \
    -l User \
    -e admin@example.com
fi

airflow scheduler &
exec airflow webserver
