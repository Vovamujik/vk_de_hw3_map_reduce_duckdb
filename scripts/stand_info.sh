#!/usr/bin/env bash
# Этап 1: состояние стенда RustFS — команды и их вывод для отчёта.
#   bash scripts/stand_info.sh | tee logs/01_rustfs.txt   (после загрузки данных: в конце виден объект)
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; [ -f .env ] && . ./.env; set +a
PORT=${RUSTFS_API_PORT:-9100}
BUCKET=${S3_BUCKET:-vacancies}
run() { printf '$'; printf ' %q' "$@"; echo; "$@" 2>&1; echo; }

run docker version --format 'Docker: клиент {{.Client.Version}}, сервер {{.Server.Version}}'
run docker compose version
run docker compose ps --format 'table {{.Name}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
run docker exec hw3-rustfs rustfs --version
run sh -c 'docker logs hw3-rustfs 2>&1 | head -3'
run curl -s -w '\nHTTP %{http_code}\n' "http://localhost:$PORT/health/ready"
run docker exec hw3-rustfs sh -c "du -sh /data/rustfs*/$BUCKET"
# Как лёг исходный CSV на первую папку-диск: метаданные xl.meta и части part.N
run docker exec hw3-rustfs sh -c "find /data/rustfs0/$BUCKET/raw -type f | sort | head -8; echo ...; find /data/rustfs0/$BUCKET/raw -type f | wc -l"
