#!/usr/bin/env bash
# Emit only phase and selected state/health metadata, never logs/config/env.
set -euo pipefail
project=$1
phase=$2
test_exit=$3
test_log=$4
destination=$5
metadata_tmp=$(mktemp -d)
trap 'rm -rf "$metadata_tmp"' EXIT
mkdir -p "$(dirname "$destination")"

docker ps -aq --filter "label=com.docker.compose.project=$project" |
while IFS= read -r container_id; do
    docker inspect --format '{"service":{{json (index .Config.Labels "com.docker.compose.service")}},"state":{{json .State.Status}},"exit_code":{{.State.ExitCode}},"oom_killed":{{.State.OOMKilled}},"image_id":{{json .Image}},"started_at":{{json .State.StartedAt}},"finished_at":{{json .State.FinishedAt}},"health":{{if .State.Health}}{{json .State.Health.Status}}{{else}}null{{end}},"health_checks":[{{if .State.Health}}{{range $i,$check := .State.Health.Log}}{{if $i}},{{end}}{"started_at":{{json $check.Start}},"finished_at":{{json $check.End}},"exit_code":{{$check.ExitCode}}}{{end}}{{end}}]}' "$container_id"
done | jq -s 'map({service,state,exit_code,oom_killed,image_id,started_at,finished_at,health,
    health_checks:((.health_checks // []) | map({started_at,finished_at,exit_code}))})' \
    > "$metadata_tmp/containers.json"

# Whitelisted exception class names contain no messages, source, or locals.
if [ -f "$test_log" ]; then
    { grep -oE 'ConnectionError|SSLEOFError|SSLError|ReadTimeout|ConnectTimeout|TimeoutError|MemoryError|RuntimeError|HTTPError|URLError|OSError|ValueError|TypeError|AssertionError|FileNotFoundError' "$test_log" || true; } |
        sort -u | jq -Rsc 'split("\n") | map(select(length > 0))' > "$metadata_tmp/error-types.json"
else
    printf '[]\n' > "$metadata_tmp/error-types.json"
fi
jq -n --arg phase "$phase" --argjson exit_code "$test_exit" \
    --slurpfile containers "$metadata_tmp/containers.json" \
    --slurpfile errors "$metadata_tmp/error-types.json" \
    '{phase:$phase,exit_code:$exit_code,containers:$containers[0],exception_types:$errors[0]}' \
    > "$metadata_tmp/result.json"
mv "$metadata_tmp/result.json" "$destination"
