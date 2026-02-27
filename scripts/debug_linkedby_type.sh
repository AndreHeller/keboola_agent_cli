#!/usr/bin/env bash
# Demonstrates Keboola Storage API inconsistency:
# - GET /v2/storage/tokens/verify → owner.id is INTEGER
# - GET /v2/storage/buckets?include=linkedBuckets → linkedBy[].project.id is STRING
#
# Bug: project IDs should be the same type across all endpoints.

set -euo pipefail

TOKEN="338-4089604-tiWChciXO55eUILpFpQ1Kdhd0QOmaA7ypTFr1dES"
STACK="https://connection.us-east4.gcp.keboola.com"

echo "=== 1) Token verify: owner.id type ==="
curl -s -H "X-StorageApi-Token: $TOKEN" \
  "$STACK/v2/storage/tokens/verify" \
  | jq '{owner_id: .owner.id, type: (.owner.id | type)}'

echo ""
echo "=== 2) List buckets: linkedBy[].project.id types ==="
curl -s -H "X-StorageApi-Token: $TOKEN" \
  "$STACK/v2/storage/buckets?include=linkedBuckets" \
  | jq '[.[] | select(.linkedBy != null and (.linkedBy | length > 0))
         | .linkedBy[] | {project_id: .project.id, type: (.project.id | type)}]
         | unique_by(.type)'

echo ""
echo "=== Summary ==="
echo "If owner.id type is 'number' but linkedBy[].project.id type is 'string',"
echo "that is the bug — project IDs should consistently be integers."
