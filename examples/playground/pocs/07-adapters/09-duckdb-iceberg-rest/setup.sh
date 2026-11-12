#!/usr/bin/env bash
# Waits for the stack in docker-compose.yml, then creates the `warehouse`
# bucket and the Lakekeeper warehouse `demo` over it. Safe to run twice.
set -euo pipefail

CATALOG="${LAKEKEEPER_URL:-http://localhost:8181}"
S3="${S3_URL:-http://localhost:8333}"

for i in $(seq 1 120); do
    if curl -fsS "$CATALOG/health" >/dev/null 2>&1 && curl -sS "$S3" >/dev/null 2>&1; then
        break
    fi
    if [ "$i" = 120 ]; then
        echo "FAIL: the catalog or S3 store did not start within 120s" >&2
        exit 1
    fi
    sleep 1
done

curl -fsS -X PUT "$S3/warehouse" >/dev/null 2>&1 || true

# Bootstrapping twice answers an error; the server is bootstrapped either way.
curl -sS -X POST "$CATALOG/management/v1/bootstrap" \
    -H 'content-type: application/json' \
    -d '{"accept-terms-of-use": true}' >/dev/null

if ! curl -fsS "$CATALOG/management/v1/warehouse" | grep -q '"name":"demo"'; then
    # Lakekeeper reaches S3 inside the compose network as `s3:8333`; DuckDB
    # reaches it from the host as localhost:8333 (rocky.toml).
    curl -fsS -X POST "$CATALOG/management/v1/warehouse" \
        -H 'content-type: application/json' \
        -d '{
          "warehouse-name": "demo",
          "storage-profile": {
            "type": "s3", "bucket": "warehouse", "region": "us-east-1",
            "endpoint": "http://s3:8333", "path-style-access": true,
            "sts-enabled": false, "flavor": "s3-compat"
          },
          "storage-credential": {
            "type": "s3", "credential-type": "access-key",
            "aws-access-key-id": "rocky-poc", "aws-secret-access-key": "rocky-poc-secret"
          }
        }' >/dev/null
fi
echo "    catalog ready: warehouse 'demo' at $CATALOG/catalog"
