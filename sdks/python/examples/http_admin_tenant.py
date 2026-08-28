from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request


def request_json(base_url: str, method: str, path: str, token: str, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        base_url + path,
        data=body,
        method=method,
        headers={
            "authorization": f"Bearer {token}",
            "content-type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.loads(response.read().decode())


def main() -> None:
    parser = argparse.ArgumentParser(description="Run tenant/admin HTTP API example.")
    parser.add_argument("--base-url", default="http://127.0.0.1:7474")
    parser.add_argument("--admin-token", default="admin:secret")
    parser.add_argument("--database", default="tenant_a")
    parser.add_argument("--user-token", default="writer:tenant-example")
    args = parser.parse_args()

    try:
        databases = request_json(
            args.base_url,
            "POST",
            "/api/admin/databases",
            args.admin_token,
            {"name": args.database},
        )
        users = request_json(
            args.base_url,
            "POST",
            "/api/admin/invoke-token",
            args.admin_token,
            {
                "name": "tenant-example",
                "token_id": "main",
                "role": "writer",
                "token": args.user_token,
                "expired_at": "0",
                "database": args.database,
                "database_role": "writer",
            },
        )
        created = request_json(
            args.base_url,
            "POST",
            "/api/query",
            args.user_token,
            {
                "database": args.database,
                "query": (
                    "MERGE (n:TenantSample {sample_id: $sample_id}) "
                    "ON CREATE SET n.name = $name "
                    "ON MATCH SET n.name = $name "
                    "RETURN n.name"
                ),
                "params": {"sample_id": "http-admin-tenant", "name": "Tenant Alice"},
            },
        )
        selected = request_json(
            args.base_url,
            "GET",
            f"/api/database?db={args.database}",
            args.user_token,
        )
    except urllib.error.URLError as err:
        raise SystemExit(f"HTTP admin tenant example failed: {err}") from err

    print("databases:", databases)
    print("users:", users)
    print("created:", created)
    print("selected:", selected)


if __name__ == "__main__":
    main()
