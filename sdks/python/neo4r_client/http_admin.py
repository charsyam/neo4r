from __future__ import annotations

import json
import time
import urllib.request
from typing import Any


class HttpAdminClient:
    def __init__(self, base_url: str = "http://127.0.0.1:7474", token: str = "admin:secret"):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.topology_cache: dict[str, dict[str, Any]] = {}

    def create_database(self, name: str) -> dict[str, Any]:
        return self._json("POST", "/api/admin/databases", {"name": name})

    def select_database(self, name: str) -> dict[str, Any]:
        return self._json("POST", "/api/use-database", {"database": name})

    def list_databases(self) -> dict[str, Any]:
        return self._json("GET", "/api/admin/databases")

    def invoke_token(
        self,
        name: str,
        token_id: str,
        token: str,
        role: str = "writer",
        expired_at: str = "0",
        database: str | None = None,
        database_role: str = "writer",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": name,
            "token_id": token_id,
            "role": role,
            "token": token,
            "expired_at": expired_at,
        }
        if database is not None:
            payload["database"] = database
            payload["database_role"] = database_role
        return self._json("POST", "/api/admin/invoke-token", payload)

    def revoke_token(self, name: str, token_id: str) -> dict[str, Any]:
        return self._json(
            "POST",
            "/api/admin/revoke-token",
            {"name": name, "token_id": token_id},
        )

    def query(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        database: str | None = None,
        token: str | None = None,
        read_consistency: str | None = None,
        max_staleness_ms: int | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"query": query, "params": params or {}}
        if database is not None:
            payload["database"] = database
        if read_consistency is not None:
            payload["read_consistency"] = read_consistency
        if max_staleness_ms is not None:
            payload["max_staleness_ms"] = max_staleness_ms
        return self._json("POST", "/api/query", payload, token=token)

    def metrics(self) -> dict[str, Any]:
        return self._json("GET", "/api/metrics")

    def routing_table(self) -> dict[str, Any]:
        return self._json("GET", "/api/cluster/routing-table")

    def cluster_registry(self) -> dict[str, Any]:
        registry = self._json("GET", "/api/cluster/registry")
        database = registry.get("database", "default")
        ttl_ms = registry.get("ttl_ms", 0)
        self.topology_cache[database] = {
            "routing_version": registry.get("routing_version", 0),
            "ownership_epoch": registry.get("ownership_epoch", 0),
            "query_peers": registry.get("query_peers", []),
            "expires_at": time.time() + (ttl_ms / 1000.0),
        }
        return registry

    def capabilities(self) -> dict[str, Any]:
        return self._json("GET", "/api/capabilities")

    def _json(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        token: str | None = None,
    ) -> dict[str, Any]:
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            self.base_url + path,
            data=body,
            method=method,
            headers={
                "authorization": f"Bearer {token or self.token}",
                "content-type": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            return json.loads(response.read().decode())
