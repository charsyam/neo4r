from __future__ import annotations

import socket
import time
from typing import Any

from .protocol import (
    CLOSE_CURSOR,
    COMMAND,
    ERROR,
    FETCH,
    PING,
    QUERY,
    QUIT,
    RESPONSE,
    NativeFrame,
    encode_query_payload,
    parse_result_page,
    parse_result_start,
    parse_rows_response,
    read_frame,
    response_field,
    write_frame,
)


class Neo4rError(Exception):
    pass


class ProtocolError(Neo4rError):
    pass


class ServerError(Neo4rError):
    pass


class RedirectError(ServerError):
    def __init__(self, redirect: dict[str, Any]):
        super().__init__(f"redirect: {redirect}")
        self.redirect = redirect


class Client:
    def __init__(
        self,
        sock: socket.socket,
        timeout: float = 3.0,
        redirect_max_hops: int = 4,
    ):
        self._sock = sock
        self._timeout = timeout
        self._redirect_max_hops = max(0, redirect_max_hops)
        self._next_request_id = 1
        self.topology_cache: dict[str, Any] = {
            "database": "default",
            "routing_version": 0,
            "ownership_epoch": 0,
            "last_address": None,
            "addresses": [],
            "expires_at": None,
        }

    @classmethod
    def connect(
        cls,
        host: str = "127.0.0.1",
        port: int = 7687,
        timeout: float = 3.0,
        retry_attempts: int = 0,
        retry_backoff: float = 0.05,
        redirect_max_hops: int = 4,
    ) -> "Client":
        attempt = 0
        while True:
            try:
                sock = socket.create_connection((host, port), timeout=timeout)
                sock.settimeout(timeout)
                return cls(sock, timeout=timeout, redirect_max_hops=redirect_max_hops)
            except OSError:
                if attempt >= retry_attempts:
                    raise
                attempt += 1
                time.sleep(retry_backoff)

    @classmethod
    def connect_with_seeds(
        cls,
        seeds: list[str | tuple[str, int]],
        timeout: float = 3.0,
        retry_attempts: int = 0,
        retry_backoff: float = 0.05,
        redirect_max_hops: int = 4,
    ) -> "Client":
        if not seeds:
            raise ProtocolError("at least one seed address is required")
        last_error: OSError | Neo4rError | None = None
        for seed in seeds:
            host, port = _split_address(seed)
            try:
                client = cls.connect(
                    host,
                    port,
                    timeout=timeout,
                    retry_attempts=retry_attempts,
                    retry_backoff=retry_backoff,
                    redirect_max_hops=redirect_max_hops,
                )
                client.bootstrap_topology()
                return client
            except (OSError, Neo4rError) as err:
                last_error = err
        if last_error is not None:
            raise last_error
        raise ProtocolError("no seed address was attempted")

    def ping(self) -> None:
        self._expect(PING, b"", "OK\tPONG")

    def close(self) -> None:
        self._expect(QUIT, b"", "OK\tBYE")
        self._sock.close()

    def query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        payload = encode_query_payload(query, params).encode()
        response = self._request(QUERY, payload)
        start = parse_result_start(response)
        rows = list(start.rows)
        has_more = start.has_more
        while has_more:
            page = self.fetch(start.cursor_id)
            rows.extend(page.rows)
            has_more = page.has_more
        self.close_cursor(start.cursor_id)
        return rows

    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        return self.query(query, params)

    def command(self, command: str) -> str:
        return self._request(COMMAND, command.encode())

    def profile(self, query: str, params: dict[str, Any] | None = None) -> str:
        response = self.command("PROFILE\t" + encode_query_payload(query, params))
        return response_field(response, "PROFILE")

    def query_plan(self, query: str, params: dict[str, Any] | None = None) -> str:
        response = self.command("QUERY_PLAN\t" + encode_query_payload(query, params))
        return response_field(response, "QUERY_PLAN")

    def statistics(self) -> str:
        return response_field(self.command("STATISTICS"), "STATISTICS")

    def storage_status(self) -> str:
        return response_field(self.command("STORAGE_STATUS"), "STORAGE_STATUS")

    def metadata_log(self) -> str:
        return response_field(self.command("METADATA_LOG"), "METADATA_LOG")

    def cluster_status(self) -> str:
        return response_field(self.command("CLUSTER_STATUS"), "CLUSTER_STATUS")

    def cluster_management_status(self) -> str:
        return response_field(self.command("CLUSTER_MANAGEMENT_STATUS"), "CLUSTER_MANAGEMENT_STATUS")

    def routing_table(self) -> str:
        return response_field(self.command("ROUTING_TABLE"), "ROUTING_TABLE")

    def cluster_registry(self) -> str:
        registry = response_field(self.command("CLUSTER_REGISTRY"), "CLUSTER_REGISTRY")
        self._update_topology_cache_from_registry(registry)
        return registry

    def bootstrap_topology(self) -> dict[str, Any]:
        self.cluster_registry()
        return self.topology_cache

    def capabilities(self) -> str:
        return response_field(self.command("CAPABILITIES"), "CAPABILITIES")

    def topology_addresses(self) -> list[str]:
        return list(self.topology_cache.get("addresses", []))

    def connect_all_topology(self) -> list["Client"]:
        clients: list[Client] = []
        for address in self.topology_addresses():
            host, port = address.rsplit(":", 1)
            clients.append(Client.connect(host, int(port), timeout=self._timeout))
        return clients

    def connect_to_cached_target(self) -> bool:
        address = self.topology_cache.get("last_address")
        expires_at = self.topology_cache.get("expires_at")
        if not address:
            return False
        if expires_at is not None and expires_at <= time.time():
            return False
        host, port = address.rsplit(":", 1)
        self._sock.close()
        self._sock = socket.create_connection((host, int(port)), timeout=self._timeout)
        self._sock.settimeout(self._timeout)
        return True

    def rows_command(self, command: str) -> list[dict[str, Any]]:
        return parse_rows_response(self.command(command))

    def fetch(self, cursor_id: int, page_size: int | None = None):
        payload = str(cursor_id) if page_size is None else f"{cursor_id}\t{page_size}"
        return parse_result_page(self._request(FETCH, payload.encode()))

    def close_cursor(self, cursor_id: int) -> None:
        self._expect(CLOSE_CURSOR, str(cursor_id).encode(), f"OK\tCURSOR_CLOSED\t{cursor_id}")

    def _expect(self, message_type: int, payload: bytes, expected: str) -> None:
        response = self._request(message_type, payload)
        if response != expected:
            raise ProtocolError(f"expected {expected}, got {response}")

    def _request(self, message_type: int, payload: bytes) -> str:
        visited: list[str] = []
        for hop in range(self._redirect_max_hops + 1):
            try:
                return self._request_once(message_type, payload)
            except RedirectError as err:
                address = err.redirect.get("address")
                if not err.redirect.get("retryable") or not address:
                    raise
                if address in visited:
                    raise ProtocolError(
                        "redirect loop detected after visiting " + " -> ".join(visited)
                    ) from err
                if hop == self._redirect_max_hops:
                    raise ProtocolError(f"redirect max hops exceeded at {address}") from err
                self._update_topology_cache_from_redirect(err.redirect)
                visited.append(address)
                host, port = address.rsplit(":", 1)
                self._sock.close()
                self._sock = socket.create_connection((host, int(port)), timeout=self._timeout)
                self._sock.settimeout(self._timeout)
        raise ProtocolError("redirect retry exhausted")

    def _request_once(self, message_type: int, payload: bytes) -> str:
        request_id = self._allocate_request_id()
        write_frame(self._sock, NativeFrame(message_type, request_id, payload))
        frame = read_frame(self._sock)
        if frame is None:
            raise ProtocolError("server closed connection")
        if frame.request_id != request_id:
            raise ProtocolError(
                f"response request id mismatch: expected {request_id}, got {frame.request_id}"
            )
        text = frame.payload.decode()
        if frame.message_type == RESPONSE:
            if text.startswith("ERR\t"):
                redirect = _parse_redirect(text)
                if redirect is not None:
                    raise RedirectError(redirect)
                raise ServerError(text)
            return text
        if frame.message_type == ERROR:
            redirect = _parse_redirect(text)
            if redirect is not None:
                raise RedirectError(redirect)
            raise ServerError(text)
        raise ProtocolError(f"unexpected response message type: {frame.message_type}")

    def _allocate_request_id(self) -> int:
        request_id = self._next_request_id
        self._next_request_id += 1
        return request_id

    def _update_topology_cache_from_redirect(self, redirect: dict[str, Any]) -> None:
        self.topology_cache["database"] = redirect.get("database", "default")
        self.topology_cache["routing_version"] = redirect.get("routing_version", 0)
        self.topology_cache["ownership_epoch"] = redirect.get("ownership_epoch", 0)
        self.topology_cache["last_address"] = redirect.get("address")
        address = redirect.get("address")
        if address:
            addresses = self.topology_cache.setdefault("addresses", [])
            if address not in addresses:
                addresses.append(address)
        self.topology_cache["expires_at"] = None

    def _update_topology_cache_from_registry(self, registry: str) -> None:
        ttl_ms = None
        local_server = None
        query_peers = None
        nodes = None
        for part in registry.split():
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key == "database":
                self.topology_cache["database"] = value
            elif key == "local_server":
                local_server = int(value)
            elif key == "routing_version":
                self.topology_cache["routing_version"] = int(value)
            elif key == "ownership_epoch":
                self.topology_cache["ownership_epoch"] = int(value)
            elif key == "ttl_ms":
                ttl_ms = int(value)
            elif key == "query_peers":
                query_peers = value
            elif key == "nodes":
                nodes = value
        if ttl_ms is not None:
            self.topology_cache["expires_at"] = time.time() + (ttl_ms / 1000.0)
        addresses = _registry_addresses(local_server, query_peers, nodes)
        address = _first_registry_address(local_server, query_peers, nodes)
        if address is not None:
            self.topology_cache["last_address"] = address
        self.topology_cache["addresses"] = addresses


def _parse_redirect(text: str) -> dict[str, Any] | None:
    parts = text.split("\t")
    if len(parts) < 2 or parts[0] != "ERR" or parts[1] not in {
        "MOVED",
        "NOT_LEADER",
        "STALE_ROUTING",
        "STALE_EPOCH",
        "REPLAYING",
    }:
        return None
    redirect: dict[str, Any] = {
        "kind": parts[1],
        "shard": 0,
        "leader": None,
        "address": None,
        "routing_version": 0,
        "ownership_epoch": 0,
        "database": "default",
        "retryable": False,
        "refresh": None,
        "server": None,
        "applied": None,
        "committed": None,
    }
    for part in parts[2:]:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        if key == "shard":
            redirect["shard"] = int(value)
        elif key == "leader" and value != "none":
            redirect["leader"] = int(value)
        elif key == "address" and value != "missing":
            redirect["address"] = value
        elif key == "routing_version":
            redirect["routing_version"] = int(value)
        elif key == "ownership_epoch":
            redirect["ownership_epoch"] = int(value)
        elif key == "database":
            redirect["database"] = value
        elif key == "retryable":
            redirect["retryable"] = value == "true"
        elif key == "refresh":
            redirect["refresh"] = value
        elif key == "server":
            redirect["server"] = int(value)
        elif key == "applied":
            redirect["applied"] = int(value)
        elif key == "committed":
            redirect["committed"] = int(value)
    if redirect["ownership_epoch"] == 0:
        redirect["ownership_epoch"] = redirect["routing_version"]
    return redirect


def _split_address(address: str | tuple[str, int]) -> tuple[str, int]:
    if isinstance(address, tuple):
        return address
    host, port = address.rsplit(":", 1)
    return host, int(port)


def _registry_addresses(
    _local_server: int | None,
    query_peers: str | None,
    nodes: str | None,
) -> list[str]:
    addresses: list[str] = []
    if query_peers and query_peers != "none":
        for peer in query_peers.split("|"):
            if ":" not in peer:
                continue
            _server, address = peer.split(":", 1)
            if address:
                if address not in addresses:
                    addresses.append(address)
    if nodes:
        for node in nodes.split("|"):
            parts = node.split(":", 2)
            if len(parts) != 3:
                continue
            _server, state, address = parts
            if state == "active" and address:
                if address not in addresses:
                    addresses.append(address)
    return addresses


def _first_registry_address(
    local_server: int | None,
    query_peers: str | None,
    nodes: str | None,
) -> str | None:
    if query_peers and query_peers != "none":
        for peer in query_peers.split("|"):
            if ":" not in peer:
                continue
            server, address = peer.split(":", 1)
            if address and (local_server is None or server != str(local_server)):
                return address
    if nodes:
        for node in nodes.split("|"):
            parts = node.split(":", 2)
            if len(parts) != 3:
                continue
            server, state, address = parts
            if state == "active" and address and (local_server is None or server != str(local_server)):
                return address
    return None
