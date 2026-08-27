from __future__ import annotations

import socket
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


class Client:
    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._next_request_id = 1

    @classmethod
    def connect(cls, host: str = "127.0.0.1", port: int = 7687, timeout: float = 3.0) -> "Client":
        return cls(socket.create_connection((host, port), timeout=timeout))

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

    def statistics(self) -> str:
        return response_field(self.command("STATISTICS"), "STATISTICS")

    def storage_status(self) -> str:
        return response_field(self.command("STORAGE_STATUS"), "STORAGE_STATUS")

    def metadata_log(self) -> str:
        return response_field(self.command("METADATA_LOG"), "METADATA_LOG")

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
            return text
        if frame.message_type == ERROR:
            raise ServerError(text)
        raise ProtocolError(f"unexpected response message type: {frame.message_type}")

    def _allocate_request_id(self) -> int:
        request_id = self._next_request_id
        self._next_request_id += 1
        return request_id
