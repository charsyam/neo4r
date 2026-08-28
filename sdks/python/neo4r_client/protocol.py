from __future__ import annotations

from dataclasses import dataclass
import socket
import struct
from typing import Any, TypeAlias

MAGIC = b"N4R1"
VERSION = 1
HEADER = struct.Struct(">4sBBHIQ")
MAX_PAYLOAD_LEN = 16 * 1024 * 1024

PING = 1
QUIT = 2
QUERY = 3
COMMAND = 4
FETCH = 5
CLOSE_CURSOR = 6
CANCEL = 7
RESPONSE = 128
ERROR = 129


@dataclass(frozen=True)
class Node:
    id: int
    labels: list[str]
    properties: dict[str, Any]


@dataclass(frozen=True)
class Relationship:
    id: int
    from_id: int
    to_id: int
    rel_type: str
    properties: dict[str, Any]


@dataclass(frozen=True)
class QueryValue:
    kind: str
    value: Any


@dataclass(frozen=True)
class NativeFrame:
    message_type: int
    request_id: int
    payload: bytes
    flags: int = 0


@dataclass(frozen=True)
class ResultStart:
    cursor_id: int
    total_rows: int | None
    rows: list[dict[str, QueryValue]]
    has_more: bool


@dataclass(frozen=True)
class ResultPage:
    cursor_id: int
    rows: list[dict[str, QueryValue]]
    has_more: bool


QueryResult: TypeAlias = ResultStart | ResultPage | list[dict[str, QueryValue]]


def write_frame(sock: socket.socket, frame: NativeFrame) -> None:
    if len(frame.payload) > MAX_PAYLOAD_LEN:
        raise ValueError(f"native frame payload too large: {len(frame.payload)}")
    header = HEADER.pack(
        MAGIC,
        VERSION,
        frame.message_type,
        frame.flags,
        len(frame.payload),
        frame.request_id,
    )
    sock.sendall(header + frame.payload)


def read_frame(sock: socket.socket) -> NativeFrame | None:
    header = _recv_exact(sock, HEADER.size)
    if header is None:
        return None
    magic, version, message_type, flags, payload_len, request_id = HEADER.unpack(header)
    if magic != MAGIC:
        raise ValueError("invalid native protocol magic")
    if version != VERSION:
        raise ValueError(f"unsupported native protocol version: {version}")
    if payload_len > MAX_PAYLOAD_LEN:
        raise ValueError(f"native frame payload too large: {payload_len}")
    payload = _recv_exact(sock, payload_len)
    if payload is None:
        raise EOFError("server closed while reading native frame payload")
    return NativeFrame(message_type, request_id, payload, flags)


def encode_query_payload(query: str, params: dict[str, Any] | None = None) -> str:
    parts = [query]
    for key in sorted((params or {}).keys()):
        parts.append(f"{key}={encode_command_value((params or {})[key])}")
    return "\t".join(parts)


def encode_command_value(value: Any) -> str:
    if value is None:
        return "n:"
    if isinstance(value, bool):
        return f"b:{str(value).lower()}"
    if isinstance(value, int) and not isinstance(value, bool):
        return f"i:{value}"
    if isinstance(value, float):
        return f"f:{value}"
    if isinstance(value, str):
        return f"s:{value}"
    if isinstance(value, (list, tuple)):
        return "v:" + ",".join(str(float(item)) for item in value)
    if isinstance(value, dict):
        return "m:" + hex_encode(encode_properties(value).encode())
    raise TypeError(f"unsupported neo4r value: {value!r}")


def decode_query_rows(payload: str) -> list[dict[str, QueryValue]]:
    if not payload:
        return []
    return [decode_query_row(row) for row in payload.split("|")]


def parse_rows_response(payload: str) -> list[dict[str, QueryValue]]:
    parts = payload.split("\t", 3)
    if len(parts) != 4 or parts[0] != "OK" or parts[1] != "ROWS":
        raise ValueError(f"expected ROWS response, got: {payload}")
    rows = decode_query_rows(parts[3])
    if len(rows) != int(parts[2]):
        raise ValueError("ROWS count mismatch")
    return rows


def parse_result_start(payload: str) -> ResultStart:
    parts = payload.split("\t", 6)
    if len(parts) != 7 or parts[0] != "OK" or parts[1] != "RESULT_START":
        raise ValueError(f"expected RESULT_START response, got: {payload}")
    rows = decode_query_rows(parts[6])
    if len(rows) != int(parts[4]):
        raise ValueError("RESULT_START count mismatch")
    return ResultStart(
        cursor_id=int(parts[2]),
        total_rows=None if parts[3] == "UNKNOWN" else int(parts[3]),
        rows=rows,
        has_more=_parse_bool(parts[5]),
    )


def parse_result_page(payload: str) -> ResultPage:
    parts = payload.split("\t", 5)
    if len(parts) != 6 or parts[0] != "OK" or parts[1] != "RESULT_PAGE":
        raise ValueError(f"expected RESULT_PAGE response, got: {payload}")
    rows = decode_query_rows(parts[5])
    if len(rows) != int(parts[3]):
        raise ValueError("RESULT_PAGE count mismatch")
    return ResultPage(cursor_id=int(parts[2]), rows=rows, has_more=_parse_bool(parts[4]))


def response_field(payload: str, expected_kind: str) -> str:
    parts = payload.split("\t", 2)
    if len(parts) != 3 or parts[0] != "OK" or parts[1] != expected_kind:
        raise ValueError(f"expected {expected_kind} response, got: {payload}")
    return unescape_response(parts[2])


def decode_query_row(payload: str) -> dict[str, QueryValue]:
    row: dict[str, QueryValue] = {}
    if not payload:
        return row
    for cell in payload.split(";"):
        name, value = cell.split("=", 1)
        row[bytes.fromhex(name).decode()] = decode_query_value(value)
    return row


def decode_query_value(payload: str) -> QueryValue:
    kind, value = payload.split(":", 1)
    if kind == "V":
        return QueryValue("scalar", decode_value(value))
    if kind == "N":
        node_id, labels, properties = value.split(":", 2)
        return QueryValue("node", Node(int(node_id), decode_labels(labels), decode_properties(properties)))
    if kind == "B":
        node_id, _owner, _version, labels, properties = value.split(":", 4)
        return QueryValue("boundary_node", Node(int(node_id), decode_labels(labels), decode_properties(properties)))
    if kind == "R":
        rel_id, from_id, to_id, rel_type, properties = value.split(":", 4)
        return QueryValue(
            "relationship",
            Relationship(
                int(rel_id),
                int(from_id),
                int(to_id),
                bytes.fromhex(rel_type).decode(),
                decode_properties(properties),
            ),
        )
    raise ValueError(f"unknown query value kind: {kind}")


def decode_value(payload: str) -> Any:
    if payload == "n":
        return None
    kind, value = payload.split(":", 1)
    if kind == "b":
        if value == "0":
            return False
        if value == "1":
            return True
        raise ValueError(f"invalid bool payload: {value}")
    if kind == "i":
        return int(value)
    if kind == "f":
        return struct.unpack(">d", int(value).to_bytes(8, "big"))[0]
    if kind == "s":
        return bytes.fromhex(value).decode()
    if kind == "v":
        if not value:
            return []
        return [struct.unpack(">f", int(item).to_bytes(4, "big"))[0] for item in value.split(",")]
    if kind == "m":
        return decode_properties(bytes.fromhex(value).decode())
    raise ValueError(f"unknown value kind: {kind}")


def encode_properties(properties: dict[str, Any]) -> str:
    entries = []
    for key in sorted(properties.keys()):
        entries.append(f"{hex_encode(key.encode())}~{encode_stored_value(properties[key])}")
    return ",".join(entries)


def decode_properties(payload: str) -> dict[str, Any]:
    if not payload:
        return {}
    properties: dict[str, Any] = {}
    for entry in split_property_entries(payload):
        key, value = entry.split("~", 1)
        properties[bytes.fromhex(key).decode()] = decode_value(value)
    return properties


def split_property_entries(payload: str) -> list[str]:
    entries: list[str] = []
    current: list[str] = []
    for part in payload.split(","):
        if "~" in part:
            if current:
                entries.append(",".join(current))
            current = [part]
        elif current:
            current.append(part)
        else:
            raise ValueError(f"invalid property entry: {part}")
    if current:
        entries.append(",".join(current))
    return entries


def encode_stored_value(value: Any) -> str:
    if value is None:
        return "n"
    if isinstance(value, bool):
        return f"b:{1 if value else 0}"
    if isinstance(value, int) and not isinstance(value, bool):
        return f"i:{value}"
    if isinstance(value, float):
        return f"f:{struct.unpack('>Q', struct.pack('>d', value))[0]}"
    if isinstance(value, str):
        return f"s:{hex_encode(value.encode())}"
    if isinstance(value, (list, tuple)):
        return "v:" + ",".join(str(struct.unpack(">I", struct.pack(">f", float(item)))[0]) for item in value)
    if isinstance(value, dict):
        return "m:" + hex_encode(encode_properties(value).encode())
    raise TypeError(f"unsupported neo4r value: {value!r}")


def decode_labels(payload: str) -> list[str]:
    if not payload:
        return []
    return [bytes.fromhex(label).decode() for label in payload.split(",")]


def hex_encode(value: bytes) -> str:
    return value.hex()


def unescape_response(value: str) -> str:
    output = []
    escaped = False
    for ch in value:
        if escaped:
            output.append("\n" if ch == "n" else ch if ch == "\\" else "\\" + ch)
            escaped = False
        elif ch == "\\":
            escaped = True
        else:
            output.append(ch)
    if escaped:
        output.append("\\")
    return "".join(output)


def _parse_bool(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError(f"expected bool token, got: {value}")


def _recv_exact(sock: socket.socket, size: int) -> bytes | None:
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            if remaining == size:
                return None
            raise EOFError("socket closed before reading expected bytes")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)
