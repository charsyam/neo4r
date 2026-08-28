import socket
import unittest

from neo4r_client.client import _parse_redirect
from neo4r_client.protocol import (
    NativeFrame,
    Node,
    QUERY,
    QueryValue,
    decode_query_rows,
    encode_query_payload,
    read_frame,
    write_frame,
)


class ProtocolTests(unittest.TestCase):
    def test_query_payload_encodes_typed_params(self):
        payload = encode_query_payload(
            "MATCH (n:Person) WHERE n.name = $name RETURN n.name",
            {"name": "Alice", "age": 42, "active": True},
        )

        self.assertEqual(
            payload,
            (
                "MATCH (n:Person) WHERE n.name = $name RETURN n.name"
                "\tactive=b:true\tage=i:42\tname=s:Alice"
            ),
        )

    def test_native_frame_round_trips_over_socket_pair(self):
        left, right = socket.socketpair()
        try:
            write_frame(left, NativeFrame(QUERY, 7, b"MATCH (n) RETURN n"))

            frame = read_frame(right)

            self.assertEqual(frame, NativeFrame(QUERY, 7, b"MATCH (n) RETURN n"))
        finally:
            left.close()
            right.close()

    def test_decodes_query_rows_from_shared_wire_fixture(self):
        rows = decode_query_rows(
            "6e2e6e616d65=V:s:416c696365;"
            "6e=N:0:506572736f6e:6e616d65~s:416c696365,616765~i:42"
        )

        self.assertEqual(
            rows,
            [
                {
                    "n.name": QueryValue("scalar", "Alice"),
                    "n": QueryValue(
                        "node",
                        Node(
                            0,
                            ["Person"],
                            {"name": "Alice", "age": 42},
                        ),
                    ),
                }
            ],
        )

    def test_decodes_node_properties_with_vector_values(self):
        rows = decode_query_rows(
            "6e=N:18:446f63756d656e74:"
            "656d62656464696e67~v:1065353216,0,"
            "7469746c65~s:517565727920506c616e6e6572204e6f746573"
        )

        self.assertEqual(
            rows,
            [
                {
                    "n": QueryValue(
                        "node",
                        Node(
                            18,
                            ["Document"],
                            {
                                "embedding": [1.0, 0.0],
                                "title": "Query Planner Notes",
                            },
                        ),
                    )
                }
            ],
        )

    def test_parse_redirect_response(self):
        redirect = _parse_redirect(
            "ERR\tMOVED\tshard=3\tleader=2\taddress=127.0.0.1:17688\t"
            "routing_version=17\townership_epoch=17\tdatabase=tenant_a\tretryable=true"
        )

        self.assertEqual(
            redirect,
            {
                "kind": "MOVED",
                "shard": 3,
                "leader": 2,
                "address": "127.0.0.1:17688",
                "routing_version": 17,
                "ownership_epoch": 17,
                "database": "tenant_a",
                "retryable": True,
            },
        )

    def test_parse_typed_stale_epoch_response(self):
        redirect = _parse_redirect(
            "ERR\tSTALE_EPOCH\ttx_epoch=1\tcurrent_epoch=2\t"
            "routing_version=2\townership_epoch=2\tretryable=true"
        )

        self.assertEqual(redirect["kind"], "STALE_EPOCH")
        self.assertEqual(redirect["routing_version"], 2)
        self.assertEqual(redirect["ownership_epoch"], 2)
        self.assertTrue(redirect["retryable"])


if __name__ == "__main__":
    unittest.main()
