import socket
import unittest

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


if __name__ == "__main__":
    unittest.main()
