from __future__ import annotations

import argparse
import socket
import sys

from neo4r_client import Client, QueryValue


def scalar(row: dict[str, QueryValue], key: str):
    value = row[key]
    if value.kind != "scalar":
        raise TypeError(f"{key} is not a scalar value: {value!r}")
    return value.value


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a basic neo4r Python SDK example.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=17687, type=int)
    args = parser.parse_args()

    try:
        client = Client.connect(args.host, args.port)
    except OSError as err:
        print(
            f"failed to connect to neo4r at {args.host}:{args.port}: {err}",
            file=sys.stderr,
        )
        print(
            "start a local server first:\n"
            "  cargo run -p neo4r-server -- "
            f"--bind {args.host}:{args.port} "
            "--data-dir /tmp/neo4r-python-sdk-example "
            "--shards 1 --partitions 1",
            file=sys.stderr,
        )
        raise SystemExit(1)

    try:
        client.ping()
        rows = client.execute(
            "MERGE (n:Person {sample_id: $sample_id}) "
            "ON CREATE SET n.name = $name, n.age = $age "
            "ON MATCH SET n.name = $name, n.age = $age "
            "RETURN n.name, n.age",
            {"sample_id": "basic-usage", "name": "Alice", "age": 42},
        )
        print("created:", scalar(rows[0], "n.name"), scalar(rows[0], "n.age"))

        rows = client.query(
            "MATCH (n:Person) WHERE n.sample_id = $sample_id RETURN n.name, n.age",
            {"sample_id": "basic-usage"},
        )
        print("matched:", scalar(rows[0], "n.name"), scalar(rows[0], "n.age"))

        profile = client.profile("MATCH (n:Person) RETURN n", {})
        print("profile:", profile)

        plan = client.query_plan("MATCH (n:Person) RETURN n", {})
        print("query_plan:", plan)

        status = client.storage_status()
        print("storage_status:", status)

        cluster = client.cluster_status()
        print("cluster_status:", cluster)
    finally:
        client.close()


if __name__ == "__main__":
    main()
