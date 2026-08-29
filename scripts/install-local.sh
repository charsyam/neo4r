#!/usr/bin/env bash
set -euo pipefail

PREFIX="${PREFIX:-/usr/local}"
SYSCONFDIR="${SYSCONFDIR:-/etc/neo4r}"
DATADIR="${DATADIR:-/var/lib/neo4r}"

cargo build -p neo4r-server -p neo4r-client --release

install -d "$PREFIX/bin" "$SYSCONFDIR" "$DATADIR"
install -m 0755 target/release/neo4r-server "$PREFIX/bin/neo4r-server"
install -m 0755 target/release/neo4r-cli "$PREFIX/bin/neo4r-cli"

if [[ ! -f "$SYSCONFDIR/server.yml" ]]; then
  install -m 0644 packaging/server.example.yml "$SYSCONFDIR/server.yml"
fi
install -m 0644 packaging/neo4r-server.env "$SYSCONFDIR/neo4r-server.env"

if command -v systemctl >/dev/null 2>&1; then
  install -m 0644 packaging/neo4r-server.service /etc/systemd/system/neo4r-server.service
  systemctl daemon-reload
fi

echo "neo4r installed under $PREFIX; edit $SYSCONFDIR/server.yml before starting"
