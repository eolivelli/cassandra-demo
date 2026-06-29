#!/usr/bin/env bash
#
# Convenience wrapper to run the Cassandra online store simulator.
#
# Assumes the fat jar has already been built (mvn package) and that `java`
# is on the PATH. All arguments are passed straight through to the tool.
#
# Examples:
#   ./store.sh --help
#   ./store.sh create-schema -H 127.0.0.1 -k store --replication-factor 3
#   ./store.sh simulate -H 127.0.0.1 -k store --parallelism 64 --operations 100000000
#   ./store.sh drop-schema -k store
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR="${STORE_JAR:-$SCRIPT_DIR/target/cassandra-store-sim.jar}"

if ! command -v java >/dev/null 2>&1; then
    echo "error: 'java' was not found on the PATH" >&2
    exit 1
fi

if [[ ! -f "$JAR" ]]; then
    echo "error: jar not found at $JAR" >&2
    echo "build it first with: mvn package" >&2
    echo "(or set STORE_JAR to point at the jar)" >&2
    exit 1
fi

exec java ${JAVA_OPTS:-} -jar "$JAR" "$@"
