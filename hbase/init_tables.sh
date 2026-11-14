#!/bin/sh
set -eu

HBASE_SHELL="${HBASE_SHELL:-/usr/local/hbase/bin/hbase}"

echo "[ThreatNexus] Initializing HBase namespace and schema tables..."

# 1. Create threatnexus namespace
if ! printf 'list_namespace\n' | "$HBASE_SHELL" shell -n 2>/dev/null | grep -q '^threatnexus'; then
    until printf "create_namespace 'threatnexus'\n" | "$HBASE_SHELL" shell -n; do
        sleep 3
    done
    echo "[ThreatNexus] Namespace 'threatnexus' created."
fi

# 2. Table: threatnexus:alerts (column family: details)
if ! printf 'list\n' | "$HBASE_SHELL" shell -n 2>/dev/null | grep -q '^threatnexus:alerts'; then
    until printf "create 'threatnexus:alerts', 'details'\n" | "$HBASE_SHELL" shell -n; do
        sleep 3
    done
    echo "[ThreatNexus] Table 'threatnexus:alerts' created."
fi

# 3. Table: threatnexus:ip_summary (column family: stats)
if ! printf 'list\n' | "$HBASE_SHELL" shell -n 2>/dev/null | grep -q '^threatnexus:ip_summary'; then
    until printf "create 'threatnexus:ip_summary', 'stats'\n" | "$HBASE_SHELL" shell -n; do
        sleep 3
    done
    echo "[ThreatNexus] Table 'threatnexus:ip_summary' created."
fi

# 4. Table: threatnexus:system_metrics (column family: value)
if ! printf 'list\n' | "$HBASE_SHELL" shell -n 2>/dev/null | grep -q '^threatnexus:system_metrics'; then
    until printf "create 'threatnexus:system_metrics', 'value'\n" | "$HBASE_SHELL" shell -n; do
        sleep 3
    done
    echo "[ThreatNexus] Table 'threatnexus:system_metrics' created."
fi

echo "[ThreatNexus] All HBase tables verified and ready."
