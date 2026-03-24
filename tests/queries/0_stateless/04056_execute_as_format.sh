#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TEST_USER="fmtuser_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${TEST_USER}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${TEST_USER}"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${TEST_USER}"
$CLICKHOUSE_CLIENT --query "GRANT IMPERSONATE ON ${TEST_USER} TO default"

echo "--- FORMAT JSON ---"
# Check that FORMAT JSON is respected by verifying JSON structure in output
$CLICKHOUSE_CLIENT --query "EXECUTE AS ${TEST_USER} SELECT 1 AS x FORMAT JSON" | grep -o '"x": [0-9]*'

echo "--- FORMAT CSV ---"
$CLICKHOUSE_CLIENT --query "EXECUTE AS ${TEST_USER} SELECT 'hello' AS s, 42 AS n FORMAT CSV"

echo "--- FORMAT TabSeparatedWithNames ---"
$CLICKHOUSE_CLIENT --query "EXECUTE AS ${TEST_USER} SELECT 1 AS a, 2 AS b FORMAT TabSeparatedWithNames"

echo "--- INTO OUTFILE with FORMAT CSV ---"
OUTFILE="${CLICKHOUSE_TMP}/execute_as_outfile_${CLICKHOUSE_DATABASE}.csv"
$CLICKHOUSE_CLIENT --query "EXECUTE AS ${TEST_USER} SELECT 'file' AS c1, 100 AS c2 INTO OUTFILE '${OUTFILE}' FORMAT CSV"
cat "${OUTFILE}"
rm -f "${OUTFILE}"

$CLICKHOUSE_CLIENT --query "DROP USER ${TEST_USER}"
