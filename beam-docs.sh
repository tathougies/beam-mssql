#!/bin/sh

set -e

. ${BEAM_DOCS_LIBRARY}

USER=$1
PASSWORD=$2
DATABASE=$3

CHINOOK_MSSQL_URL="https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_SqlServerCompact_AutoIncrementPKs.sqlce"
EXPECTED_SHA256="94c929a672364c98b732f0eba1030bf2658f9df4c9b468b2fbcf7113a20cb9d5";

DOCKERIMG="mcr.microsoft.com/mssql/server:2017-CU8-ubuntu"
PORT=1433
SQSH=$(nix-build --no-out-link -A pkgs.sqsh '<nixos>')/bin/sqsh
DOS2UNIX=$(nix-build --no-out-link -A pkgs.dos2unix '<nixos>')/bin/dos2unix

run_sqsh() {
    DBARG=""
    if [ ! -z "$1" ]; then
        DBARG="-D $1"
    fi
    $SQSH -U $USER -P $PASSWORD -S localhost:$PORT ${DBARG}
}

db_exists() {
    echo "SELECT name FROM master.dbo.sysdatabases;" | $SQSH -U $USER -P $PASSWORD -S localhost:$PORT -h | awk '{$1=$1;print}' | grep -Fx "$1" >/dev/null
}

print_open_statement() {
    echo "let o = tdsOptionHost \"localhost\" <> tdsOptionPort ${PORT} <>"
    echo "        tdsDebugLogging <> tdsOptionUserAndPassword \"${USER}\" \"${PASSWORD}\" <>"
    echo "        tdsOptionDatabase \"${DATABASE}\""
    echo "chinook <- login o"
}

if db_exists "${DATABASE}"; then
    print_open_statement
    exit 0
fi

if [ ! -f chinook-data/Chinook_MsSQL.sql ]; then
    download "chinook-data/Chinook_MsSQL.sql" "$CHINOOK_MSSQL_URL" "$EXPECTED_SHA256" "tail -c +4 | ${DOS2UNIX}"
    exit 1
fi

beam_doc_status "Creating temporary MsSQL database ${DATABASE}..."

echo "CREATE DATABASE ${DATABASE};" | run_sqsh
pv chinook-data/Chinook_MsSQL.sql | run_sqsh "${DATABASE}" >/dev/null

beam_doc_status "Success"
print_open_statement
