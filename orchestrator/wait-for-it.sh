#!/bin/bash
# wait-for-it.sh
export PATH=/usr/pgsql-9.2/bin:$PATH


set -e

host="$1"
port="$2"
shift 2

# Store the command to execute
cmd=()
while [ "$#" -gt 0 ]; do
    if [ "$1" = "--" ]; then
        shift
        break
    fi
    cmd+=("$1")
    shift
done

# Add remaining arguments to command
cmd+=("$@")

until PGPASSWORD=$POSTGRES_PASSWORD psql -h "$host" -p "$port" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

>&2 echo "Postgres is up - executing command"
exec "${cmd[@]}"