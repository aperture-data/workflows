#!/bin/bash
ARGS=$(getopt -o i:p:e:n:h:o:U:P:D: -l images:,pdfs,table-ignore:,column-ignore:,host:,port:,username:,password:,database: -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1
fi

vars=()

IMAGES=''
PDFS=''
TIGNORES=''
CIGNORES=''
SQL_HOST=''
SQL_PORT=''
SQL_USERNAME=''
SQL_PASSWORD=''
SQL_DATABASE=''
eval set -- "$ARGS"
while [ : ]; do
  case "$1" in
      -i | --images)
        IMAGES=$2
        shift 2
        ;;
      -p | --pdfs)
        PDFS=$2
        shift 2
        ;;
      -e | --table-ignore)
        TIGNORES=$2
        shift 2
        ;;
      -n | --column-ignore)
        CIGNORES=$2
        shift 2
        ;;
      -h | --host)
        SQL_HOST=$2
        shift 2
        ;;
      -o | --port)
        SQL_PORT=$2
        shift 2
        ;;
      -U | --username)
        SQL_USERNAME=$2
        shift 2
        ;;
      -P | --password)
        SQL_PASSWORD=$2
        shift 2
        ;;
      -D | --database)
        SQL_DATABASE=$2
        shift 2
        ;;
    *)
        break;;
  esac
done

if [ -z "$SQL_HOST" ]; then
    echo "need sql host"
    exit 1;
fi

if [ -z "$SQL_USERNAME" ]; then
    echo "need sql username"
    exit 1;
fi

if [ -z "$SQL_PASSWORD" ]; then
    echo "need sql password"
    exit 1;
fi
if [ -z "$SQL_DATABASE" ]; then
    echo "need sql db"
    exit 1;
fi

vars+=(-e WF_SQL_HOST="$SQL_HOST")
if [ ! -z "$SQL_PORT" ]; then
    vars+=(-e WF_SQL_PORT="$SQL_PORT")
fi
vars+=(-e WF_SQL_USER="$SQL_USERNAME")
vars+=(-e WF_SQL_PASSWORD="$SQL_PASSWORD")
vars+=(-e WF_SQL_DATABASE="$SQL_DATABASE")

if [ ! -z "$TIGNORES" ]; then
    vars+=(-e WF_TABLES_TO_IGNORE="$TIGNORES")
fi

if [ ! -z "$CIGNORES" ]; then
    vars+=(-e WF_COLUMNS_TO_IGNORE="$CIGNORES")
fi

if [ ! -z "$IMAGES" ]; then
    vars+=(-e WF_IMAGE_TABLES="$IMAGES")
fi

if [ ! -z "$PDFS" ]; then
    vars+=(-e WF_PDF_TABLES="$PDFS")
fi

vars+=(-e WF_LOG_LEVEL="INFO")
vars+=(--add-host host.docker.internal:host-gateway )
vars+=(-e DB_HOST="host.docker.internal")

echo docker run --rm -it ${vars[@]} aperturedata/workflows-ingest-from-sql
docker run --rm -it ${vars[@]} aperturedata/workflows-ingest-from-sql
