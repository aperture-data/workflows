#!/bin/bash
ARGS=$(getopt -o i:p:e:n:h:o:U:P:D:L:XE:F: -l images:,pdfs:,table-ignore:,column-ignore:,host:,port:,username:,password:,database:,url-columns:,automate-foreign-keys,entity-map:,fk-map: -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1
fi

vars=()

IMAGES=''
PDFS=''
TIGNORES=''
CIGNORES=''
URLS=''
SQL_HOST=''
SQL_PORT=''
SQL_USERNAME=''
SQL_PASSWORD=''
SQL_DATABASE=''
AUTO_FK=''
ENTITY_MAP=''
FK_MAP=''
eval set -- "$ARGS"
while [ : ]; do
  case "$1" in
      -i | --images)
        IMAGES=$2
        shift 2
        ;;
      -p | --pdfs)
        PDFS="$2"
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
      -L | --url-columns)
        URLS=$2
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
      -X | --automate-foreign-keys)
        AUTO_FK="TRUE"
        shift
        ;;
      -E | --entity-map)
        ENTITY_MAP=$2
        shift 2
        ;;
      -F | --fk-map)
        FK_MAP=$2
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

# append commandline arguments that are set into env vars for docker.
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

if [ ! -z "$URLS" ]; then
    vars+=(-e WF_URL_COLUMNS_FOR_BINARY_DATA="$URLS")
fi

if [ ! -z "$AUTO_FK" ]; then
    vars+=(-e WF_AUTOMATIC_FOREIGN_KEY="$AUTO_FK")
fi

if [ ! -z "$ENTITY_MAP" ]; then
    vars+=(-e WF_TABLE_TO_ENTITY_MAPPING="$ENTITY_MAP")
fi

if [ ! -z "$FK_MAP" ]; then
    vars+=(-e WF_FOREIGN_KEY_ENTITY_MAPPING="$FK_MAP")
fi

vars+=(-e WF_LOG_LEVEL="INFO")
vars+=(--add-host host.docker.internal:host-gateway )
vars+=(-e DB_HOST="host.docker.internal")
#vars+=(-e DB_PORT="55551" )

echo docker run --rm -it ${vars[@]} aperturedata/workflows-ingest-from-sql
docker run --rm -it ${vars[@]} aperturedata/workflows-ingest-from-sql
