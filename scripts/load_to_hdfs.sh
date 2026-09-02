#!/usr/bin/env bash
set -euo pipefail

# Carga el dataset TRM (datos.gov.co) a HDFS dentro del contenedor namenode.
# Uso: ./scripts/load_to_hdfs.sh <ruta_local_csv> [ruta_hdfs_destino]

CSV_PATH="${1:?Uso: $0 <ruta_local_csv> [ruta_hdfs_destino]}"
HDFS_DEST="${2:-/data}"
FILENAME=$(basename "$CSV_PATH")

if [ ! -f "$CSV_PATH" ]; then
  echo "Error: no se encontró el archivo $CSV_PATH" >&2
  exit 1
fi

echo "Creando directorio HDFS: $HDFS_DEST"
docker exec namenode hdfs dfs -mkdir -p "$HDFS_DEST"

echo "Copiando $FILENAME al contenedor namenode..."
docker cp "$CSV_PATH" "namenode:/tmp/$FILENAME"

echo "Subiendo a HDFS..."
docker exec namenode hdfs dfs -put -f "/tmp/$FILENAME" "$HDFS_DEST/"

echo "Verificando carga:"
docker exec namenode hdfs dfs -ls "$HDFS_DEST/"

echo "Listo. Ruta HDFS: hdfs://namenode:8020${HDFS_DEST}/${FILENAME}"

chmod +x scripts/load_to_hdfs.sh