#!/usr/bin/env bash
set -euo pipefail

# Carga un CSV local a HDFS dentro del contenedor namenode.
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

# El job de Spark corre como usuario 'spark' y debe poder escribir sus
# salidas bajo este directorio; la carga se hace como 'root' (755 por defecto).
echo "Ajustando permisos de escritura en $HDFS_DEST..."
docker exec namenode hdfs dfs -chmod -R 777 "$HDFS_DEST"

echo "Limpiando archivo temporal en el contenedor..."
docker exec namenode rm -f "/tmp/$FILENAME"

echo "Verificando carga:"
docker exec namenode hdfs dfs -ls "$HDFS_DEST/"

echo "Listo. Ruta HDFS: hdfs://namenode:8020${HDFS_DEST}/${FILENAME}"