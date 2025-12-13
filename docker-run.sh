#!/bin/bash

# Script para construir y ejecutar el contenedor de desarrollo

echo "=== Construyendo imagen Docker ==="
docker build -t medallion-env .

echo ""
echo "=== Iniciando contenedor ==="
echo "Puertos expuestos:"
echo "  - Airflow webserver: http://localhost:8080"
echo ""

docker run -it --rm \
  -v $(pwd):/workspace \
  -p 8080:8080 \
  --name medallion-dev \
  medallion-env

echo ""
echo "Contenedor finalizado"
