#!/bin/bash

echo "Iniciando carga: TB_FT_VIRTUAL_LOCALIZADORES"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --jars /usr/share/aws/iceberg/lib/iceberg-spark-runtime-3.5_2.12-1.7.1-amzn-2.jar \
  s3://sgdai-projetos/2instancia/2inst_acervo_aws/scripts/virtual_localizadores.py

if [ $? -eq 0 ]; then
    echo "SUCESSO: TB_FT_VIRTUAL_LOCALIZADORES carregada."
else
    echo "ERRO: Falha na carga de TB_FT_VIRTUAL_LOCALIZADORES."
    exit 1
fi
