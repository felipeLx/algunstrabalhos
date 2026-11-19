#!/bin/bash

# Define o mês e ano que queremos processar
ANO="2025"
MES="03"

# Loop de 1 a 31 (Janeiro)
for dia in {08..09}
do
    DATA_REF="$ANO-$MES-$dia)"
    
    echo "--------------------------------------------------------"
    echo "Iniciando processamento do dia: $DATA_REF"
    echo "--------------------------------------------------------"

    # Comando spark-submit configurado com os recursos que validamos
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --num-executors 12 \
      --executor-memory 10G \
      --executor-cores 4 \
      --driver-memory 4G \
      --conf spark.sql.shuffle.partitions=400 \
      --conf spark.sql.session.timeZone=America/Sao_Paulo \
      s3://sgdai-projetos/2instancia/acervo/scripts/acervo_v6.py \
      --modo historico --ref_date "$DATA_REF"

    # Verifica se o comando anterior teve sucesso antes de prosseguir
    if [ $? -eq 0 ]; then
        echo "SUCESSO: Dia $DATA_REF concluído."
    else
        echo "ERRO: Falha no processamento do dia $DATA_REF. Interrompendo loop."
        exit 1
    fi

    # Pausa de 10 segundos para o YARN limpar o overhead de rede/memória
    sleep 10
done

echo "Processamento de Janeiro de 2026 finalizado com sucesso!"


