#!/bin/bash

# Define o mês e ano que queremos processar
ANO="2025"
MES="01"

# Loop de 1 a 31 (Janeiro)
for dia in {01..31}
do
    DATA_REF="$ANO-$MES-$(printf "%02d" $dia)"
    
    echo "--------------------------------------------------------"
    echo "Iniciando processamento do dia: $DATA_REF"
    echo "--------------------------------------------------------"

    # Comando spark-submit configurado com os recursos que validamos
    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --num-executors 15 \
      --executor-memory 12G \
      --executor-cores 4 \
      --driver-memory 8G \
      --conf spark.sql.shuffle.partitions=400 \
      --conf spark.sql.session.timeZone=America/Sao_Paulo \
      acervo_v3.py --modo historico --ref_date "$DATA_REF"

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
