/*
Query to create the analytic table for Ejud Meta 01 data, joining meta data with bronze information
*/
WITH meta AS (
    SELECT 
        meta.id_processo as processo_id,
        meta.id_classe_judicial as cod_classe,
        meta.id_sigilo as nivel_sigilo,
        pr.codmag_ejud as cod_magistrado,
        meta.id_orgao_colegiado as cod_orgao_julgador,
        1 as distribuido,
        meta.dth_inicio as dth_distribuicao,
        meta.situacao,
        meta.indicador,
        TODAY() as dth_referencia,
        meta.num_processo
    FROM 
        parquet.`${export_path}/data/prata/ejud/meta_cnj2025/meta_01/current/cnm01.parquet/*` as meta
    JOIN
        ouro.`${export_path}/data/ouro/din2inst/dim_magistrado/*.parquet` as pr
    ON
        meta.id_orgao_juizo = pr.codmag_eproc
),
last_mov_relator as (
    select coddoc, codmagrel from (
        select coddoc, codmagrel,
        row_number() over (partition by coddoc order by dtmov desc) as rn
        from bronze.`${export_path}/data/bronze/ejud/movimentorelator/**/*.parquet/*` as pr
        join meta on pr.coddoc = meta.coddoc
    ) where rn = 1
) select mr.codmagrel as cod_magistrado, m.* from meta m
join last_mov_relator mr on m.coddoc = mr.coddoc
