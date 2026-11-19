/*
Query to create the analytic table for Ejud Meta 01 data, joining meta data with bronze information
*/
WITH meta AS (
    SELECT 
        meta.coddoc as processo_id,
        meta.cod_classe,
        meta.nivel_sigilo,
        1 as distribuido,
        meta.dthrmov as dth_distribuicao,
        meta.situacao,
        meta.indicador,
        TODAY() as dth_referencia,
        (CASE LENGTH(to_char(NUMSEQUENCIAL))
            WHEN 1
                THEN '000000' || to_char(NUMSEQUENCIAL) 
                   || (
                   case length(to_char(DV)) 
                    when 1 then '0' || to_char(dv)
                    when 2 then to_char(dv)
                    else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                       || 
                       (CASE LENGTH(to_char(codlocregi))
                           WHEN 0
                           THEN '0000'  || to_char(CODLOCREGI)
                           WHEN 1
                            THEN '000'  || to_char(CODLOCREGI)
                           WHEN 2
                            THEN '00'  || to_char(CODLOCREGI)
                           WHEN 3
                            THEN '0'  || to_char(CODLOCREGI)
                           ELSE to_char(codlocregi)
                        END) 
                WHEN 2
                   THEN '00000' || to_char(NUMSEQUENCIAL) 
                   || (
                   case length(to_char(DV)) 
                    when 1 then '0' || to_char(dv)
                    when 2 then to_char(dv)
                    else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                       || 
                       (CASE LENGTH(to_char(codlocregi))
                           WHEN 0
                           THEN '0000'  || to_char(CODLOCREGI)
                           WHEN 1
                            THEN '000'  || to_char(CODLOCREGI)
                           WHEN 2
                            THEN '00'  || to_char(CODLOCREGI)
                           WHEN 3
                            THEN '0'  || to_char(CODLOCREGI)
                           ELSE to_char(codlocregi)
                        END) 
               WHEN 3
                   THEN '0000' || to_char(NUMSEQUENCIAL) 
                   || (
                       case length(to_char(DV)) 
                        when 1 then '0' || to_char(dv)
                        when 2 then to_char(dv)
                        else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                       || 
                       (CASE LENGTH(to_char(codlocregi))
                           WHEN 0
                           THEN '0000'  || to_char(CODLOCREGI)
                           WHEN 1
                            THEN '000'  || to_char(CODLOCREGI)
                           WHEN 2
                            THEN '00'  || to_char(CODLOCREGI)
                           WHEN 3
                            THEN '0'  || to_char(CODLOCREGI)
                           ELSE to_char(codlocregi)
                        END) 
               WHEN 4
               THEN '000' || to_char(NUMSEQUENCIAL) 
               || (
                   case length(to_char(DV)) 
                    when 1 then '0' || to_char(dv)
                    when 2 then to_char(dv)
                    else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                   || 
                  (CASE LENGTH(to_char(codlocregi))
                       WHEN 0
                       THEN '0000'  || to_char(CODLOCREGI)
                       WHEN 1
                        THEN '000'  || to_char(CODLOCREGI)
                       WHEN 2
                        THEN '00'  || to_char(CODLOCREGI)
                       WHEN 3
                        THEN '0'  || to_char(CODLOCREGI)
                       ELSE to_char(codlocregi)
                    END)
                WHEN 5
              THEN '00' || to_char(NUMSEQUENCIAL) 
                || (
                   case length(to_char(DV)) 
                    when 1 then '0' || to_char(dv)
                    when 2 then to_char(dv)
                    else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                   || 
                   (CASE LENGTH(to_char(codlocregi))
                       WHEN 0
                       THEN '0000'  || to_char(CODLOCREGI)
                       WHEN 1
                        THEN '000'  || to_char(CODLOCREGI)
                       WHEN 2
                        THEN '00'  || to_char(CODLOCREGI)
                       WHEN 3
                        THEN '0'  || to_char(CODLOCREGI)
                       ELSE to_char(codlocregi)
                    END)
                WHEN 6
                THEN '0' || to_char(NUMSEQUENCIAL) 
                    || (
                   case length(to_char(DV)) 
                    when 1 then '0' || to_char(dv)
                    when 2 then to_char(dv)
                    else null end)
                        to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                   || 
                   (CASE LENGTH(to_char(codlocregi))
                           WHEN 0
                           THEN '0000'  || to_char(CODLOCREGI)
                           WHEN 1
                            THEN '000'  || to_char(CODLOCREGI)
                           WHEN 2
                            THEN '00'  || to_char(CODLOCREGI)
                           WHEN 3
                            THEN '0'  || to_char(CODLOCREGI)
                           ELSE to_char(codlocregi)
                    END)
                    WHEN 7
                    THEN to_char(NUMSEQUENCIAL) 
                        || (
                       case length(to_char(DV)) 
                        when 1 then '0' || to_char(dv)
                        when 2 then to_char(dv)
                        else null end)
                            to_char(NUMANO)  to_char(CODJUSTICA)  to_char(CODREGI) 
                       || 
                       (CASE LENGTH(to_char(codlocregi))
                               WHEN 0
                               THEN '0000'  || to_char(CODLOCREGI)
                               WHEN 1
                                THEN '000'  || to_char(CODLOCREGI)
                               WHEN 2
                                THEN '00'  || to_char(CODLOCREGI)
                               WHEN 3
                                THEN '0'  || to_char(CODLOCREGI)
                               ELSE to_char(codlocregi)
                        END)
                ELSE to_char(NUMSEQUENCIAL) END) as num_processo
    FROM 
        parquet.`${export_path}/data/prata/ejud/meta_cnj2025/meta_01/current/cnm01.parquet/*` as meta
    JOIN
        bronze.`${export_path}/data/bronze/ejud/t_processo/**/*.parquet/*` as pr
    ON
        meta.coddoc = pr.coddoc
),
last_mov_relator as (
    select coddoc, codmagrel from (
        select coddoc, codmagrel, coddorgjulg,
        row_number() over (partition by coddoc order by dtmov desc) as rn
        from bronze.`${export_path}/data/bronze/ejud/movimentorelator/**/*.parquet/*` as pr
        join meta on pr.coddoc = meta.coddoc
    ) where rn = 1
) select mr.codmagrel as cod_magistrado, mr.codorgjulg as cod_orgao_julgador, m.* from meta m
join last_mov_relator mr on m.coddoc = mr.coddoc
