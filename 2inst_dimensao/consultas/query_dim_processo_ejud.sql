with dim_processo_ejud as (
    SELECT 
        P.*,
        format_string('%07d-%02d.%s.%s.%02d.%04d',
            CAST(p.numsequencial AS BIGINT),
            CAST(p.dv AS BIGINT),
            p.numano,
            p.codjustica,
            CAST(p.codregi AS BIGINT),
            CAST(p.codlocregi AS BIGINT)
        ) AS cod_cnj,
        CASE 
            WHEN p.indsupersigil = 'S' THEN 'RESTRITO'
            ELSE format_string('%07d-%02d.%s.%s.%02d.%04d',
                CAST(p.numsequencial AS BIGINT),
                CAST(p.dv AS BIGINT),
                p.numano,
                p.codjustica,
                CAST(p.codregi AS BIGINT),
                CAST(p.codlocregi AS BIGINT)
            )
        END AS cod_cnj_sigilo,
        P.CODCOMPT as COD_COMPETENCIA,
        CASE 
            WHEN p.indsupersigil = 'S' THEN 5
            WHEN p.indsigil = 'S' THEN 4
            WHEN p.indsegrjust = 'S' THEN 1
            ELSE 0
        END AS id_sigilo,
        tp.codtipproc AS cod_tipo_processo,
        tp.codext AS cod_classe_externa,
        tp.descr AS nm_classe,
        tp.codextcnj AS cod_classe_cnj,
        CASE 
            WHEN c.codnat = 1 THEN 'S'
            ELSE 'N'
        END AS ind_civel,
        CASE 
            WHEN c.codnat = 2 THEN 'S'
            ELSE 'N'
        END AS ind_criminal,
        CASE 
            WHEN lpad(CAST(p.codlocregi AS STRING), 4, '0') = '0000' THEN 'S'
            ELSE 'N'
        END AS ind_originario,
        C.DESCR as nm_competencia,
        CASE 
            WHEN EXISTS (
                SELECT 1
                FROM ejud.processoassunto pa
                WHERE pa.coddoc = p.coddoc
                  AND pa.codass IN (
                    3415,3430,3432,3523,3524,3527,3529,3530,3531,3533,3534,3535,3536,3539,
                    3540,3541,3545,3547,3548,3550,3551,3552,3553,3554,3555,3556,3557,3558,
                    3560,3561,3562,3563,3564,3565,3566,3567,3568,3569,3570,3571,3572,3573,
                    3574,3576,3577,3579,3580,3582,3585,3589,3592,3593,3594,3595,3596,3597,
                    3598,3603,3604,3606,3612,3614,3628,3642,3650,3651,5571,5872,5873,5874,
                    5875,5876,5877,5903,5904,5905,5906,5907,5908,5909,5910,9674,9689,9691,
                    9704,9807,9808,9809,9811,9812,9813,9815,9816,9817,9818,9821,9822,9823,
                    9826,9828,9829,9830,9831,9832,9833,9834,9835,9836,9837,9838,9839,9840,
                    9841,9842,9843,9844,9845,9846,9847,9857,9872,9874,9888,9899,9903,9904,
                    9917,9918,9919,9920,9921,9922,9923,9924,9925,9926,9927,9928,9929,9930,
                    9931,9932,9934,9935,9937,9938,9940,9943,9947,9950,9951,9952,9953,9955,
                    9956,9957,9958,9959,9960,9961,9962,9963,10012,10013,10014,10982,10983,
                    10984,10985,10986,10990,10991,10993,10994,10995,10998,11005,11006,
                    11007,11068,11072,11073,11152,11166,11167,11797
                  )
            ) THEN 'S'
            ELSE 'N'
        END AS IND_CRIME_ADM_PUBLICO,
        'EJUD' AS ind_sistema
    FROM ejud.t_processo p
    LEFT JOIN ejud.tipoprocesso tp ON p.codtipproc = tp.codtipproc
    left join ejud.competencia c on p.codcompt = c.codcompt
), processo_assunto as (
    select
        pa.coddoc,
        a.codass as COD_ASSUNTO,
        a.descr as nm_assunto_principal
    from ejud.processoassunto pa
    join ejud.assunto a on pa.codass = a.codass
    where pa.numord = 1
), processo_dcp_ejud as (
    select 
        AA.CODDOC                                              as CODDOC,
        p.id_proc                                              as ID_PROCESSO_ORIGEM,
        cast(p.cod_classe as int)                              as COD_CLASSE_ORIGEM,
        cast(C.DESCR as STRING)                                as CLASSE_ORIGEM,
        pp.cod_cnj                                             as NUM_PROCESSO_ORIGEM,
        aa.codjustorig                                         as COD_JUSTICA_ORIGEM,
        aa.indorigpje                                          as IND_ORIGEM_PJE,
        aa.codvara                                             as COD_VARA_ORIGEM,
        P.nome                                                 as VARA_ORIGEM,
        case 
            when p.cod_classe = 15190 then 'S'
            else 'N'
        end                                                    as IND_DESTITUICAO_PODER_FAMILIAR,
        'DCP'                                                  as IND_SISTEMA_ORIGEM
    FROM dcp.processo p 
    left join (select id_proc, cod_cnj, cod_proc from proccorp.processo where cod_tip_proc = 1) pp on p.cod_proc = pp.cod_proc
    join (select CODDOC, NUMSEQ, NUMPROC, IDPROCCORP, INDORIGPJE, codjustorig, codvara from ejud.autoassociado where NUMSEQ = 1) aa on pp.id_proc = aa.idproccorp
    left join ejud.vara V on AA.CODVARA = V.CODVARA AND aa.codjustorig = v.codjustorig
    left join ejud.pessoa P on V.CODPESS = P.CODPESS
    left join dcp.classe C on P.COD_CLASSE = C.COD_CLASSE
), processo_pje_ejud as (
    select     
        AA.CODDOC,
        cp.ID_PROCESSO_TRF                                     as ID_PROCESSO_ORIGEM,
        cp.NR_PROCESSO                                         as NUM_PROCESSO_ORIGEM,
        cast(cp.id_classe_judicial as int)                     as ID_CLASSE_JUDICIAL,
        cast(cp.cd_classe_judicial as int)                     as COD_CLASSE_ORIGEM,
        cast(cp.ds_classe_judicial as STRING)                  as CLASSE_ORIGEM,
        aa.codjustorig                                         as COD_JUSTICA_ORIGEM,
        aa.indorigpje                                          as IND_ORIGEM_PJE,
        aa.codvara                                             as COD_VARA_ORIGEM,
        P.nome                                                 as VARA_ORIGEM,
        case 
            when cj.cd_classe_judicial in ('15193','15190','12230','1399') then 'S'
            else 'N'
        end                                                    as IND_DESTITUICAO_PODER_FAMILIAR,
        'PJE'                                                  as IND_SISTEMA_ORIGEM
    from pje.tb_cabecalho_processo cp
    join pje.tb_classe_judicial cj on cp.id_classe_judicial = cj.id_classe_judicial
    join (select pp.cod_cnj, pp.cod_proc, pp.cod_tip_proc from proccorp.processo pp where pp.cod_tip_proc = 13) pp on cp.nr_processo = pp.cod_cnj
    join (select CODDOC, NUMSEQ, NUMPROC, IDPROCCORP, INDORIGPJE, codjustorig, codvara from ejud.autoassociado where NUMSEQ = 1) aa on aa.numproc = pp.cod_proc
    left join ejud.vara V on AA.CODVARA = V.CODVARA AND aa.codjustorig = v.codjustorig
    left join ejud.pessoa P on V.CODPESS = P.CODPESS
)
SELECT 
	--IDENTIFICACAO DO PROCESSO
    p.coddoc                                                  as COD_DOCUMENTO,
    p.dtautua                                                 as DTH_AUTUACAO,
    p.COD_CNJ,
    p.COD_CNJ_SIGILO,
    p.ID_SIGILO,
    p.COD_TIPO_PROCESSO,
    --INDICADORES DE CLASSE
    p.COD_CLASSE_EXTERNA,
    p.NM_CLASSE,
    p.COD_CLASSE_CNJ,
    --INDICADORES DE COMPETENCIA
    p.COD_COMPETENCIA,
    p.NM_COMPETENCIA,
    p.IND_CRIMINAL,
    p.IND_CIVEL,
    --INDICADORES DE ASSUNTO
    pa.COD_ASSUNTO,
    pa.NM_ASSUNTO_PRINCIPAL,
    --INDICADORES DE ORIGEM
    P.ind_originario,
    case
        when pje.ID_PROCESSO_ORIGEM is not null or dcp.ID_PROCESSO_ORIGEM is not null then 'S'
        else 'N'
    end                                                       AS IND_AUTOASSOCIADO_ORIGEM,
    COALESCE(dcp.ID_PROCESSO_ORIGEM, pje.ID_PROCESSO_ORIGEM) AS ID_PROCESSO_RELACIONADO,
    COALESCE(dcp.NUM_PROCESSO_ORIGEM, pje.NUM_PROCESSO_ORIGEM) AS NUM_PROCESSO_ORIGEM,
    COALESCE(dcp.COD_CLASSE_ORIGEM, pje.COD_CLASSE_ORIGEM)   as COD_CLASSE_ORIGEM,
    COALESCE(dcp.CLASSE_ORIGEM, pje.CLASSE_ORIGEM)           as CLASSE_ORIGEM,
    coalesce(DCP.IND_SISTEMA_ORIGEM, PJE.IND_SISTEMA_ORIGEM) as IND_SISTEMA_ORIGEM,
    COALESCE(dcp.COD_VARA_ORIGEM, pje.COD_VARA_ORIGEM)       AS COD_VARA_ORIGEM,
    COALESCE(dcp.VARA_ORIGEM, pje.VARA_ORIGEM)               AS VARA_ORIGEM,  
    --INDICADORES DO PROCESSO
    p.INDPRIOIDOSO                                            AS IND_PRIORIDADE_IDOSO,
    P.INDPRIOIDOSO80                                          AS IND_PRIORIDADE_IDOSO_80,
    P.INDDOENCAGRAVE                                          AS IND_DOENCA_GRAVE,
    P.INDPRIODEFIC                                            AS IND_PRIORIDADE_DEFICIENTE,
    P.INDMENOR                                                AS IND_MENOR,
    P.INDMENORRECOL                                           AS IND_MENOR_RECOLHIDO,
    P.INDJUSTGRATI                                            as IND_JUSTICA_GRATUITA,   
    p.INDJUIZODIGITAL										  AS IND_JUIZO_DIGITAL,
    p.INDORIGPJEACAOCOLE									  AS IND_ORIGEM_PJE_ACAO_COLETIVA,
    p.INDPROCADMDISC										  AS IND_PROCESSO_ADMNISTRATIVO_DISCIPLINAR,
    p.INDPEDLIMIN											  AS IND_PEDIDO_LIMINAR,
    --p.INDTEMPS não sei do q se trata											  
    --p.INDPREP, não sei do q se trata	
    p.INDELETR												  AS IND_ELETROINICO,
    p.INDMP													  AS IND_MINISTERIO_PUBLICO,
    p.INDURGENTE											  AS IND_URGENTE,
    --p.INDNUMANTVIS, não sei do que se trata
    p.INDSEGRJUST											  AS IND_SEGREDO_JUSTICA,
    p.INDSIGIL												  AS IND_SIGILOSO,
    p.INDDEFENPUB											  AS IND_DEFENSORIA_PUBLICA,
    --p.INDPROCORIG, processo de origem?
    --p.IND3VPIMPED, não sei
    p.INDEFEITOSUSP											  AS IND_EFEITO_SUSPENSIVO,
    p.INDRITOSUM											  AS IND_RITO_SUMARIO,
    p.INDPGE												  AS IND_PROCURADORIA_GERAL_ESTADO,
    p.INDPGM												  AS IND_PROCURADORIA_GERAL_MUNICIPIO,
--    p.INDDIMINCOMPLEX, --não sei
--    p.INDAUTUASEP, --não sei
--    p.INDDELITOINDETER, --não sei
--    p.INDRECINATIVO, --recurso inativo
--    p.INDSEESP, --não sei
    p.INDREUPRESOPJE										  AS IND_REU_PRESO,
    p.INDSUPERSIGIL											  AS IND_SUPER_SIGILOSO,
--    p.INDSECIV, --não sei
--    p.INDSEPRI, --não sei
--    p.INDCONF, --não sei
--    p.INDIMPRDISTR, --não sei
--    p.INDINT, --não sei
--    p.INDPREF, --não sei
--    p.INDADMITREV, --não sei
    p.INDSITRUA												 AS IND_SITUACAO_RUA,
    case 
        when COALESCE(DCP.IND_DESTITUICAO_PODER_FAMILIAR, DCP.IND_DESTITUICAO_PODER_FAMILIAR) is null then 'N'
        else COALESCE(DCP.IND_DESTITUICAO_PODER_FAMILIAR, DCP.IND_DESTITUICAO_PODER_FAMILIAR)
    end                                                       as IND_DESTITUICAO_PODER_FAMILIAR,
    p.IND_CRIME_ADM_PUBLICO,
    p.IND_SISTEMA,
    current_timestamp AS dth_etl
from dim_processo_ejud p
left join processo_assunto pa on p.coddoc = pa.coddoc
left join processo_pje_ejud pje on p.coddoc = pje.coddoc
left join processo_dcp_ejud dcp on p.coddoc = dcp.coddoc;