with dim_processo_ejud as (
SELECT 
        p.coddoc AS cod_documento,
		TO_CHAR(p.numsequencial, 'FM0000000') || '-' ||
		TO_CHAR(p.dv, 'FM00') || '.' ||
		p.numano || '.' ||
		p.codjustica || '.' ||
		TO_CHAR(p.codregi, 'FM00') || '.' ||
		TO_CHAR(p.codlocregi, 'FM0000') AS cod_cnj,
    CASE 
        WHEN p.indsupersigil = 'S' THEN 'RESTRITO'
        ELSE 
		TO_CHAR(p.numsequencial, 'FM0000000') || '-' ||
		TO_CHAR(p.dv, 'FM00') || '.' ||
		p.numano || '.' ||
		p.codjustica || '.' ||
		TO_CHAR(p.codregi, 'FM00') || '.' ||
		TO_CHAR(p.codlocregi, 'FM0000') END AS cod_cnj_sigilo,   
    CASE 
        WHEN p.indsupersigil = 'S' THEN 5
        WHEN p.indsigil = 'S' THEN 4
        WHEN p.indsegrjust = 'S' THEN 1
        ELSE 0
    END AS id_sigilo,   
    tp.codtipproc AS cod_classe_interna,
    tp.codext AS cod_classe_externa,
    tp.codextcnj AS cod_classe_cnj,
    tp.descr AS nm_classe,    
    CASE 
        WHEN LPAD(TO_CHAR(p.codlocregi, '9999'), 4, '0') = '0000' THEN 'S'
        ELSE 'N'
    END AS ind_originario,    
    p.indprioidoso AS ind_prioridade_idoso,
    p.indprioidoso80 AS ind_prioridade_idoso_80,
    p.inddoencagrave AS ind_doenca_grave,
    p.indpriodefic AS ind_prioridade_deficiente,
    p.indmenor as ind_menor,
    p.indmenorrecol as ind_menor_recolhido,
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
    ELSE 'N' end as IND_CRIME_ADM_PUBLICO,
    'EJUD' AS ind_sistema,
    p.dtautua as dth_autuacao
FROM ejud.t_processo p
LEFT JOIN ejud.tipoprocesso tp 
    ON p.codtipproc = tp.codtipproc
where p.dtautua <= '{v_dateref_str}' 
)
SELECT 
    CAST(COD_DOCUMENTO AS NUMERIC(30,0)) AS COD_DOCUMENTO,
    CAST(COD_CNJ AS VARCHAR(30)) AS COD_CNJ,
    CAST(COD_CNJ_SIGILO AS VARCHAR(30)) AS COD_CNJ_SIGILO,
    CAST(ID_SIGILO AS INTEGER) AS ID_SIGILO,
    CAST(COD_CLASSE_INTERNA AS INTEGER) AS COD_CLASSE_INTERNA,
    CAST(COD_CLASSE_EXTERNA AS INTEGER) AS COD_CLASSE_EXTERNA,
    CAST(COD_CLASSE_CNJ AS INTEGER) AS COD_CLASSE_CNJ,
    CAST(NM_CLASSE AS VARCHAR(100)) AS NM_CLASSE,
    CAST(IND_ORIGINARIO AS CHAR(1)) AS IND_ORIGINARIO,
    CAST(IND_PRIORIDADE_IDOSO AS CHAR(1)) AS IND_PRIORIDADE_IDOSO,
    CAST(IND_PRIORIDADE_IDOSO_80 AS CHAR(1)) AS IND_PRIORIDADE_IDOSO_80,
    CAST(IND_DOENCA_GRAVE AS CHAR(1)) AS IND_DOENCA_GRAVE,
    CAST(IND_PRIORIDADE_DEFICIENTE AS CHAR(1)) AS IND_PRIORIDADE_DEFICIENTE,
    CAST(IND_MENOR AS CHAR(1)) AS IND_MENOR,
    CAST(IND_MENOR_RECOLHIDO AS CHAR(1)) AS IND_MENOR_RECOLHIDO,
    CAST(DTH_AUTUACAO AS TIMESTAMP) AS DTH_AUTUACAO,
    CAST(IND_CRIME_ADM_PUBLICO AS CHAR(1)) AS IND_CRIME_ADM_PUBLICO,
    CAST(IND_SISTEMA AS VARCHAR(5)) AS IND_SISTEMA
FROM dim_processo_ejud
