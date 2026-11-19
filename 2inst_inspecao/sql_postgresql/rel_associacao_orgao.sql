INSERT INTO insp2inst.rel_associacao_orgao (
    cod_orgao_antigo,
    cod_orgao_novo,
    ind_ejud,
    ind_eproc2g
)

WITH orgao_ejud AS (
    SELECT 
        oj.codorgjulg AS codorg_antigo,
        COALESCE(c.codorgjulgespec, oj.codorgjulg) AS codorg_novo
    FROM ejud.orgaojulgador oj
    JOIN scol.orgao so 
      ON oj.codorgjulg = so.cod_org 
    LEFT JOIN ejud.camaracivelespecializada c 
      ON oj.codorgjulg = c.codorgjulgciv
), 
orgao_eproc AS (
    SELECT 
        id_orgao AS codorg,
        cod_orgao
    FROM eproc2g.orgao eo
    JOIN scol.orgao so 
      ON eo.id_orgao = so.cod_org 
)
SELECT  
    COALESCE(ejud.codorg_antigo, eproc.codorg) AS cod_orgao_antigo,
    COALESCE(ejud.codorg_novo,   eproc.codorg) AS cod_orgao_novo,
    CASE WHEN ejud.codorg_antigo IS NOT NULL THEN 'S' ELSE 'N' END AS ind_ejud,
    CASE WHEN eproc.codorg IS NOT NULL THEN 'S' ELSE 'N' END AS ind_eproc
FROM orgao_ejud ejud
FULL OUTER JOIN orgao_eproc eproc ON ejud.codorg_antigo = eproc.codorg
union all 
select 
	0 as cod_orgao_antigo,
	0 as cod_orgao_novo,
	'?' as ind_ejud,
	'?' as ind_eproc2g

ON CONFLICT (cod_orgao_antigo)
DO UPDATE SET
    cod_orgao_novo = EXCLUDED.cod_orgao_novo,
    ind_ejud = EXCLUDED.ind_ejud,
    ind_eproc2g = EXCLUDED.ind_eproc2g;