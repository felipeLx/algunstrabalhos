-- ============================================================
-- EJUD — Cleanup: Remove codnat=2 (criminal) conhecimento processes
-- These codextcnj codes belong to CLASSE_CONHECIMENTO but have
-- codnat=2 (criminal variant) — they must NOT be in conhecimento metas.
-- Confirmed problematic codextcnj: 183, 202, 307, 325, 417, 419, 421,
--                                  426, 428, 432, 1208, 1710
-- ============================================================

-- ── 1. Count affected rows before deletion (prata staging) ──────────────────

SELECT cod_indicador, count(*) AS n_to_delete
FROM dw_2inst_prata.tb_2inst_metacnj
WHERE meta_ano = '2026'
  AND cod_sistema = 1
  AND cod_indicador NOT IN (1, 2, 3, 4, 5)
  AND id_processo IN (
      SELECT DISTINCT p.nrproc
      FROM ejud_bronze.processo p
      JOIN ejud_bronze.tipoprocesso tp ON tp.codtipproc = p.codtipproc
      WHERE trim(tp.codextcnj) IN ('183','202','307','325','417','419','421',
                                    '426','428','432','1208','1710')
        AND cast(trim(tp.codnat) AS int) = 2
  )
GROUP BY cod_indicador
ORDER BY cod_indicador;

-- ── 2. Count affected rows before deletion (ouro production) ────────────────

SELECT cod_indicador, count(*) AS n_to_delete
FROM dw_2inst_ouro.tb_ft_meta_analitico
WHERE meta_ano = 2026
  AND cod_indicador NOT IN (1, 2, 3, 4, 5)
  AND id_processo IN (
      SELECT DISTINCT p.nrproc
      FROM ejud_bronze.processo p
      JOIN ejud_bronze.tipoprocesso tp ON tp.codtipproc = p.codtipproc
      WHERE trim(tp.codextcnj) IN ('183','202','307','325','417','419','421',
                                    '426','428','432','1208','1710')
        AND cast(trim(tp.codnat) AS int) = 2
  )
GROUP BY cod_indicador
ORDER BY cod_indicador;

-- ── 3. MERGE DELETE from prata staging ──────────────────────────────────────
-- Run after confirming counts above.

MERGE INTO dw_2inst_prata.tb_2inst_metacnj AS target
USING (
    SELECT DISTINCT p.nrproc AS id_processo
    FROM ejud_bronze.processo p
    JOIN ejud_bronze.tipoprocesso tp ON tp.codtipproc = p.codtipproc
    WHERE trim(tp.codextcnj) IN ('183','202','307','325','417','419','421',
                                  '426','428','432','1208','1710')
      AND cast(trim(tp.codnat) AS int) = 2
) AS src
ON  target.id_processo = src.id_processo
AND target.meta_ano    = '2026'
AND target.cod_sistema = 1
AND target.cod_indicador NOT IN (1, 2, 3, 4, 5)
WHEN MATCHED THEN DELETE;

-- ── 4. MERGE DELETE from ouro production ────────────────────────────────────
-- Run after confirming counts above.

MERGE INTO dw_2inst_ouro.tb_ft_meta_analitico AS target
USING (
    SELECT DISTINCT p.nrproc AS id_processo
    FROM ejud_bronze.processo p
    JOIN ejud_bronze.tipoprocesso tp ON tp.codtipproc = p.codtipproc
    WHERE trim(tp.codextcnj) IN ('183','202','307','325','417','419','421',
                                  '426','428','432','1208','1710')
      AND cast(trim(tp.codnat) AS int) = 2
) AS src
ON  target.id_processo = src.id_processo
AND target.meta_ano    = 2026
AND target.cod_indicador NOT IN (1, 2, 3, 4, 5)
WHEN MATCHED THEN DELETE;
