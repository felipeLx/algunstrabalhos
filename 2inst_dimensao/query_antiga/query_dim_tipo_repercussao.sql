WITH bridge AS (
  SELECT 2 AS cod_unificado, 'Tema' AS desc_unificada, 9 AS cod_eproc_orig
  UNION ALL SELECT 2, 'Tema', 13
  UNION ALL SELECT 3, 'IRDR', 19
  UNION ALL SELECT 3, 'IRDR', 27
  UNION ALL SELECT 4, 'IAC', 14
  UNION ALL SELECT 4, 'IAC', 17
  UNION ALL SELECT 4, 'IAC', 22
  UNION ALL SELECT 4, 'IAC', 26
  UNION ALL SELECT 5, 'GR', 28
  UNION ALL SELECT 6, 'SIRDR', 15
  UNION ALL SELECT 6, 'SIRDR', 16
  UNION ALL SELECT 7, 'ADI', 30
  UNION ALL SELECT 7, 'ADI', 68
  UNION ALL SELECT 8, 'ADC', 31
  UNION ALL SELECT 8, 'ADC', 69
  UNION ALL SELECT 9, 'ADO', 33
  UNION ALL SELECT 10, 'ADPF', 32
  UNION ALL SELECT 11, 'PUIL', 29
)

SELECT
  CAST(e.cod_tipo_repercussao AS INT) AS cod_unificado,
  e.desc_tipo_repercussao AS desc_unificada,
  'EJUD' AS sistema,
  CAST(e.cod_tipo_repercussao AS INT) AS cod_original
FROM ejud_src e

UNION ALL

SELECT
  b.cod_unificado,
  b.desc_unificada,
  'EPROC' AS sistema,
  e.cod_tipo_repercussao AS cod_original
FROM eproc_src e
JOIN bridge b
  ON e.cod_tipo_repercussao = b.cod_eproc_orig

ORDER BY cod_unificado, sistema;
