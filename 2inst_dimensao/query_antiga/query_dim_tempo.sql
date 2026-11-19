-- Gera calendário 1900-01-01 a 2200-12-31 em Spark SQL
WITH params AS (
  SELECT to_date('1900-01-01') AS dt_ini, to_date('2200-12-31') AS dt_fim
),
cal AS (
  SELECT explode(sequence(dt_ini, dt_fim, interval 1 day)) AS dt_base
  FROM params
),
fer AS (
  -- opcional: se tiver tabela/arquivo de feriados
  SELECT DISTINCT CAST(dtfer AS DATE) AS dtfer
  FROM feriado
)
SELECT
  c.dt_base,
  CAST(date_trunc('month', c.dt_base) AS DATE)                AS dt_inicio_mes,
  last_day(c.dt_base)                                         AS dt_fim_mes,
  year(c.dt_base)                                             AS ano,
  month(c.dt_base)                                            AS mes,
  day(c.dt_base)                                              AS dia,
  quarter(c.dt_base)                                          AS trimestre,
  CASE WHEN month(c.dt_base) <= 6 THEN 1 ELSE 2 END           AS semestre,
  CASE WHEN c.dt_base = last_day(c.dt_base) THEN 'S' ELSE 'N' END AS ind_final_mes,
  CASE
    WHEN ( (year(c.dt_base) % 400 = 0)
        OR (year(c.dt_base) % 4 = 0 AND year(c.dt_base) % 100 <> 0) )
    THEN 'S' ELSE 'N'
  END                                                         AS ano_bissexto,
  date_format(c.dt_base, 'yyyy-MM')                           AS ano_mes,
  CASE WHEN f.dtfer IS NOT NULL THEN 'S' ELSE 'N' END         AS feriado,
  CASE
    WHEN dayofweek(c.dt_base) IN (1,7) OR f.dtfer IS NOT NULL THEN 0
    ELSE 1
  END                                                         AS dia_util,
  CASE month(c.dt_base)
    WHEN 1  THEN 'Janeiro'   WHEN 2  THEN 'Fevereiro' WHEN 3  THEN 'Março'
    WHEN 4  THEN 'Abril'     WHEN 5  THEN 'Maio'      WHEN 6  THEN 'Junho'
    WHEN 7  THEN 'Julho'     WHEN 8  THEN 'Agosto'    WHEN 9  THEN 'Setembro'
    WHEN 10 THEN 'Outubro'   WHEN 11 THEN 'Novembro'  WHEN 12 THEN 'Dezembro'
  END                                                         AS descricao_mes
FROM cal c
LEFT JOIN fer f ON f.dtfer = c.dt_base;

