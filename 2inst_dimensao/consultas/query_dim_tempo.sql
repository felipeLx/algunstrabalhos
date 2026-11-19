-- Dimensão de Tempo (1900-01-01 a 2100-12-31)
WITH calendario AS (
  SELECT
    explode(
      sequence(
        to_date('1900-01-01'),
        to_date('2100-12-31'),
        interval 1 day
      )
    ) AS dt
),
feriados AS (
  SELECT DISTINCT
    to_date(DTFER) AS dt
  FROM glue_catalog.ejud.feriado
  WHERE DTFER IS NOT NULL
)
SELECT
  c.dt                                                     AS dt,
  CAST(date_trunc('month', c.dt) AS DATE)                  AS dt_inicio_mes,
  CAST(last_day(c.dt) AS DATE)                             AS dt_fim_mes,

  CAST(year(c.dt) AS INT)                                  AS ano,
  CAST(month(c.dt) AS INT)                                 AS mes,
  CAST(day(c.dt) AS INT)                                   AS dia,

  CAST(dayofweek(c.dt) AS INT) AS dia_semana_num,

  CASE dayofweek(c.dt)
    WHEN 1 THEN 'Domingo'
    WHEN 2 THEN 'Segunda-feira'
    WHEN 3 THEN 'Terça-feira'
    WHEN 4 THEN 'Quarta-feira'
    WHEN 5 THEN 'Quinta-feira'
    WHEN 6 THEN 'Sexta-feira'
    WHEN 7 THEN 'Sábado'
  END AS descricao_dia_semana,

  CAST(quarter(c.dt) AS INT)                               AS trimestre,
  CAST(CASE WHEN month(c.dt) <= 6 THEN 1 ELSE 2 END AS INT) AS semestre,

  CASE WHEN c.dt = last_day(c.dt) THEN 'S' ELSE 'N' END    AS ind_final_mes,

  CASE
    WHEN ( (year(c.dt) % 400 = 0)
        OR (year(c.dt) % 4 = 0 AND year(c.dt) % 100 <> 0) )
    THEN 'S' ELSE 'N'
  END                                                      AS ano_bissexto,

  date_format(c.dt, 'yyyy-MM')                             AS ano_mes,

  CASE WHEN f.dt IS NOT NULL THEN 'S' ELSE 'N' END         AS feriado,

  CAST(
    CASE
      WHEN dayofweek(c.dt) IN (1,7) OR f.dt IS NOT NULL THEN 0
      ELSE 1
    END
  AS INT)                                                  AS dia_util,

  CASE month(c.dt)
    WHEN 1  THEN 'Janeiro'   WHEN 2  THEN 'Fevereiro' WHEN 3  THEN 'Março'
    WHEN 4  THEN 'Abril'     WHEN 5  THEN 'Maio'      WHEN 6  THEN 'Junho'
    WHEN 7  THEN 'Julho'     WHEN 8  THEN 'Agosto'    WHEN 9  THEN 'Setembro'
    WHEN 10 THEN 'Outubro'   WHEN 11 THEN 'Novembro'  WHEN 12 THEN 'Dezembro'
  END                                                      AS descricao_mes

FROM calendario c
LEFT JOIN feriados f
  ON c.dt = f.dt
;

