INSERT INTO prata.tf_distribuicao_2inst
WITH distribuicao 
       AS (
         SELECT pe.dth_evento AS dth_distribuicao,
                pe.id_processo,
                ej.cod_mni_movimento,
                concat(
                  substr(p.num_processo_20,  1, 7), '-',
                  substr(p.num_processo_20,  8, 2), '.',
                  substr(p.num_processo_20, 10, 4), '.',
                  substr(p.num_processo_20, 14, 1), '.',
                  substr(p.num_processo_20, 15, 2), '.',
                  substr(p.num_processo_20, 17, 4)
                ) AS num_processo,
                hdj.cod_magistrado
           FROM eproc2g.processo_evento pe
             JOIN eproc2g.historico_distribuicao_judicial hdj ON hdj.id_processo_evento = pe.id_processo_evento
             LEFT JOIN eproc2g.evento_judicial ej             ON pe.id_evento_judicial = ej.id_evento_judicial
             LEFT JOIN ( SELECT processo.id_processo,
                                lpad(cast(processo.num_processo as varchar), 20, '0') AS num_processo_20
                           FROM eproc2g.processo processo) p ON pe.id_processo = p.id_processo
                          WHERE ej.cod_mni_movimento IN (26, 36)),       
       designacao_orgao_passado 
        AS (
         SELECT x.id_processo,
                x.dth_distribuicao,
                x.id_orgao_colegiado
           FROM ( SELECT d_1.id_processo,
                         d_1.dth_distribuicao,
                         lp.dth_alteracao,
                         lp.id_orgao_colegiado,
                         row_number() OVER (PARTITION BY d_1.id_processo, d_1.dth_distribuicao ORDER BY lp.dth_alteracao) AS rn
                    FROM distribuicao d_1
                     JOIN eproc2g.log_processo lp on  lp.id_processo = d_1.id_processo 
                                                 AND lp.dth_alteracao > d_1.dth_distribuicao
                   WHERE lp.id_orgao_colegiado <> 3583) x
          WHERE x.rn = 1),                                                         
       designacao_orgao_atual 
        AS (
         SELECT x.id_processo,
                x.dth_distribuicao,
                x.id_orgao_colegiado
           FROM ( SELECT d_1.id_processo,
                         d_1.dth_distribuicao,
                         lp.dth_alteracao,
                         lp.id_orgao_colegiado,
                         row_number() OVER (PARTITION BY d_1.id_processo ORDER BY lp.dth_alteracao DESC) AS rn
                    FROM ( SELECT max(distribuicao.dth_distribuicao) AS dth_distribuicao,
                                  distribuicao.id_processo
                             FROM distribuicao
                           GROUP BY distribuicao.id_processo) d_1
                     JOIN ( SELECT log_processo.dth_alteracao,
                                   log_processo.id_processo,
                                   log_processo.id_orgao_colegiado 
                              FROM eproc2g.log_processo log_processo
                           UNION ALL
                            SELECT CURRENT_TIMESTAMP AS dth_alteracao,
                                   processo.id_processo,
                                   processo.id_orgao_colegiado
                              FROM eproc2g.processo processo) lp on  lp.id_processo = d_1.id_processo
                                                                  AND lp.dth_alteracao > d_1.dth_distribuicao
                    WHERE lp.id_orgao_colegiado <> 3583) x
          WHERE x.rn = 1)
  SELECT d.dth_distribuicao,
         EXTRACT(year FROM d.dth_distribuicao)  AS ano,
         EXTRACT(month FROM d.dth_distribuicao) AS mes,
         d.id_processo,
         d.num_processo,
         d.cod_magistrado,
         m.nome_magistrado,
         COALESCE(doa.id_orgao_colegiado, dop.id_orgao_colegiado) AS cod_orgao_julgador,
         o.des_orgao AS nm_orgao_colegiado,
         CAST('EPROC2G' AS varchar) AS ind_sistema
    FROM distribuicao d
     LEFT JOIN designacao_orgao_passado dop ON dop.id_processo = d.id_processo
                                            AND dop.dth_distribuicao = d.dth_distribuicao
     LEFT JOIN designacao_orgao_atual   doa ON doa.id_processo = d.id_processo 
                                            AND doa.dth_distribuicao = d.dth_distribuicao
     LEFT JOIN eproc2g.magistrado       m   ON m.cod_magistrado = d.cod_magistrado
     LEFT JOIN eproc2g.orgao            o   ON o.id_orgao = COALESCE(doa.id_orgao_colegiado, dop.id_orgao_colegiado)
union all 
select 
      md.dthrmov                     as dth_distribuicao,
      EXTRACT(year FROM md.dthrmov)  AS ano,
      EXTRACT(month FROM md.dthrmov) AS mes,
      p.coddoc                       as id_processo,
      concat(
        lpad(cast(p.numsequencial as varchar), 7, '0'), '-',
        lpad(cast(p.dv            as varchar), 2, '0'), '.',
        cast(p.numano     as varchar), '.',
        cast(p.codjustica as varchar), '.',
        lpad(cast(p.codregi    as varchar), 2, '0'), '.',
        lpad(cast(p.codlocregi as varchar), 4, '0')
            )                        as num_processo,
      mr.codmagrel                   as cod_magistrado,
      pm.nome                        as nome_magistrado,
      mr.codorgjulg                  as cod_orgao_julgador,
      po.nome                        as nm_orgao_colegiado,
      CAST('EJUD' AS varchar)        AS ind_sistema
from ejud.t_processo p
  join ejud.movdistribuicao  md  on p.coddoc      = md.coddoc
  left join ejud.fase        f   on md.codfase    = f.codfase
left join ejud.faseexterna   fe  on f.codfase     = fe.codfase
join ejud.movimentorelator   mr  on md.coddoc     = mr.coddoc and md.dthrmov = mr.dthrmov
left join ejud.magistrado    m   on m.codmag      = mr.codmagrel
join ejud.pessoa             pm  on m.codpess     = pm.codpess
left join ejud.orgaojulgador oj  on mr.codorgjulg = oj.codorgjulg
join ejud.pessoa             po  on oj.codpess    = po.codpess
where f.codfase in (26,36);