select codcompt as cod_competencia,
       descr as desc_competencia,
       indativo,
       'EJUD' as sistema
  from glue_catalog.ejud.competencia;
