select codcargo as cod_cargo,
       descr as desc_cargo,
       indativo,
       'EJUD' as sistema
  from glue_catalog.ejud.cargo;
