select codtrib as cod_tribunal,
       nome as desc_tribunal,
       indreper,
       indreperorig,
       'EJUD' as sistema
  from glue_catalog.ejud.tribunal t2 
  join glue_catalog.ejud.pessoa p on t2.codpess =p.codpess     
