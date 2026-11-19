select codtrib as cod_tribunal,
       nome as desc_tribunal,
       indreper,
       indreperorig,
       'EJUD' as sistema
  from ejud_tribunal t2 
  join ejud_pessoa p on t2.codpess =p.codpess 
