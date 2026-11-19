  select codvara as cod_vara,
         nome as desc_vara,
         coduf,
         codcid,
         codjustorig,
         'EJUD' as sistema
    from glue_catalog.ejud.vara v
    join glue_catalog.ejud.pessoa p on v.codpess = p.codpess 
