  select codvara as cod_vara,
         nome as desc_vara,
         coduf,
         codcid,
         codjustorig,
         'EJUD' as sistema
    from ejud_vara v
    join ejud_pessoa p on v.codpess = p.codpess 

