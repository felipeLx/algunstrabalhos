select  codext as codclasse,
        descr as classe,
        codextcnj as codclassecnj,
        codnat,
        indativo,
        inddistr,
        'EJUD' as sistema
  from  glue_catalog.ejud.tipoprocesso;
