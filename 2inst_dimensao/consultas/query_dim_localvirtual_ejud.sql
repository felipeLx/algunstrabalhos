   SELECT DISTINCT
        codlocalvirt AS cod_localvirtual,
        descr AS localvirtual,
        indativo,
        'EJUD' AS sistema
    from glue_catalog.ejud.localvirtual
