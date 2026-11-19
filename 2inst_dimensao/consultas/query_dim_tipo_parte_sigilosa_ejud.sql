select codtippartesigil as cod_tipo_parte_sigilosa,
       descr as desc_tipo_parte_sigilosa,
       indativo,
       'EJUD' as sistema
from glue_catalog.ejud.tipopartesigilosa t ;  
