select codtipcarac as cod_tipo_caracteristica,
       descr as desc_tipo_caracteristica,
       indativo,
       'EJUD' as sistema 
from glue_catalog.ejud.tipocaracteristica t; 
