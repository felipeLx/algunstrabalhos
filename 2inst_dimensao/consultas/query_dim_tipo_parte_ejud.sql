select codtipparte as cod_tipo_parte,
       codtippolo as cod_tipo_polo,
       descr as desc_tipo_parte,
       descrmnem,
       indativo,
       'EJUD' as sistema
from glue_catalog.ejud.tipoparte; 
