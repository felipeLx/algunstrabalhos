select codtipevntmag as cod_tipo_evento_mag,
       descr as desc_tipo_evento_mag,
       'EJUD'        as sistema
from glue_catalog.ejud.tipoeventomagistrado t
