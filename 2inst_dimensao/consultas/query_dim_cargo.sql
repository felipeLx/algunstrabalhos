SELECT codcargo as cod_cargo, descr as cargo
FROM ejud.cargo

UNION ALL

SELECT 0 as cod_cargo, 'Não encontrado' as cargo
UNION ALL
SELECT 9 as cod_cargo, 'JDS. Desembargador' as cargo
UNION ALL
SELECT 10 as cod_cargo, 'Presidente de Câmara' as cargo;