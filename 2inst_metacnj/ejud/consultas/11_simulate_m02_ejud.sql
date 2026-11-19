-- ============================================================
-- SIMULATION — ejud_metacnj_m02_emr.py in pure SQL
--
-- Reproduces every step the script executes:
--   1. eligible coddoc     (same as m01: conhecimento+criminal+outros+competencia+documento)
--   2. judmigracao exclusion (ejud.movimento.usuincl = 'JUDMIGRACAO' or LIKE 'USUARIO_%')
--   3. dism02              (first sit-88 BEFORE cutoff, not judged, not judmigracao)
--   4. dism_ant02          (same, bounded by 15-year cutoff; judgment anti-join to full cutoff)
--   5. julgadom02          (union prim_sent + baixa + mpu, first per dism02 pool, AFTER cutoff)
--   6. julgadom02_ant      (same, from dism_ant02 pool)
--   7. susm02              (last suspension ADD per dism02 pool, not judged)
--   8. susm02_ant          (same for dism_ant02 pool)
--   9. desm_ant02          (levantamento AFTER cutoff, dism_ant02 pool, not judged)
--
-- All codes from ejud/json/meta_config.json.
--
-- Parameters:
--   cutoff_date    = 2023-12-31   (meta_02 cutoff — year-3-12-31 by convention)
--   meta15_cutoff  = 2011-12-31   (cutoff - 12 years = year-15-12-31)
--   period_end     = 2026-12-31
--   dist_start     = 1980-01-01   (shared.dist_start)
--   meta_ano       = 2026
-- ============================================================


WITH

-- ── eligible coddoc: same as m01 (see 10_simulate_m01_ejud.sql for comments) ─
elig_conhecimento AS (
    SELECT DISTINCT codtipproc FROM ejud.tipoprocesso
    WHERE TRY_CAST(REGEXP_REPLACE(CAST(codextcnj AS VARCHAR), '\.0$', '') AS INTEGER) IN (
        7, 22, 26, 27, 28, 29, 30, 31, 32, 34, 35, 37, 38, 39, 40, 41, 44, 45, 47, 48,
        49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 69, 72,
        74, 76, 77, 79, 80, 81, 82, 83, 84, 85, 86, 87, 89, 90, 91, 92, 93, 94, 95, 96,
        97, 98, 99, 100, 107, 108, 110, 111, 112, 113, 114, 115, 118, 119, 120, 121, 122,
        123, 124, 127, 128, 129, 134, 135, 136, 137, 138, 140, 141, 142, 143, 144, 145,
        172, 175, 176, 177, 178, 179, 180, 181, 182, 183, 186, 188, 190, 192, 193, 194,
        195, 196, 198, 199, 202, 206, 208, 216, 221, 233, 244, 251, 278, 281, 282, 283,
        284, 285, 287, 288, 289, 290, 292, 293, 294, 295, 297, 299, 300, 302, 303, 304,
        305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 325, 327, 328, 329, 330,
        413, 416, 417, 418, 419, 420, 421, 422, 424, 425, 426, 427, 428, 432, 433, 436,
        457, 460, 976, 980, 985, 986, 987, 988, 1000, 1001, 1002, 1003, 1004, 1005, 1006,
        1007, 1008, 1009, 1015, 1016, 1025, 1026, 1027, 1029, 1030, 1031, 1032, 1033,
        1035, 1036, 1037, 1044, 1049, 1053, 1054, 1062, 1064, 1066, 1067, 1107, 1118,
        1122, 1124, 1125, 1126, 1137, 1145, 1178, 1202, 1208, 1233, 1267, 1268, 1269,
        1271, 1285, 1289, 1294, 1295, 1296, 1297, 1311, 1312, 1313, 1314, 1315, 1316,
        1317, 1318, 1319, 1320, 1321, 1322, 1326, 1327, 1328, 1329, 1330, 1331, 1332,
        1334, 1335, 1336, 1337, 1339, 1342, 1343, 1344, 1345, 1346, 1347, 1348, 1349,
        1350, 1351, 1352, 1353, 1354, 1386, 1389, 1390, 1391, 1392, 1396, 1399, 1401,
        1412, 1414, 1415, 1417, 1420, 1424, 1425, 1426, 1436, 1438, 1440, 1464, 1669,
        1670, 1674, 1675, 1678, 1679, 1682, 1683, 1685, 1689, 1690, 1691, 1703, 1704,
        1705, 1706, 1707, 1709, 1710, 1715, 1720, 1721, 1722, 1726, 1728, 1729, 10933,
        10942, 10943, 10944, 10967, 10970, 11027, 11029, 11031, 11034, 11035, 11036,
        11037, 11038, 11039, 11040, 11042, 11043, 11044, 11045, 11046, 11397, 11398,
        11526, 11527, 11528, 11529, 11531, 11532, 11533, 11535, 11536, 11537, 11538,
        11539, 11541, 11547, 11548, 11549, 11550, 11551, 11552, 11555, 11556, 11791,
        11793, 11798, 11880, 11881, 11882, 11886, 11955, 11956, 12057, 12058, 12059,
        12061, 12070, 12071, 12072, 12073, 12074, 12076, 12083, 12084, 12085, 12086,
        12087, 12117, 12118, 12122, 12133, 12134, 12135, 12138, 12190, 12191, 12193,
        12230, 12232, 12233, 12234, 12249, 12250, 12369, 12370, 12371, 12372, 12373,
        12374, 12375, 12376, 12377, 12389, 12390, 12391, 12394, 12423, 12424, 12541,
        12558, 12625, 12626, 12627, 12628, 12630, 12631, 12633, 12760, 12761, 12762,
        12763, 12764, 12775, 12791, 14208, 14209, 14671, 14677, 14695, 14701, 14734,
        14774, 14775, 14991, 15011, 15090, 15140, 15159, 15170, 15171, 15172, 15190,
        15191, 15192, 15193, 15194, 15217, 15224, 15303, 15309, 15323, 15532, 15533,
        15534, 15535, 15543, 15620
    )
    AND (TRY_CAST(REGEXP_REPLACE(CAST(codnat AS VARCHAR), '\.0$', '') AS INTEGER) = 1
         OR codnat IS NULL)
),
elig_criminal AS (
    SELECT DISTINCT codtipproc FROM ejud.tipoprocesso
    WHERE TRY_CAST(REGEXP_REPLACE(CAST(codextcnj AS VARCHAR), '\.0$', '') AS INTEGER) IN (
        278, 281, 282, 283, 284, 285, 287, 288, 289, 290, 292, 293, 294, 295, 297, 299,
        300, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 325,
        327, 328, 329, 330, 413, 416, 417, 418, 419, 420, 421, 422, 424, 425, 426, 427,
        428, 432, 433, 1033, 1178, 1268, 1317, 1330, 1337, 1339, 1343, 1349, 1678, 1710,
        1715, 1729, 10943, 10944, 10967, 11031, 11037, 11038, 11039, 11040, 11042, 11043,
        11044, 11045, 11046, 11398, 11528, 11791, 11793, 11798, 11955, 12122, 12394,
        14209, 14701, 14734, 15170, 15224
    )
),
elig_outros AS (
    SELECT DISTINCT codtipproc FROM ejud.tipoprocesso
    WHERE TRY_CAST(REGEXP_REPLACE(CAST(codextcnj AS VARCHAR), '\.0$', '') AS INTEGER) IN (
        2, 5, 46, 88, 158, 169, 170, 171, 173, 191, 197, 200, 203, 204, 207, 211,
        212, 213, 214, 215, 218, 224, 228, 229, 230, 231, 232, 234, 236, 237, 238,
        239, 240, 241, 242, 256, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277,
        279, 280, 291, 316, 317, 318, 319, 320, 321, 322, 323, 324, 326, 331, 332,
        333, 385, 406, 407, 408, 409, 410, 411, 412, 547, 1013, 1017, 1018, 1019,
        1028, 1034, 1038, 1040, 1042, 1045, 1046, 1057, 1063, 1068, 1070, 1071,
        1072, 1106, 1198, 1199, 1209, 1230, 1231, 1232, 1262, 1264, 1265, 1266,
        1283, 1284, 1288, 1291, 1298, 1299, 1301, 1303, 1304, 1306, 1307, 1308,
        1310, 1323, 1325, 1333, 1338, 1340, 1341, 1355, 1356, 1357, 1358, 1359,
        1360, 1361, 1362, 1363, 1364, 1365, 1366, 1367, 1368, 1369, 1370, 1371,
        1372, 1373, 1374, 1375, 1376, 1377, 1378, 1379, 1380, 1381, 1382, 1383,
        1384, 1385, 1459, 1460, 1461, 1462, 1463, 1671, 1672, 1673, 1677, 1680,
        1701, 1702, 1711, 1712, 1717, 1719, 1723, 1725, 1727, 1730, 1731, 1733,
        10969, 10971, 10972, 10973, 10974, 10975, 10976, 10977, 10979, 10981,
        11026, 11028, 11030, 11032, 11033, 11041, 11400, 11427, 11525, 11534,
        11540, 11544, 11545, 11553, 11787, 11788, 11789, 11790, 11794, 11799,
        11800, 11887, 11888, 11889, 11890, 11891, 11892, 11893, 11894, 11953,
        11957, 11976, 12060, 12075, 12077, 12080, 12081, 12089, 12119, 12121,
        12123, 12124, 12125, 12126, 12127, 12128, 12129, 12132, 12137, 12139,
        12153, 12192, 12226, 12227, 12228, 12229, 12248, 12357, 12386, 12388,
        12464, 12465, 12466, 12551, 12562, 12632, 12776, 12777, 12778, 12779,
        12780, 12781, 12782, 12783, 12784, 12785, 12790, 14123, 14676, 14678,
        15167, 15195, 15196, 15228, 15239, 15240, 15241, 15242, 15243
    )
),
eligible_codcompt AS (
    SELECT DISTINCT TRY_CAST(REGEXP_REPLACE(CAST(codcompt AS VARCHAR), '\.0$', '') AS INTEGER) AS codcompt_int
    FROM ejud.competencia
    WHERE TRY_CAST(REGEXP_REPLACE(CAST(codcompt AS VARCHAR), '\.0$', '') AS INTEGER)
          NOT IN (1, 9, 12, 21)
),
active_docs AS (
    SELECT DISTINCT coddoc FROM ejud.documento WHERE indativo = 'S'
),
eligible AS (
    SELECT DISTINCT p.coddoc
    FROM ejud.t_processo p
    JOIN (
        SELECT codtipproc FROM elig_conhecimento
        UNION SELECT codtipproc FROM elig_criminal
        UNION SELECT codtipproc FROM elig_outros
    ) et ON p.codtipproc = et.codtipproc
    JOIN eligible_codcompt ec
        ON TRY_CAST(REGEXP_REPLACE(CAST(p.codcompt AS VARCHAR), '\.0$', '') AS INTEGER) = ec.codcompt_int
    JOIN active_docs d ON p.coddoc = d.coddoc
),

-- ── judmigracao exclusion ───────────────────────────────────────────────────
-- Any coddoc with ANY movement from JUDMIGRACAO or USUARIO_* is excluded from stock
-- meta_config.json: no config key — hardcoded in script build_dism02() step 6
judmig_coddoc AS (
    SELECT DISTINCT coddoc
    FROM ejud.movimento
    WHERE usuincl = 'JUDMIGRACAO'
       OR usuincl LIKE 'USUARIO_%'
),

-- ── judgment events for anti-join (dism02) ─────────────────────────────────
-- JUDGMENT_CODES from meta_config.json → shared.situacao_codes.judgment
-- sit-17 guard: only valid MPU cod_tip_mov counts as judgment
-- Date window for dism02: 1980-01-01 → cutoff_date (2023-12-31)
judged_dism02 AS (
    SELECT DISTINCT coddoc
    FROM dw_2inst_ouro.tb_tf_situacao_ejud
    WHERE situacao IN (62, 27, 28, 2, 10, 23, 41, 18, 29, 90, 129, 26, 65, 91, 134, 154, 132, 17, 34)
      AND dthrmov >= TIMESTAMP '1980-01-01 00:00:00'
      AND dthrmov <= TIMESTAMP '2023-12-31 23:59:59'   -- cutoff_date
      AND (
          situacao != 17
          OR TRY_CAST(cod_tip_mov AS INTEGER)
             IN (11423, 11424, 11425, 12476, 12479, 15486, 15487, 15488)
      )
),

-- ── sit-88 + trigger scan up to cutoff (shared by dism02 and dism_ant02) ──
-- trigger codes = distribution_codes_stock: [9, 65, 81, 61, 24]
-- meta_config.json → shared.situacao_codes.distribution_codes_stock
sit88_cutoff AS (
    SELECT coddoc, situacao, dthrmov, cod_classe, nivel_sigilo
    FROM dw_2inst_ouro.tb_tf_situacao_ejud
    WHERE situacao IN (88, 9, 65, 81, 61, 24)
      AND dthrmov >= TIMESTAMP '1980-01-01 00:00:00'
      AND dthrmov <= TIMESTAMP '2023-12-31 23:59:59'   -- cutoff_date
),

-- ── dism02: first sit-88 per coddoc, trigger validated ─────────────────────
first_88_dism02_ranked AS (
    SELECT coddoc, dthrmov AS first_88_dt, cod_classe, nivel_sigilo,
           ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov ASC) AS rn
    FROM sit88_cutoff WHERE situacao = 88
),
first_88_dism02 AS (
    SELECT coddoc, first_88_dt, cod_classe, nivel_sigilo
    FROM first_88_dism02_ranked WHERE rn = 1
),
triggers_dism02 AS (
    SELECT coddoc, dthrmov AS trigger_dt
    FROM sit88_cutoff WHERE situacao IN (9, 65, 81, 61, 24)
),
valid_88_dism02 AS (
    SELECT DISTINCT f.coddoc, f.first_88_dt, f.cod_classe, f.nivel_sigilo
    FROM first_88_dism02 f
    JOIN triggers_dism02 t ON f.coddoc = t.coddoc AND t.trigger_dt <= f.first_88_dt
),
dism02 AS (
    SELECT v.coddoc,
           v.first_88_dt  AS dth_distribuicao,
           v.cod_classe,
           v.nivel_sigilo AS cod_sigilo,
           6              AS cod_indicador,
           'dism02'       AS variavel
    FROM valid_88_dism02 v
    JOIN eligible e ON v.coddoc = e.coddoc
    LEFT JOIN judged_dism02 j ON v.coddoc = j.coddoc
    LEFT JOIN judmig_coddoc m ON v.coddoc = m.coddoc
    WHERE j.coddoc IS NULL AND m.coddoc IS NULL
),

-- ── dism_ant02: same but sit-88 bounded by meta15_cutoff (2011-12-31) ──────
-- Judgment anti-join still extends to cutoff_date (2023-12-31) — not meta15_cutoff
sit88_ant AS (
    SELECT coddoc, situacao, dthrmov, cod_classe, nivel_sigilo
    FROM dw_2inst_ouro.tb_tf_situacao_ejud
    WHERE situacao IN (88, 9, 65, 81, 61, 24)
      AND dthrmov >= TIMESTAMP '1980-01-01 00:00:00'
      AND dthrmov <= TIMESTAMP '2011-12-31 23:59:59'   -- meta15_cutoff (year-15-12-31)
),
first_88_ant_ranked AS (
    SELECT coddoc, dthrmov AS first_88_dt, cod_classe, nivel_sigilo,
           ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov ASC) AS rn
    FROM sit88_ant WHERE situacao = 88
),
first_88_ant AS (
    SELECT coddoc, first_88_dt, cod_classe, nivel_sigilo
    FROM first_88_ant_ranked WHERE rn = 1
),
triggers_ant AS (
    SELECT coddoc, dthrmov AS trigger_dt
    FROM sit88_ant WHERE situacao IN (9, 65, 81, 61, 24)
),
valid_88_ant AS (
    SELECT DISTINCT f.coddoc, f.first_88_dt, f.cod_classe, f.nivel_sigilo
    FROM first_88_ant f
    JOIN triggers_ant t ON f.coddoc = t.coddoc AND t.trigger_dt <= f.first_88_dt
),
dism_ant02 AS (
    SELECT v.coddoc,
           v.first_88_dt  AS dth_distribuicao,
           v.cod_classe,
           v.nivel_sigilo AS cod_sigilo,
           11             AS cod_indicador,
           'dism_ant02'   AS variavel
    FROM valid_88_ant v
    JOIN eligible e ON v.coddoc = e.coddoc
    LEFT JOIN judged_dism02 j ON v.coddoc = j.coddoc     -- anti-join to cutoff_date
    LEFT JOIN judmig_coddoc m ON v.coddoc = m.coddoc
    WHERE j.coddoc IS NULL AND m.coddoc IS NULL
),

-- ── Flow variables AFTER cutoff (2024-01-01 → 2026-12-31) ─────────────────

-- primeirasentm02: first sentence per dism02 pool AFTER cutoff
prim_sent02_ranked AS (
    SELECT s.coddoc, s.dthrmov, s.situacao, s.cod_classe,
           ROW_NUMBER() OVER (PARTITION BY s.coddoc ORDER BY s.dthrmov ASC) AS rn
    FROM dw_2inst_ouro.tb_tf_situacao_ejud s
    JOIN dism02 d ON s.coddoc = d.coddoc
    WHERE s.situacao IN (27, 28, 62, 18, 29, 90, 129)
      AND s.dthrmov > TIMESTAMP '2023-12-31 23:59:59'    -- AFTER cutoff
      AND s.dthrmov <= TIMESTAMP '2026-12-31 23:59:59'
),
primeirasentm02 AS (SELECT coddoc, dthrmov, cod_classe FROM prim_sent02_ranked WHERE rn = 1),

-- baixm02: first baixa per dism02 pool AFTER cutoff
baixm02_ranked AS (
    SELECT s.coddoc, s.dthrmov, s.situacao, s.cod_classe,
           ROW_NUMBER() OVER (PARTITION BY s.coddoc ORDER BY s.dthrmov ASC) AS rn
    FROM dw_2inst_ouro.tb_tf_situacao_ejud s
    JOIN dism02 d ON s.coddoc = d.coddoc
    WHERE s.situacao IN (2, 10, 23, 41, 134, 154, 132)
      AND s.dthrmov > TIMESTAMP '2023-12-31 23:59:59'
      AND s.dthrmov <= TIMESTAMP '2026-12-31 23:59:59'
),
baixm02 AS (SELECT coddoc, dthrmov, cod_classe FROM baixm02_ranked WHERE rn = 1),

-- mpum02: first MPU per dism02 pool AFTER cutoff
mpum02_ranked AS (
    SELECT s.coddoc, s.dthrmov, s.situacao, s.cod_classe,
           ROW_NUMBER() OVER (PARTITION BY s.coddoc ORDER BY s.dthrmov ASC) AS rn
    FROM dw_2inst_ouro.tb_tf_situacao_ejud s
    JOIN dism02 d ON s.coddoc = d.coddoc
    WHERE s.situacao IN (17, 34)
      AND TRY_CAST(s.cod_tip_mov AS INTEGER)
          IN (11423, 11424, 11425, 12476, 12479, 15486, 15487, 15488)
      AND s.dthrmov > TIMESTAMP '2023-12-31 23:59:59'
      AND s.dthrmov <= TIMESTAMP '2026-12-31 23:59:59'
),
mpum02 AS (SELECT coddoc, dthrmov, cod_classe FROM mpum02_ranked WHERE rn = 1),

-- julgadom02: union prim + baixa + mpu, first per coddoc
julg02_union AS (
    SELECT coddoc, dthrmov, cod_classe FROM primeirasentm02
    UNION ALL SELECT coddoc, dthrmov, cod_classe FROM baixm02
    UNION ALL SELECT coddoc, dthrmov, cod_classe FROM mpum02
),
julg02_ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov ASC) AS rn
    FROM julg02_union
),
julgadom02 AS (
    SELECT coddoc, dthrmov, cod_classe,
           7 AS cod_indicador, 'julgadom02' AS variavel
    FROM julg02_ranked WHERE rn = 1
),

-- julgadom02_ant: same but from dism_ant02 pool
julg02_ant_union AS (
    SELECT s.coddoc, s.dthrmov, s.cod_classe
    FROM dw_2inst_ouro.tb_tf_situacao_ejud s
    JOIN dism_ant02 d ON s.coddoc = d.coddoc
    WHERE s.situacao IN (27, 28, 62, 18, 29, 90, 129,  -- prim_sent
                         2, 10, 23, 41, 134, 154, 132)  -- baixa
      AND s.dthrmov > TIMESTAMP '2023-12-31 23:59:59'
      AND s.dthrmov <= TIMESTAMP '2026-12-31 23:59:59'
),
julg02_ant_ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY coddoc ORDER BY dthrmov ASC) AS rn
    FROM julg02_ant_union
),
julgadom02_ant AS (
    SELECT coddoc, dthrmov, cod_classe,
           10 AS cod_indicador, 'julgadom02_ant' AS variavel
    FROM julg02_ant_ranked WHERE rn = 1
),

-- susm02: last suspension ADD per dism02 pool, not judged, <= period_end
susm02_all AS (
    SELECT s.coddoc, s.dthrmov, s.situacao, s.cod_classe,
           ROW_NUMBER() OVER (PARTITION BY s.coddoc ORDER BY s.dthrmov DESC) AS rn
    FROM dw_2inst_ouro.tb_tf_situacao_ejud s
    JOIN dism02 d ON s.coddoc = d.coddoc
    LEFT JOIN julgadom02 j ON s.coddoc = j.coddoc
    WHERE s.situacao IN (
        135, 92, 93, 46, 45, 94, 95, 47, 144, 96, 48, 49,   -- suspension_add
        138, 97, 98, 20, 106, 107, 99, 100, 105, 101, 103, 102, 104  -- suspension_remove
    )
    AND s.dthrmov > TIMESTAMP '2023-12-31 23:59:59'
    AND s.dthrmov <= TIMESTAMP '2026-12-31 23:59:59'
    AND j.coddoc IS NULL
),
susm02 AS (
    SELECT coddoc, dthrmov, cod_classe,
           8 AS cod_indicador, 'susm02' AS variavel
    FROM susm02_all
    WHERE rn = 1
      AND situacao IN (135, 92, 93, 46, 45, 94, 95, 47, 144, 96, 48, 49)
),

-- susm02_ant: same for dism_ant02 pool
susm02_ant_all AS (
    SELECT s.coddoc, s.dthrmov, s.situacao, s.cod_classe,
           ROW_NUMBER() OVER (PARTITION BY s.coddoc ORDER BY s.dthrmov DESC) AS rn
    FROM dw_2inst_ouro.tb_tf_situacao_ejud s
    JOIN dism_ant02 d ON s.coddoc = d.coddoc
    LEFT JOIN julgadom02_ant j ON s.coddoc = j.coddoc
    WHERE s.situacao IN (
        135, 92, 93, 46, 45, 94, 95, 47, 144, 96, 48, 49,
        138, 97, 98, 20, 106, 107, 99, 100, 105, 101, 103, 102, 104
    )
    AND s.dthrmov > TIMESTAMP '2023-12-31 23:59:59'
    AND s.dthrmov <= TIMESTAMP '2026-12-31 23:59:59'
    AND j.coddoc IS NULL
),
susm02_ant AS (
    SELECT coddoc, dthrmov, cod_classe,
           12 AS cod_indicador, 'susm02_ant' AS variavel
    FROM susm02_ant_all
    WHERE rn = 1
      AND situacao IN (135, 92, 93, 46, 45, 94, 95, 47, 144, 96, 48, 49)
),

-- desm_ant02: levantamento AFTER cutoff, dism_ant02 pool, not judged
desm_ant02_ranked AS (
    SELECT s.coddoc, s.dthrmov, s.cod_classe,
           ROW_NUMBER() OVER (PARTITION BY s.coddoc ORDER BY s.dthrmov ASC) AS rn
    FROM dw_2inst_ouro.tb_tf_situacao_ejud s
    JOIN dism_ant02 d ON s.coddoc = d.coddoc
    LEFT JOIN julgadom02_ant j ON s.coddoc = j.coddoc
    WHERE s.situacao IN (138, 97, 98, 20, 106, 107, 99, 100, 105, 101, 103, 102, 104)
      AND s.dthrmov > TIMESTAMP '2023-12-31 23:59:59'
      AND s.dthrmov <= TIMESTAMP '2026-12-31 23:59:59'
      AND j.coddoc IS NULL
),
desm_ant02 AS (
    SELECT coddoc, dthrmov, cod_classe,
           9 AS cod_indicador, 'desm_ant02' AS variavel
    FROM desm_ant02_ranked WHERE rn = 1
)

-- ── Final: union all variables ──────────────────────────────────────────────
SELECT
    CAST(coddoc AS VARCHAR) AS id_processo,
    variavel, cod_indicador,
    dth_distribuicao AS dth_evento_indicador,
    TRY_CAST(cod_classe AS INT) AS cod_classe_cnj,
    1 AS cod_sistema, '02' AS cd_meta, 2026 AS meta_ano
FROM dism02

UNION ALL SELECT CAST(coddoc AS VARCHAR), variavel, cod_indicador,
    dth_distribuicao, TRY_CAST(cod_classe AS INT), 1, '02', 2026 FROM dism_ant02

UNION ALL SELECT CAST(coddoc AS VARCHAR), variavel, cod_indicador,
    dthrmov, TRY_CAST(cod_classe AS INT), 1, '02', 2026 FROM julgadom02

UNION ALL SELECT CAST(coddoc AS VARCHAR), variavel, cod_indicador,
    dthrmov, TRY_CAST(cod_classe AS INT), 1, '02', 2026 FROM julgadom02_ant

UNION ALL SELECT CAST(coddoc AS VARCHAR), variavel, cod_indicador,
    dthrmov, TRY_CAST(cod_classe AS INT), 1, '02', 2026 FROM susm02

UNION ALL SELECT CAST(coddoc AS VARCHAR), variavel, cod_indicador,
    dthrmov, TRY_CAST(cod_classe AS INT), 1, '02', 2026 FROM susm02_ant

UNION ALL SELECT CAST(coddoc AS VARCHAR), variavel, cod_indicador,
    dthrmov, TRY_CAST(cod_classe AS INT), 1, '02', 2026 FROM desm_ant02

ORDER BY cod_indicador, dth_evento_indicador
LIMIT 200;


-- ── Summary count per variable (run separately) ────────────────────────────
/*
SELECT variavel, cod_indicador, count(*) AS n
FROM (
    SELECT 'dism02' AS variavel, 6 AS cod_indicador, coddoc FROM dism02
    UNION ALL SELECT 'dism_ant02',   11, coddoc FROM dism_ant02
    UNION ALL SELECT 'julgadom02',    7, coddoc FROM julgadom02
    UNION ALL SELECT 'julgadom02_ant',10, coddoc FROM julgadom02_ant
    UNION ALL SELECT 'susm02',        8, coddoc FROM susm02
    UNION ALL SELECT 'susm02_ant',   12, coddoc FROM susm02_ant
    UNION ALL SELECT 'desm_ant02',    9, coddoc FROM desm_ant02
)
GROUP BY variavel, cod_indicador
ORDER BY cod_indicador;
*/
