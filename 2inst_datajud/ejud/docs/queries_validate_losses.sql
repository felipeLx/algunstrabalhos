-- ============================================================
-- VALIDATION QUERIES: Top unresolved codfase combinations
-- Run on Oracle TJ01 (EJUD)
-- Purpose: determine if losses are real gaps or void/old data
-- ============================================================

-- ============================================================
-- LOSS #1: codfase=31, faseext=60007 (1,651 combos)
-- RESULT: "Baixa provisória" - void movement, old data
-- STATUS: VALIDATED - can ignore
-- ============================================================
SELECT
    m.coddoc, m.dthrmov,
    f.codfase, fe.codfaseext, f.descr AS fase,
    c.codcompl, c.descr AS compl1
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
JOIN ejud.complemento c ON m.codcompl1 = c.codcompl
JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
WHERE f.codfase = 31 AND fe.codfaseext = 60007
ORDER BY m.dthrmov DESC;


-- ============================================================
-- LOSS #2: codfase=10, faseext=8 (799 combos) - IN integracao
-- Issue: complement mismatch (codcompl1 not in integracaofasecompl)
-- Question: which codcompl1 values exist that integracao doesn't cover?
-- ============================================================
SELECT
    f.codfase, fe.codfaseext, f.descr AS fase,
    c.codcompl, c.descr AS compl1,
    COUNT(*) AS total_movimentos,
    MIN(m.dthrmov) AS primeiro_mov,
    MAX(m.dthrmov) AS ultimo_mov
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
JOIN ejud.complemento c ON m.codcompl1 = c.codcompl
JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
LEFT JOIN ejud.integracaofasecompl_ejud_cnj i
    ON fe.codfaseext = i.codfasenivel2
   AND i.indativo = 'S'
   AND (i.ultimonivelcnj < 3 OR m.codcompl1 = i.codcompl1nivel3)
WHERE f.codfase = 10
  AND i.seq IS NULL
GROUP BY f.codfase, fe.codfaseext, f.descr, c.codcompl, c.descr
ORDER BY total_movimentos DESC;


-- ============================================================
-- LOSS #3: codfase=982, faseext=982 (339 combos)
-- NOT in integracao. 982 = Remessa (Distribuidor)
-- Question: old data? still happening?
-- ============================================================
SELECT
    m.coddoc, m.dthrmov,
    f.codfase, fe.codfaseext, f.descr AS fase,
    c.codcompl, c.descr AS compl1
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
JOIN ejud.complemento c ON m.codcompl1 = c.codcompl
JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
WHERE f.codfase = 982
ORDER BY m.dthrmov DESC
FETCH FIRST 50 ROWS ONLY;

-- Count + date range
SELECT
    f.descr AS fase,
    COUNT(*) AS total,
    MIN(m.dthrmov) AS primeiro,
    MAX(m.dthrmov) AS ultimo
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
WHERE f.codfase = 982
GROUP BY f.descr;


-- ============================================================
-- LOSS #4: codfase=50010, faseext=50010 (137 combos)
-- NOT in integracao. 50010 = EJUD custom code
-- Question: what is this movement? still in use?
-- ============================================================
SELECT
    m.coddoc, m.dthrmov,
    f.codfase, fe.codfaseext, f.descr AS fase,
    c.codcompl, c.descr AS compl1
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
JOIN ejud.complemento c ON m.codcompl1 = c.codcompl
JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
WHERE f.codfase = 50010
ORDER BY m.dthrmov DESC
FETCH FIRST 50 ROWS ONLY;

-- Count + date range
    SELECT
        f.descr AS fase,
        COUNT(*) AS total,
        MIN(m.dthrmov) AS primeiro,
        MAX(m.dthrmov) AS ultimo
    FROM ejud.movimento m
    JOIN ejud.fase f ON m.codfase = f.codfase
    WHERE f.codfase = 50010
    GROUP BY f.descr;


-- ============================================================
-- LOSS #5: codfase=29, faseext=60006 (64 combos)
-- NOT in integracao.
-- ============================================================
SELECT
    f.descr AS fase,
    c.codcompl, c.descr AS compl1,
    COUNT(*) AS total,
    MIN(m.dthrmov) AS primeiro,
    MAX(m.dthrmov) AS ultimo
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
JOIN ejud.complemento c ON m.codcompl1 = c.codcompl
JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
WHERE f.codfase = 29 AND fe.codfaseext = 60006
GROUP BY f.descr, c.codcompl, c.descr
ORDER BY total DESC;


-- ============================================================
-- LOSS #6: codfase=30, faseext=60005 (56 combos)
-- NOT in integracao.
-- ============================================================
SELECT
    f.descr AS fase,
    c.codcompl, c.descr AS compl1,
    COUNT(*) AS total,
    MIN(m.dthrmov) AS primeiro,
    MAX(m.dthrmov) AS ultimo
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
JOIN ejud.complemento c ON m.codcompl1 = c.codcompl
JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
WHERE f.codfase = 30 AND fe.codfaseext = 60005
GROUP BY f.descr, c.codcompl, c.descr
ORDER BY total DESC;


-- ============================================================
-- LOSS #7: codfase=3, faseext=3 (50 combos) - IN integracao
-- Issue: complement mismatch
-- Question: which codcompl1 values don't match integracao?
-- ============================================================
SELECT
    f.descr AS fase,
    c.codcompl, c.descr AS compl1,
    COUNT(*) AS total,
    MIN(m.dthrmov) AS primeiro,
    MAX(m.dthrmov) AS ultimo
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
JOIN ejud.complemento c ON m.codcompl1 = c.codcompl
JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
LEFT JOIN ejud.integracaofasecompl_ejud_cnj i
    ON fe.codfaseext = i.codfasenivel2
   AND i.indativo = 'S'
   AND (i.ultimonivelcnj < 3 OR m.codcompl1 = i.codcompl1nivel3)
WHERE f.codfase = 3
  AND i.seq IS NULL
GROUP BY f.descr, c.codcompl, c.descr
ORDER BY total DESC;


-- ============================================================
-- LOSS #8: codfase=24, faseext=50193 (43 combos) - IN integracao
-- Issue: complement mismatch
-- ============================================================
SELECT
    f.descr AS fase,
    c.codcompl, c.descr AS compl1,
    COUNT(*) AS total,
    MIN(m.dthrmov) AS primeiro,
    MAX(m.dthrmov) AS ultimo
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
JOIN ejud.complemento c ON m.codcompl1 = c.codcompl
JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
LEFT JOIN ejud.integracaofasecompl_ejud_cnj i
    ON fe.codfaseext = i.codfasenivel2
   AND i.indativo = 'S'
   AND (i.ultimonivelcnj < 3 OR m.codcompl1 = i.codcompl1nivel3)
WHERE f.codfase = 24
  AND i.seq IS NULL
GROUP BY f.descr, c.codcompl, c.descr
ORDER BY total DESC;


-- ============================================================
-- LOSS #9: codfase=11009, faseext=11009 (21 combos) - IN integracao
-- 11009 = Despacho. Complement mismatch.
-- ============================================================
SELECT
    f.descr AS fase,
    c.codcompl, c.descr AS compl1,
    COUNT(*) AS total,
    MIN(m.dthrmov) AS primeiro,
    MAX(m.dthrmov) AS ultimo
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
JOIN ejud.complemento c ON m.codcompl1 = c.codcompl
JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
LEFT JOIN ejud.integracaofasecompl_ejud_cnj i
    ON fe.codfaseext = i.codfasenivel2
   AND i.indativo = 'S'
   AND (i.ultimonivelcnj < 3 OR m.codcompl1 = i.codcompl1nivel3)
WHERE f.codfase = 11009
  AND i.seq IS NULL
GROUP BY f.descr, c.codcompl, c.descr
ORDER BY total DESC;


-- ============================================================
-- LOSS #10: codfase=50510, faseext=60011 (14 combos)
-- NOT in integracao. EJUD custom code.
-- ============================================================
SELECT
    f.descr AS fase,
    c.codcompl, c.descr AS compl1,
    COUNT(*) AS total,
    MIN(m.dthrmov) AS primeiro,
    MAX(m.dthrmov) AS ultimo
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
JOIN ejud.complemento c ON m.codcompl1 = c.codcompl
JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
WHERE f.codfase = 50510
GROUP BY f.descr, c.codcompl, c.descr
ORDER BY total DESC;


-- ============================================================
-- BONUS: Summary of ALL unresolved movements (volume by year)
-- Shows if losses are concentrated in old data or still happening
-- ============================================================
SELECT
    EXTRACT(YEAR FROM m.dthrmov) AS ano,
    f.codfase,
    f.descr AS fase,
    COUNT(*) AS total
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
LEFT JOIN ejud.integracaofasecompl_ejud_cnj i
    ON fe.codfaseext = i.codfasenivel2
   AND i.indativo = 'S'
   AND (i.ultimonivelcnj < 3 OR m.codcompl1 = i.codcompl1nivel3)
   AND (i.ultimonivelcnj < 4 OR m.codcompl2 = i.codcompl2nivel4)
   AND (i.ultimonivelcnj < 5 OR m.codcompl3 = i.codcompl3nivel5)
WHERE i.seq IS NULL
  AND m.dthrmov >= DATE '2020-01-01'
GROUP BY EXTRACT(YEAR FROM m.dthrmov), f.codfase, f.descr
ORDER BY ano DESC, total DESC;


-- ============================================================
-- BONUS: 79 codfases WITHOUT faseexterna at all
-- These are EJUD codes that never got external mapping
-- Most have 0-1 combos (very rare), but worth checking
-- ============================================================
SELECT
    f.codfase, f.descr AS fase,
    COUNT(*) AS total,
    MIN(m.dthrmov) AS primeiro,
    MAX(m.dthrmov) AS ultimo
FROM ejud.movimento m
JOIN ejud.fase f ON m.codfase = f.codfase
LEFT JOIN ejud.faseexterna fe ON f.codfase = fe.codfase
WHERE fe.codfase IS NULL
GROUP BY f.codfase, f.descr
ORDER BY total DESC;
