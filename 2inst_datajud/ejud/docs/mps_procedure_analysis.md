# MPS Procedure Analysis: METAS_CNJ_DATAJUD_2024_GERA

## Overview

Oracle procedure run by MPS team to calculate Meta 2 and Meta 10 indicators for DataJud.
Delegates Meta 1/4/8/11 to older `METAS_CNJ_DATAJUD_2023_GERA`.

Source: `TAB_XML_ANDAMENTO` (pre-computed table with already-translated CNJ codes, NOT raw `ejud.movimento`).

## Date Logic

- `v_DTHRFIM_Meta_2 = year - 24 months` (cases must be older than 2 years to enter Meta 2 acervo)
- `v_DTHRFIM_Meta_2_Ant = year - 156 months` (13 years, for "antigos" bucket)
- `v_DTHRFIM_Meta_10 = year` (current year start)
- Excluded competencies: `codcompt NOT IN (9,10,11,14,15,19,20,22)`
- Excluded classes: `Cod_Classe NOT IN (39,49,108)`

## Indicators Extracted

### Meta 2 — NOVO (Caso Novo / Distribution)

CNJ codes: **26, 36, 391, 393, 849, 981**
Plus: 10966 (Evolução de Classe) with specific classes
Plus: 85 (Petição) with complement `19:tipo_de_peticao:52`

Logic: First occurrence only (`NOT EXISTS` earlier same type).

### Meta 2 — SENT (Julgamento/Sentença)

**240 distinct CNJ movement codes** — the full julgamento tree.

Key groups:
- 193 (Julgamento base)
- 196-244 range (various sentence types)
- 385, 442-473 (decisões)
- 900-901, 972-973 (acórdãos)
- 1042-1050 (decisões específicas)
- 10964-10965 (homologações)
- 11373-11411 (decisões monocráticas extended)
- 11795-11801, 11876-11879 (newer julgamento codes)
- 12032, 12184-12792 range (bulk newer codes)
- 14210-14848, 14937 (recent additions)
- 15022-15214 (latest parametrization)
- Special: 123 (Remessa) with complement `18:motivo_da_remessa:38` or `194`
- Special: 22, 246, 466, 488, 853

### Meta 2 — DESS (Desarquivamento)

CNJ codes: **893, 12066**
- 893 = Desarquivamento
- 12066 = Levantada suspensão/sobrestamento por cumprimento

Note: Only inserts if NOT already SENT (INC2025.0065716 fix).

### Meta 2 — SUSP (Suspensão)

CNJ code: **12065**
- 12065 = Sobrestamento/Suspensão (por cumprimento)

**GAP FOUND: Code 12065 is NOT in our situacoes.json (neither EJUD nor EPROC2G).**

### Meta 2 — SAIDA (Saída do Acervo)

- 10966 (Evolução de Classe) with classes: 151,152,156,12078,12246
- 85 (Petição) with complement `19:tipo_de_peticao:52`

### Meta 2 — NOVO_ANT

Cases with NOVO before `year - 156 months` (very old cases, 13+ years).

### Meta 2 — NAO_JULG

Cases with NOVO or DESS that have NO subsequent SENT/SAIDA/SUSP.

## Meta 10 — Same Structure, Filtered by Theme

Themes (via `VM_XML_ASSUNTO`):
- AMB (Ambiental): 72 assunto codes, class 293
- IND (Indígena): 7 assunto codes (9989, 12824, 13199, 9901, 15220, 10102, 3647)
- QUI (Quilombola): 2 assunto codes (12031, 12825)

Same NOVO/SENT/DESS/SUSP/SAIDA logic per theme.

## Cross-Reference Results vs Our situacoes.json

### Coverage Status

| Indicator | MPS Codes | In Our Mapping | Status |
|-----------|-----------|----------------|--------|
| NOVO | 6 base codes | 6/6 (100%) | OK |
| SENT | 240 codes | 240/240 (100%) | OK |
| DESS | 893, 12066 | 2/2 (100%) | OK |
| SUSP | 12065 | 1/1 (100%) | FIXED — added to sit 46 |
| SAIDA | 10966, 85 | 2/2 (100%) | OK |
| Remessa 123 | 1 code | 1/1 (100%) | OK |

### SENT Codes → Our Situacoes

| Situacao | Explanation | # SENT Codes |
|----------|-------------|--------------|
| 2 | Arquivado definitivamente | 1 (246) |
| 10 | Baixado definitivamente | 1 (22) |
| 23 | Distribuição cancelada | 1 (488) |
| 27 | Julgado com resolução do mérito | 177 |
| 28 | Julgado sem resolução do mérito | 58 |
| 29 | Julgamento homologatório proferido | 4 |
| 62 | Julgado | 1 |

**Interpretation**: MPS considers ALL of these as "julgamento" for Meta 2 purposes. 
This includes 246 (Arquivamento), 22 (Baixa), 488 (Cancelamento) — broader than just "julgamento" in strict sense.

### The 12065 Gap — FIXED

Code 12065 = "Sobrestamento/Suspensão" (by cumprimento/compliance).
- Not a leaf in CNJ situação parametrization
- Was missing from EJUD and EPROC2G situacoes.json
- MPS uses it as the sole SUSP indicator

**Fix applied**: Added cod 12065 to situacao 46 ("Suspenso/sobrestado por decisão judicial") in both EJUD and EPROC2G.

## MPS OJ Resolution — BuscarOrgaoJulgadorMovimento

MPS resolves cod_oj per movement via chain:
```
MovimentoRelator.CodOrgJulg
  → LEFT JOIN CamaraCivelEspecializada (CodOrgJulgCiv → CodOrgJulgEspec)
  → JOIN OrgaoJulgador.CodOrgJulgCNJ
```

**CamaraCivelEspecializada**: small mapping table (27 rows) that remaps old civil chambers
to their current specialized equivalents. Example: CodOrgJulgCiv=12267 → CodOrgJulgEspec=23.
All rows map to either 23 (Direito Privado) or 24 (Direito Público).

Additional filters:
- Excludes cancelled distributions (via `MovCanceDistr`)
- Falls back through: last dist before movement → first dist after → single-OJ competência
- Returns NULL if no OJ found → movement NOT sent to CNJ

### Our approach (2inst_metacnj)

We read `ejud.movimentorelator.codorgjulg` directly without CamaraCivelEspecializada translation.

**Decision**: We chose NOT to apply CamaraCivelEspecializada remapping. Reason: using raw internal
codes exposes data mismatches that MPS would silently hide. If OJ code is wrong in source, we see
it and can act — MPS would just translate it to a valid-looking CNJ code, masking the problem.

Cancelled distribution filter: applied via `ejud.movcancedistr` anti-join in `_enrich_relator`.

### CamaraCivelEspecializada reference data

| CODCOMPTESPEC | CODORGJULGCIV | CODORGJULGESPEC | NOMEORGJULGESPEC |
|---------------|---------------|-----------------|------------------|
| 431 | 12267 | 23 | DECIMA CAMARA DE DIREITO PRIVADO |
| 8484 | 12287 | 23 | DECIMA PRIMEIRA CAMARA DE DIREITO PRIVADO |
| 444 | 12268 | 23 | DECIMA SEGUNDA CAMARA DE DIREITO PRIVADO |
| 8479 | 12269 | 23 | DECIMA TERCEIRA CAMARA DE DIREITO PRIVADO |
| 439 | 12288 | 23 | DECIMA QUARTA CAMARA DE DIREITO PRIVADO |
| 3905 | 12270 | 23 | DECIMA QUINTA CAMARA DE DIREITO PRIVADO |
| 434 | 12289 | 23 | DECIMA SEXTA CAMARA DE DIREITO PRIVADO |
| 8483 | 12290 | 23 | DECIMA SETIMA CAMARA DE DIREITO PRIVADO |
| 445 | 12291 | 23 | DECIMA OITAVA CAMARA DE DIREITO PRIVADO |
| 8482 | 12271 | 23 | DECIMA NONA CAMARA DE DIREITO PRIVADO |
| 438 | 12282 | 23 | PRIMEIRA CAMARA DE DIREITO PRIVADO |
| 441 | 12272 | 23 | VIGESIMA CAMARA DE DIREITO PRIVADO |
| 3904 | 12292 | 23 | VIGESIMA PRIMEIRA CAMARA DE DIREITO PRIVADO |
| 8480 | 12293 | 23 | VIGESIMA SEGUNDA CAMARA DE DIREITO PRIVADO |
| 433 | 12263 | 23 | SEGUNDA CAMARA DE DIREITO PRIVADO |
| 440 | 12260 | 24 | SEGUNDA CAMARA DE DIREITO PUBLICO |
| 448 | 12264 | 23 | TERCEIRA CAMARA DE DIREITO PRIVADO |
| 436 | 12261 | 24 | TERCEIRA CAMARA DE DIREITO PUBLICO |
| 435 | 12283 | 23 | QUARTA CAMARA DE DIREITO PRIVADO |
| 437 | 12280 | 24 | QUARTA CAMARA DE DIREITO PUBLICO |
| 8481 | 12265 | 23 | QUINTA CAMARA DE DIREITO PRIVADO |
| 446 | 12262 | 24 | QUINTA CAMARA DE DIREITO PUBLICO |
| 443 | 12284 | 23 | SEXTA CAMARA DE DIREITO PRIVADO |
| 8478 | 12281 | 24 | SEXTA CAMARA DE DIREITO PUBLICO |
| 442 | 12285 | 23 | SETIMA CAMARA DE DIREITO PRIVADO |
| 447 | 12266 | 23 | OITAVA CAMARA DE DIREITO PRIVADO |
| 432 | 12286 | 23 | NONA CAMARA DE DIREITO PRIVADO |

---

## Key Architectural Insights

1. **TAB_XML_ANDAMENTO is pre-computed**: MPS procedure doesn't do the raw chain (movimento → faseexterna → integracao). It reads from a table that already has `Cod_Tip_Mov_Num` (CNJ codes). This is why MPS doesn't care about the translation chain gaps — they're already resolved elsewhere.

2. **First-occurrence semantics**: Every indicator uses `NOT EXISTS (earlier occurrence)`. This means only the FIRST distribution, FIRST julgamento, FIRST desarquivamento counts.

3. **Precedence**: SENT > DESS/SAIDA/SUSP. If a case is already SENT, it won't appear as DESS or SUSP (INC2025.0065716 fix from 2025).

4. **Complement matching**: Only two complement-based rules:
   - `85 + 19:tipo_de_peticao:52` = Petição Inicial (starts a case)
   - `123 + 18:motivo_da_remessa:38/194` = Remessa (counts as julgamento/resolution)

5. **Class filtering for SAIDA/NOVO**: Code 10966 (Evolução de Classe) only counts for specific classes that represent criminal/special proceedings evolution.

## Complete Loss Validation (All 10 Oracle Queries)

### IGNORE — Void/Old Data

| # | codfase | faseext | Description | Volume | Period | Why ignore |
|---|---------|---------|-------------|--------|--------|------------|
| 1 | 31 | 60007 | Baixa provisória | 1651 | old | Not in CNJ, void movement |
| 3 | 982 | 982 | Remessa Distribuidor | 339 | 1999-2002 | All pre-2003 data |
| 4 | 50010 | 50010 | Baixa diligência | 137 | 1998-2012 | EJUD custom, old, like baixa provisória |
| 10 | 50510 | 60011 | Pedido de Desarquivamento | 54946 | 2012-2026 | Internal request (3VP/NUGEP), NOT CNJ 893. Process may or may not get actual 893 desarquivamento after |

### HANDLED — Fallback/MPS Transform Already Covers

| # | codfase | faseext | Description | Volume | Why handled |
|---|---------|---------|-------------|--------|-------------|
| 2 | 10 | 8 | Complement mismatch | 799 | Two-pass fallback to base code |
| 7 | 3 | 3 | Decisão, codcompl=502 (Retenção) | 2067 | Fallback to parent cod 3 |
| 8 | 24 | 50193 | Julgamento, wildcards 59998/59999 | 43 | MPS transform 24→193 |

### NEEDS FIX — Force-to-Parent Transformations

| # | codfase | faseext | Description | Volume | Period | Fix |
|---|---------|---------|-------------|--------|--------|-----|
| 5 | 29 | 60006 | Decisão em Petição | 6434 | 2011-2026 | Force faseext 60006→3 (Decisão). Complements exist under CNJ Decisão tree |
| 6 | 30 | 60005 | Despacho em Petição | 52336 | 2011-2026 | Force faseext 60005→11009 (Despacho). Biggest recovery. Caveats: codcompl 59999 (wildcard, ignore), 218 (Sem Resolução Mérito, wrong tree — lives under 193 not 11009) |
| 9 | 11009 | 11009 | Despacho, codcompl=60438 (Aud. Mediação) | 91 | 2024-2026 | Active gap. codcompl=60438 not in integracao. Force to parent (Despacho base) |

### Query 5 Detail: Decisão em Petição (codfase=29)

Top complements — all exist under CNJ "Decisão" (cod 3) tree:
- codcompl=968 Não-Concessão (2427 records, active through May 2026)
- codcompl=1013 Determinação (1161, active)
- codcompl=163 Não-Recebimento (609, active)
- codcompl=83 Cancelamento da distribuição (392, active)
- codcompl=817 Concessão (251, active)
- codcompl=207 Não-Admissão (237, active)

### Query 6 Detail: Despacho em Petição (codfase=30)

Top complements — mixed:
- codcompl=11010 Mero expediente (36775, active) — exists under Despacho tree
- codcompl=59999 Indefinido (12954, active) — WILDCARD, ignore
- codcompl=11025 Suspensão/Sobrestamento (1203) — exists under Despacho tree
- codcompl=218 Sem Resolução Mérito (1047, active) — **WRONG TREE** (lives under Julgamento 193, not Despacho 11009)
- codcompl=11023 Concessão (159) — exists under Despacho tree

### Query 10 Detail: Pedido de Desarquivamento (codfase=50510)

Internal administrative movement. "Pedido" = request, not the act. Destinations are internal departments (3VP-NUGEP, 3VP-Divisão de Comunicação, etc.). The actual desarquivamento (CNJ 893) may or may not follow. This is NOT equivalent to CNJ 893 — it's an EJUD workflow step.

### Implementation Priority

1. **codfase=30→11009 (Despacho em Petição)**: 52K records, biggest single recovery
2. **codfase=29→3 (Decisão em Petição)**: 6.4K records, straightforward
3. Both require code change in `ejud_situacao_emr.py` — add faseexterna override before the join chain
