# Perguntas para Engenheiros de Dados - 1 Instncia

**Contexto**: Estamos construindo o pipeline de 2 instncia (EJUD + EPROC2G) para clculo de situao CNJ e metas. Analisamos os procedures Oracle da MPS e identificamos pontos que precisamos alinhar com vocs.

---

## 1. TAB_XML_ANDAMENTO

A MPS usa a `TAB_XML_ANDAMENTO` como fonte pr-computada para o clculo de metas.
Ns replicamos essa lgica no pipeline `2inst_situacao` (tabela Iceberg `tb_tf_situacao_ejud`).

**Perguntas:**
- Vocs leem da `TAB_XML_ANDAMENTO` diretamente ou reconstroem dos movimentos brutos?
- Se reconstroem: usam a mesma cadeia de fallback (`BUSCARDADOS_FASECOMPL_CNJ` nveis 5→4→3→2)?
- Qual tabela do Iceberg vocs usam como equivalente?

---

## 2. Vigncia de OJ (rgo Julgador)

A MPS resolve OJ por movimento via `BuscarOrgaoJulgadorMovimento`:
```
MovimentoRelator.CodOrgJulg
  → LEFT JOIN CamaraCivelEspecializada
  → JOIN OrgaoJulgador.CodOrgJulgCNJ
```

Ns usamos `ejud.movimentorelator.codorgjulg` direto (sem traduo CamaraCivelEspecializada).

**Perguntas:**
- Vocs aplicam a traduo `CamaraCivelEspecializada` → `OrgaoJulgador.CodOrgJulgCNJ`?
- A tabela de vigncia de OJ que vocs construram (`tb_datajud_processo_oj_vigencia`) j est em produo? Podemos consultar?
- Para o DataJud, o campo `cod_oj` precisa ser o cdigo CNJ (`CodOrgJulgCNJ`) ou o cdigo interno funciona?

---

## 3. Distribuies Canceladas

A MPS exclui movimentos com distribuio cancelada (`MovCanceDistr`/`MovCanceDist`).
Ns aplicamos anti-join contra `ejud.movcancedistr`.

**Perguntas:**
- Vocs tambm filtram distribuies canceladas no pipeline de 1inst?
- A tabela que vocs usam  a mesma (`movcancedistr` / `movcanceldistr`)?

---

## 4. Movimentos sem Traduo CNJ

Identificamos ~59.000 movimentos EJUD 2inst sem traduo na cadeia `integracaofasecompl_ejud_cnj`:
- `codfase=30, faseext=60005` (Despacho em Petio): 52.336 registros
- `codfase=29, faseext=60006` (Deciso em Petio): 6.434 registros
- `codfase=11009, codcompl=60438` (Aud. Mediao): 91 registros

A MPS tambm no traduz esses (retorna NULL → no envia ao CNJ).

**Perguntas:**
- Vocs encontraram gaps similares em 1inst?
- Existe algum mapeamento adicional que vocs criaram para cobrir esses casos?
- Como vocs lidam com movimentos sem traduo (excluem silenciosamente ou marcam como pendente)?

---

## 5. Modelo Cannico DataJud

Vimos que 1inst tem o modelo cannico completo (`datajud_cnj/docs/`).

**Perguntas:**
- As tabelas cannicas (`tb_datajud_processo`, `tb_datajud_processo_oj_vigencia`, `tb_datajud_processo_evento`) j esto rodando?
- Qual a frequncia de atualizao?
- Tem alguma documentao sobre como adaptar para 2inst (diferenas de fonte)?
- O schema do Iceberg (Glue Catalog)  compartilhado ou separado por instncia?

---

## 6. Integrao / Prximos Passos

Queremos:
1. Comparar nosso resultado de situao com o `datamart.id_situacao_atual` do DataJud
2. Produzir dois resultados: **nosso clculo** vs **clculo filtrado por regras MPS**
3. Apresentar divergncias para diretores decidirem qual regra vale

**Perguntas:**
- Qual a melhor fonte para consultar `datamart.id_situacao_atual` de G2? PostgreSQL (`172.20.30.141:55432`)? CSV bulk? API?
- Vocs j fizeram essa comparao para G1? Qual foi o % de match?
- Tem interesse em unificar pipeline ou preferem manter separado por instncia?

---

## Referncia: Documentao Criada

Toda anlise dos procedures MPS est documentada em:
- `iceberg/2inst_datajud/mps_procedure_analysis.md` — Anlise completa dos procedures Oracle
- `iceberg/2inst_datajud/docs.md` — Viso geral do projeto DataJud 2inst
- `iceberg/2inst_datajud/PLAN.md` — Plano de implementao com fases

Notebooks:
- `iceberg/2inst_datajud/analise_mps_vs_nosso.ipynb` — Comparativo MPS vs nosso pipeline

---

*Preparado por: Equipe 2 Instncia | Data: Maio/2026*
