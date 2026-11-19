import json, csv, re, glob

# Load files
with open(r'C:\Users\felip\Workspace\iceberg\2inst_situacao\ejud\json\situacoes.json', 'r', encoding='utf-8') as f:
    ejud_json = json.load(f)
with open(r'C:\Users\felip\Workspace\iceberg\2inst_situacao\eproc2g\json\situacoes.json', 'r', encoding='utf-8') as f:
    eproc_json = json.load(f)

cnj_file = glob.glob(r'C:\Users\felip\Downloads\situacoes-datamart*')[0]

ejud_idx = {e['situacao']: e for e in ejud_json}
eproc_idx = {e['situacao']: e for e in eproc_json}


def parse_cnj_csv(filepath):
    result = {}
    with open(filepath, 'r', encoding='cp1252') as f:
        reader = csv.reader(f, delimiter=';')
        next(reader)
        next(reader)
        for row in reader:
            if len(row) < 4 or not row[0].strip():
                continue
            sit_match = re.search(r'\((\d+)\)', row[0])
            if not sit_match:
                continue
            sit_num = int(sit_match.group(1))
            mov_text = row[3]

            entries = []
            chains = re.split(
                r'(?=(?:MAGISTRADO|SERVENTU|ESCRIV|CONTADOR|DISTRIBUIDOR)\s*\()',
                mov_text
            )

            for chain in chains:
                chain = chain.strip()
                if not chain:
                    continue

                brackets = re.findall(r'\[([^\]]+)\]', chain)
                complements = {}
                for bracket_content in brackets:
                    for part in bracket_content.split(';'):
                        parts = part.split(':')
                        if len(parts) >= 3:
                            try:
                                indicator = int(parts[0])
                                value = int(parts[2])
                            except ValueError:
                                continue
                            if indicator == 15:
                                complements['complemento1'] = value
                            elif indicator == 16:
                                complements['complemento2'] = value
                            elif indicator == 27:
                                complements['complemento1'] = value
                            elif indicator == 18:
                                complements['complemento1'] = value

                hier_part = re.sub(r'\[.*?\]', '', chain)
                codes = [int(x) for x in re.findall(r'\((\d+)\)', hier_part)]
                nivel1_codes = {1, 14, 15, 48, 18}
                movement_codes = [c for c in codes if c not in nivel1_codes]

                if not movement_codes:
                    continue

                leaf = movement_codes[-1]
                entry = {'cod_mni_movimento': leaf}
                entry.update(complements)
                entries.append(entry)

            seen = set()
            unique_entries = []
            for e in entries:
                key = (e['cod_mni_movimento'], e.get('complemento1'), e.get('complemento2'))
                if key not in seen:
                    seen.add(key)
                    unique_entries.append(e)

            result[sit_num] = unique_entries

    return result


cnj_data = parse_cnj_csv(cnj_file)
print(f'Parsed {len(cnj_data)} situacoes from CNJ CSV')

skip_sits = {25, 65, 88}

# ====== EJUD ANALYSIS ======
ejud_additions = {}

for sit_num, cnj_entries in cnj_data.items():
    if sit_num in skip_sits:
        continue
    if sit_num not in ejud_idx:
        continue

    existing = set()
    for m in ejud_idx[sit_num].get('movimentos', []):
        key = (m['cod_mni_movimento'], m.get('complemento1'), m.get('complemento2'))
        existing.add(key)

    for entry in cnj_entries:
        key = (entry['cod_mni_movimento'], entry.get('complemento1'), entry.get('complemento2'))
        if key not in existing:
            if sit_num not in ejud_additions:
                ejud_additions[sit_num] = []
            ejud_additions[sit_num].append(entry)
            existing.add(key)

total_ejud = sum(len(v) for v in ejud_additions.values())
print(f'\nEJUD: {total_ejud} entries to add across {len(ejud_additions)} situacoes')

for sit in sorted(ejud_additions.keys()):
    entries = ejud_additions[sit]
    codes = sorted(set(e['cod_mni_movimento'] for e in entries))
    print(f'  Sit {sit:3d}: +{len(entries)} -> {codes[:10]}')

# ====== APPLY EJUD UPDATES ======
for sit_num, additions in ejud_additions.items():
    ejud_idx[sit_num]['movimentos'].extend(additions)

with open(r'C:\Users\felip\Workspace\iceberg\2inst_situacao\ejud\json\situacoes.json', 'w', encoding='utf-8') as f:
    json.dump(ejud_json, f, ensure_ascii=False, indent=2)
    f.write('\n')

print(f'\nEJUD situacoes.json UPDATED')

# ====== EPROC2G ANALYSIS ======
eproc_additions = {}

for sit_num, cnj_entries in cnj_data.items():
    if sit_num in skip_sits:
        continue
    if sit_num not in eproc_idx:
        continue

    existing = set()
    for m in eproc_idx[sit_num].get('movimentos', []):
        key = (
            m['cod_mni_movimento'],
            m.get('cod_mni_complemento'),
            m.get('cod_mni_complemento_tabelado')
        )
        existing.add(key)

    for entry in cnj_entries:
        c1 = entry.get('complemento1')
        c2 = entry.get('complemento2')

        eproc_entry = {'cod_mni_movimento': entry['cod_mni_movimento']}
        eproc_key_parts = [entry['cod_mni_movimento'], None, None]

        if c1 is not None and c2 is not None:
            eproc_entry['cod_mni_complemento'] = 15
            eproc_entry['cod_mni_complemento_tabelado'] = c1
            eproc_key_parts = [entry['cod_mni_movimento'], 15, c1]
            # Also need second complement entry
            eproc_entry2 = {
                'cod_mni_movimento': entry['cod_mni_movimento'],
                'cod_mni_complemento': 16,
                'cod_mni_complemento_tabelado': c2
            }
        elif c1 is not None:
            eproc_entry['cod_mni_complemento'] = 15
            eproc_entry['cod_mni_complemento_tabelado'] = c1
            eproc_key_parts = [entry['cod_mni_movimento'], 15, c1]
            eproc_entry2 = None
        else:
            eproc_entry2 = None

        key = tuple(eproc_key_parts)
        if key not in existing:
            if sit_num not in eproc_additions:
                eproc_additions[sit_num] = []
            eproc_additions[sit_num].append(eproc_entry)
            existing.add(key)

total_eproc = sum(len(v) for v in eproc_additions.values())
print(f'\nEPROC2G: {total_eproc} entries to add across {len(eproc_additions)} situacoes')

for sit in sorted(eproc_additions.keys()):
    entries = eproc_additions[sit]
    codes = sorted(set(e['cod_mni_movimento'] for e in entries))
    print(f'  Sit {sit:3d}: +{len(entries)} -> {codes[:10]}')

# ====== APPLY EPROC2G UPDATES ======
for sit_num, additions in eproc_additions.items():
    eproc_idx[sit_num]['movimentos'].extend(additions)

with open(r'C:\Users\felip\Workspace\iceberg\2inst_situacao\eproc2g\json\situacoes.json', 'w', encoding='utf-8') as f:
    json.dump(eproc_json, f, ensure_ascii=False, indent=2)
    f.write('\n')

print(f'\nEPROC2G situacoes.json UPDATED')
