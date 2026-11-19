"""Cross-reference MPS procedure CNJ codes against our situacoes.json"""
import json

novo_codes = {26, 36, 391, 393, 849, 981}

sent_codes = {
    193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242,
    385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
    900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050,
    10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
    11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032,
    12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324,
    12326, 12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436,
    12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452,
    12453, 12475, 12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654,
    12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669,
    12670, 12671, 12672, 12673, 12674, 12675, 12676, 12677, 12678, 12679,
    12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689,
    12690, 12691, 12692, 12693, 12694, 12695, 12696, 12697, 12698, 12699,
    12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735,
    12792, 14210, 14211, 14213, 14214, 14215, 14216, 14217, 14219, 14680,
    14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165,
    15166, 15211, 15212, 15213, 15214, 466, 218, 228, 230, 235, 236, 244,
    454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853,
    10961, 11373, 11374, 11375, 11376, 11377, 11378, 11379, 11380, 11381,
    11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617,
    12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716, 12717, 12718,
    12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488
}

dess_codes = {893, 12066}
susp_codes = {12065}
saida_class_change = {10966}
saida_petition = {85}

all_mps_codes = novo_codes | sent_codes | dess_codes | susp_codes | saida_class_change | saida_petition | {123}

print(f"=== MPS PROCEDURE CODE EXTRACTION ===")
print(f"NOVO (distribuicao): {sorted(novo_codes)} ({len(novo_codes)} codes)")
print(f"SENT (julgamento): {len(sent_codes)} codes")
print(f"DESS (desarquivamento): {sorted(dess_codes)}")
print(f"SUSP (suspensao): {sorted(susp_codes)}")
print(f"SAIDA (class change): {sorted(saida_class_change)}")
print(f"SAIDA (petition): {sorted(saida_petition)} + complement 19:tipo_de_peticao:52")
print(f"Remessa 123 + complement 18:motivo_da_remessa:38/194")
print(f"TOTAL distinct codes: {len(all_mps_codes)}")
print()

with open(r"C:\Users\felip\Workspace\iceberg\2inst_situacao\ejud\json\situacoes.json", "r", encoding="utf-8") as f:
    sit_ejud = json.load(f)

our_codes = set()
code_to_sit = {}
for s in sit_ejud:
    for m in s.get("movimentos", []):
        c = m["cod_mni_movimento"]
        our_codes.add(c)
        if c not in code_to_sit:
            code_to_sit[c] = []
        code_to_sit[c].append((s["situacao"], s["explanation"]))

print(f"=== OUR situacoes.json (EJUD) ===")
print(f"Total distinct cod_mni_movimento: {len(our_codes)}")
print()

# SENT codes not in our mapping
in_mps_not_in_ours = sent_codes - our_codes
print(f"=== CROSS REFERENCE: SENT codes ===")
print(f"MPS SENT codes NOT in our situacoes.json: {len(in_mps_not_in_ours)}")
if in_mps_not_in_ours:
    for c in sorted(in_mps_not_in_ours):
        print(f"  {c}")
print()

mps_sent_in_ours = sent_codes & our_codes
print(f"MPS SENT codes that ARE in our mapping: {len(mps_sent_in_ours)}")

sent_situacoes = {}
for s in sit_ejud:
    for m in s.get("movimentos", []):
        if m["cod_mni_movimento"] in sent_codes:
            sit_num = s["situacao"]
            if sit_num not in sent_situacoes:
                sent_situacoes[sit_num] = {"explanation": s["explanation"], "codes": []}
            sent_situacoes[sit_num]["codes"].append(m["cod_mni_movimento"])

print(f"\nSituacoes that contain SENT (julgamento) codes:")
for sit_num in sorted(sent_situacoes.keys()):
    info = sent_situacoes[sit_num]
    print(f"  sit {sit_num:3d} ({info['explanation']}): {len(info['codes'])} codes")

# NOVO codes
print(f"\n=== CROSS REFERENCE: NOVO codes ===")
novo_in_ours = novo_codes & our_codes
novo_not_in_ours = novo_codes - our_codes
print(f"NOVO codes in our mapping: {sorted(novo_in_ours)}")
print(f"NOVO codes NOT in our mapping: {sorted(novo_not_in_ours)}")
for c in sorted(novo_codes):
    if c in code_to_sit:
        for sit, expl in code_to_sit[c]:
            print(f"  cod {c} -> sit {sit} ({expl})")
    else:
        print(f"  cod {c} -> NOT MAPPED")

# DESS codes
print(f"\n=== CROSS REFERENCE: DESS codes ===")
for c in sorted(dess_codes):
    if c in code_to_sit:
        for sit, expl in code_to_sit[c]:
            print(f"  cod {c} -> sit {sit} ({expl})")
    else:
        print(f"  cod {c} -> NOT MAPPED")

# SUSP
print(f"\n=== CROSS REFERENCE: SUSP codes ===")
for c in sorted(susp_codes):
    if c in code_to_sit:
        for sit, expl in code_to_sit[c]:
            print(f"  cod {c} -> sit {sit} ({expl})")
    else:
        print(f"  cod {c} -> NOT MAPPED")

# SAIDA
print(f"\n=== CROSS REFERENCE: SAIDA codes ===")
for c in sorted(saida_class_change | saida_petition):
    if c in code_to_sit:
        for sit, expl in code_to_sit[c]:
            print(f"  cod {c} -> sit {sit} ({expl})")
    else:
        print(f"  cod {c} -> NOT MAPPED")

# Remessa 123
print(f"\n=== CROSS REFERENCE: Remessa 123 ===")
if 123 in code_to_sit:
    for sit, expl in code_to_sit[123]:
        print(f"  cod 123 -> sit {sit} ({expl})")
else:
    print(f"  cod 123 -> NOT MAPPED")
