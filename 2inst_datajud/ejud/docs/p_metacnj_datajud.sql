create or replace PROCEDURE METAS_CNJ_DATAJUD_2024_GERA
(
	P_META		NUMBER,
	P_DTHRINI	DATE := null,
	P_CODDOC	T_PROCESSO.CODDOC%TYPE := null
)
is
	V_DTHRINI 		DATE;
	V_DTHRFIM		TIMESTAMP;
	v_Cont			Number;
	v_DestinoEmails		varchar2(4000);
	v_RemetenteEmails	varchar2(200);
	v_DTHRFIM_Meta_2	DATE;
	v_DTHRFIM_Meta_2_Ant	DATE;
	v_DTHRFIM_Meta_10	DATE;

begin

	if nvl(sval_const('$$JobMetaCNJDatajudEmManut'), 'NAO') = 'SIM' then return; end if;

	if p_META in (1,4,8,11) then
		METAS_CNJ_DATAJUD_2023_GERA (P_META, P_DTHRINI, P_CODDOC);
		return;
	end if;

	--se não vier nenhuma data inicial, calcula a partir de 1/1 do ano corrente até 'hoje'... caso contrário calcula para o ano informado, de 1/1 até 31/12
	if p_dtHrIni is null then
		V_DTHRINI := trunc(sysdate,'yyyy');
		v_DtHrFim := trunc(sysdate);
	else
		V_DTHRINI := trunc(p_DtHrIni,'yyyy');

		--Se for o ano corrente, daí calcula até o zero horas do dia de hoje, caso contrário calcula para o ano todo
		if V_DTHRINI = trunc(sysdate,'yyyy') then
			v_DtHrFim := trunc(sysdate);
		else
			if V_DTHRINI < trunc(sysdate,'yyyy') then
				v_DtHrFim := add_months(trunc(V_DTHRINI,'mm'),12);
			else
				raise_application_error(-20101,'Data inicial não pode ser de ano maior que o atual');
			end if;
		end if;

	end if;

	v_DTHRFIM_Meta_2 := add_months(trunc(V_DTHRINI,'yyyy'),-24);
	v_DTHRFIM_Meta_2_Ant := add_months(trunc(V_DTHRINI,'yyyy'),-156);
	v_DTHRFIM_Meta_10:= add_months(trunc(V_DTHRINI,'yyyy'), 0);

	if nvl(sval_const('$$JobGeraMetasDataJudEnvMail'), 'NAO') = 'SIM' then

		v_RemetenteEmails := 'ejud.notificacao@tjrj.jus.br';
		v_DestinoEmails := nvl(sval_const('$$JobMetasCNJEmailsDestino'),'milton.takahashi@mps.com.br');

		PCK_EMAIL.prc_envia_email
		(	v_RemetenteEmails,
			pck_email.split(v_DestinoEmails,';'),
			null,
			to_char(systimestamp, 'YYYY/MM/DD hh24:mi:ss,ff6') || ' - ' || SYS_CONTEXT('USERENV','CURRENT_SCHEMA') || ' - DATAJUD - Geracao da Meta ' || nvl(to_char(P_META),'TODAS') || ' - INICIO: '|| to_char(sysdate,'dd/mm/yyyy hh24:mi:ss') || ' - DataIni: ' || V_DTHRINI || '.',
			'INICIO da execucao da procedure METAS_CNJ_DATAJUD_2024_GERA: '|| to_char(sysdate,'dd/mm/yyyy hh24:mi:ss')
		);
	end if;

	v_DtHrFim := v_DtHrFim-1*(1/24/60/60);--um segundo

	----- Depuração -----
	dbms_output.put_line('Datas inicial e final: ' || V_DTHRINI || ' - ' || V_DTHRFIM);
	---------------------

	delete MetasCNJ_DATAJUD where Meta = P_META and ANO = trunc(v_DtHrIni,'yyyy') and (P_CODDOC is null or CodDoc = P_CODDOC);

	IF P_META = 2 THEN

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, 'NOVO' as Indicador, p.coddoc, tp.codext as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador
		from	TAB_XML_ANDAMENTO m, t_Processo p, TipoProcesso tp
		where	( (m.Cod_Tip_Mov_Num in (26, 36, 391, 393, 849, 981)) or
		          (m.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246,282,283,287,288,289,290,293,294,295,297,299,300,302,1033,1317,10943,10944,11037,11528)) or
		          (m.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52')
			)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (26, 36, 391, 393, 849, 981) and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246,282,283,287,288,289,290,293,294,295,297,299,300,302,1033,1317,10943,10944,11037,11528) and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52' and m2.DtHrMov < m.DtHrMov)
		and	m.Cod_Classe not in (39,49,108)
		and	m.CodDoc = p.CodDoc
		and	(P_CODDOC is null or p.CodDoc = P_CODDOC)
		and	p.codtipproc = tp.codtipproc
		and	p.codcompt not in (9,10,11,14,15,19,20,22)
		and	m.Cod_OJ is not null
		and	m.dthrmov < V_DTHRFIM_Meta_2
		and	not exists (	select	1
					from	TAB_XML_ANDAMENTO m0
					where	( (m0.Cod_Tip_Mov_Num in (	193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
										900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
										11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
										12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
										12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
										12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
										12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
										14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
										218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
										11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
										12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488)) or
						  (m0.Cod_Tip_Mov_Num = 123 and (Complemento like '18:motivo_da_remessa:38%' or Complemento like '18:motivo_da_remessa:194%'))
						)
					and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m0.CodDoc and m2.Cod_Tip_Mov_Num in (
										193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
										900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
										11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
										12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
										12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
										12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
										12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
										14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
										218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
										11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
										12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488) and m2.DtHrMov < m0.DtHrMov)
					and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m0.CodDoc and m2.Cod_Tip_Mov_Num = 123 and (m2.Complemento like '18:motivo_da_remessa:38%' or m2.Complemento like '18:motivo_da_remessa:194%') and m2.DtHrMov < m0.DtHrMov)
					and	m0.CodDoc = m.CodDoc
					and	(P_CODDOC is null or m0.CodDoc = P_CODDOC)
					and	m0.Cod_OJ is not null
					and	m0.dthrmov < V_DTHRINI);

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, 'SENT' as Indicador, m.coddoc, (select tp.codext from TipoProcesso tp, t_Processo p where tp.CodTipProc = p.CodTipProc and p.CodDoc = d.CodDoc) as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador
		from	MetasCNJ_DATAJUD d, TAB_XML_ANDAMENTO m
		where	( (m.Cod_Tip_Mov_Num in (	193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
							900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
							11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
							12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
							12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
							12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
							12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
							14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
							218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
							11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
							12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488)) or
		          (m.Cod_Tip_Mov_Num = 123 and (Complemento like '18:motivo_da_remessa:38%' or Complemento like '18:motivo_da_remessa:194%'))
			)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (
							193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
							900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
							11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
							12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
							12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
							12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
							12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
							14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
							218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
							11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
							12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488) and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num = 123 and (m2.Complemento like '18:motivo_da_remessa:38%' or m2.Complemento like '18:motivo_da_remessa:194%'))
		and	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador = 'NOVO'
		and	d.CodDoc = m.CodDoc
		and	(P_CODDOC is null or m.CodDoc = P_CODDOC)
		and	m.Cod_OJ is not null
		and	m.dthrmov between V_DTHRINI and V_DTHRFIM;

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, 'DESS' as Indicador, m.coddoc, (select tp.codext from TipoProcesso tp, t_Processo p where tp.CodTipProc = p.CodTipProc and p.CodDoc = d.CodDoc) as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador
		from	MetasCNJ_DATAJUD d, TAB_XML_ANDAMENTO m
		where	m.Cod_Tip_Mov_Num in (893, 12066)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (893, 12066) and m2.DtHrMov < m.DtHrMov)
		and	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador = 'NOVO'
		and	d.CodDoc = m.CodDoc
		--INC2025.0065716 - Processos já julgados não retornam como DESS
		and	not exists (select 1 from MetasCNJ_DATAJUD d2 where d2.CodDoc = m.CodDoc and d2.Indicador in ('SENT') and d2.DtHrMov < m.DtHrMov)
		and	(P_CODDOC is null or m.CodDoc = P_CODDOC)
		and	m.Cod_OJ is not null
		and	m.dthrmov between V_DTHRINI and V_DTHRFIM;

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, 'SUSP' as Indicador, m.coddoc, (select tp.codext from TipoProcesso tp, t_Processo p where tp.CodTipProc = p.CodTipProc and p.CodDoc = d.CodDoc) as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador
		from	MetasCNJ_DATAJUD d, TAB_XML_ANDAMENTO m
		where	m.Cod_Tip_Mov_Num in (12065)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (12065) and m2.DtHrMov < m.DtHrMov)
		and	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador = 'NOVO'
		and	d.CodDoc = m.CodDoc
		--INC2025.0065716 - Processos já julgados não retornam como SUSP
		and	not exists (select 1 from MetasCNJ_DATAJUD d2 where d2.CodDoc = m.CodDoc and d2.Indicador in ('SENT') and d2.DtHrMov < m.DtHrMov)
		and	(P_CODDOC is null or m.CodDoc = P_CODDOC)
		and	m.Cod_OJ is not null
		and	m.dthrmov between V_DTHRINI and V_DTHRFIM;

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, 'SAIDA' as Indicador, m.coddoc, (select tp.codext from TipoProcesso tp, t_Processo p where tp.CodTipProc = p.CodTipProc and p.CodDoc = d.CodDoc) as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador

		from	MetasCNJ_DATAJUD d, TAB_XML_ANDAMENTO m
		where	( (m.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246)) or
		          (m.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52')
			)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52' and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246) and m2.DtHrMov < m.DtHrMov)
		and	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador = 'NOVO'
		and	d.CodDoc = m.CodDoc
		--INC2025.0065716 - Processos já julgados não retornam como SAIDA
		and	not exists (select 1 from MetasCNJ_DATAJUD d2 where d2.CodDoc = m.CodDoc and d2.Indicador in ('SENT') and d2.DtHrMov < m.DtHrMov)
		and	(P_CODDOC is null or m.CodDoc = P_CODDOC)
		and	m.Cod_OJ is not null
		and	m.dthrmov between V_DTHRINI and V_DTHRFIM;

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	d.DTHRGERA, d.ANO, d.META, 'NOVO_ANT' as Indicador, d.CODDOC, d.CODTIPPROCEXT, d.DTHRMOV, d.CODMAG, d.CODORGJULG
		from	MetasCNJ_DATAJUD d
		where	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador = 'NOVO'
		and	(P_CODDOC is null or d.CodDoc = P_CODDOC)
		and	d.dthrmov < v_DTHRFIM_Meta_2_Ant;

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	d.DTHRGERA, d.ANO, d.META, d.Indicador || '_ANT' as Indicador, d.CODDOC, d.CODTIPPROCEXT, d.DTHRMOV, d.CODMAG, d.CODORGJULG
		from	MetasCNJ_DATAJUD d
		where	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador in ('SENT','DESS','SAIDA','SUSP')
		and	exists (select 1 from MetasCNJ_DATAJUD d2 where d2.DtHrGera = V_DTHRFIM and d2.Meta = p_META and d2.Indicador = 'NOVO_ANT' and d2.CodDoc = d.CodDoc)
		and	(P_CODDOC is null or d.CodDoc = P_CODDOC);

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, 'NAO_JULG' as Indicador, d.coddoc, (select tp.codext from TipoProcesso tp, t_Processo p where tp.CodTipProc = p.CodTipProc and p.CodDoc = d.CodDoc) as CodTipProcExt,
			d.DtHrMov, RELATOR_DATA(d.CodDoc, d.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(d.CodDoc, d.DtHrMov, 'S') as OrgaoJulgador
		from	MetasCNJ_DATAJUD d
		where	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador in ('NOVO','DESS')
		and	(P_CODDOC is null or d.CodDoc = P_CODDOC)
		--and	d.dthrmov between V_DTHRINI and V_DTHRFIM
		and	not exists (	select	1
					from	MetasCNJ_DATAJUD d2
					where	d2.CodDoc = d.CodDoc
					and	d2.DtHrGera = V_DTHRFIM
					and	d2.Meta = p_META
					and	d2.DtHrMov > d.DtHrMov
					and	d2.Indicador in ('SENT','SAIDA','SUSP'));

	ELSIF P_META = 10 THEN

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, 'NOVO_AMB' as Indicador, p.coddoc, tp.codext as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador
		from	TAB_XML_ANDAMENTO m, t_Processo p, TipoProcesso tp
		where	( (m.Cod_Tip_Mov_Num in (26, 36, 391, 393, 849, 981)) or
		          (m.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246,282,283,287,288,289,290,293,294,295,297,299,300,302,1033,1317,10943,10944,11037,11528)) or
		          (m.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52')
			)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (26, 36, 391, 393, 849, 981) and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246,282,283,287,288,289,290,293,294,295,297,299,300,302,1033,1317,10943,10944,11037,11528) and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52' and m2.DtHrMov < m.DtHrMov)
		and	m.Cod_Proc in (select a.Cod_Proc from VM_XML_ASSUNTO a where a.Cod_Assunto in (	3423, 3424, 3429, 3619, 14779, 14780, 14785, 14783, 14781, 14782, 14784, 3620, 14790, 14792, 14786, 14788, 14787, 14789, 14791,
													14793, 3621, 14798, 14795, 14799, 14796, 14797, 14794, 3622, 3623, 14805, 14804, 14803, 3624, 3626, 3627, 3651, 4794, 5950, 5958,
													5961, 5962, 9583, 10016, 10018, 10085, 10086, 10091, 10094, 10095, 10102, 10103, 10104, 10105, 10106, 10108, 10110, 10111, 10112,
													10113, 10114, 10115, 10116, 10118, 10119, 10396, 10397, 10438, 10986, 11822, 11823, 11824, 11825, 11826, 11827, 11828, 11829,
													11830, 11836, 11841, 11853, 11862, 11873))
		and	m.Cod_Classe in (293)
		and	m.CodDoc = p.CodDoc
		and	(P_CODDOC is null or p.CodDoc = P_CODDOC)
		and	p.codtipproc = tp.codtipproc
		and	p.codcompt not in (9,10,11,14,15,19,20,22)
		and	m.Cod_OJ is not null
		and	m.dthrmov < v_DTHRFIM_Meta_10
		and	not exists (	select	1
					from	TAB_XML_ANDAMENTO m0
					where	( (m0.Cod_Tip_Mov_Num in (	193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
										900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
										11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
										12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
										12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
										12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
										12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
										14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
										218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
										11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
										12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488)) or
						  (m0.Cod_Tip_Mov_Num = 123 and (Complemento like '18:motivo_da_remessa:38%' or Complemento like '18:motivo_da_remessa:194%'))
						)
					and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m0.CodDoc and m2.Cod_Tip_Mov_Num in (
										193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
										900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
										11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
										12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
										12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
										12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
										12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
										14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
										218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
										11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
										12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488) and m2.DtHrMov < m0.DtHrMov)
					and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m0.CodDoc and m2.Cod_Tip_Mov_Num = 123 and (m2.Complemento like '18:motivo_da_remessa:38%' or m2.Complemento like '18:motivo_da_remessa:194%') and m2.DtHrMov < m0.DtHrMov)
					and	m0.CodDoc = m.CodDoc
					and	(P_CODDOC is null or m0.CodDoc = P_CODDOC)
					and	m0.Cod_OJ is not null
					and	m0.dthrmov < V_DTHRINI);

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, 'NOVO_IND' as Indicador, p.coddoc, tp.codext as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador
		from	TAB_XML_ANDAMENTO m, t_Processo p, TipoProcesso tp
		where	( (m.Cod_Tip_Mov_Num in (26, 36, 391, 393, 849, 981)) or
		          (m.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246,282,283,287,288,289,290,293,294,295,297,299,300,302,1033,1317,10943,10944,11037,11528)) or
		          (m.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52')
			)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (26, 36, 391, 393, 849, 981) and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246,282,283,287,288,289,290,293,294,295,297,299,300,302,1033,1317,10943,10944,11037,11528) and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52' and m2.DtHrMov < m.DtHrMov)
		and	m.Cod_Proc in (select a.Cod_Proc from VM_XML_ASSUNTO a where a.Cod_Assunto in (9989, 12824, 13199, 9901, 15220, 10102, 3647))
		and	m.CodDoc = p.CodDoc
		and	(P_CODDOC is null or p.CodDoc = P_CODDOC)
		and	p.codtipproc = tp.codtipproc
		and	p.codcompt not in (9,10,11,14,15,19,20,22)
		and	m.Cod_OJ is not null
		and	m.dthrmov < v_DTHRFIM_Meta_10
		and	not exists (	select	1
					from	TAB_XML_ANDAMENTO m0
					where	( (m0.Cod_Tip_Mov_Num in (	193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
										900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
										11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
										12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
										12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
										12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
										12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
										14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
										218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
										11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
										12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488)) or
						  (m0.Cod_Tip_Mov_Num = 123 and (Complemento like '18:motivo_da_remessa:38%' or Complemento like '18:motivo_da_remessa:194%'))
						)
					and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m0.CodDoc and m2.Cod_Tip_Mov_Num in (
										193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
										900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
										11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
										12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
										12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
										12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
										12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
										14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
										218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
										11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
										12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488) and m2.DtHrMov < m0.DtHrMov)
					and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m0.CodDoc and m2.Cod_Tip_Mov_Num = 123 and (m2.Complemento like '18:motivo_da_remessa:38%' or m2.Complemento like '18:motivo_da_remessa:194%') and m2.DtHrMov < m0.DtHrMov)
					and	m0.CodDoc = m.CodDoc
					and	(P_CODDOC is null or m0.CodDoc = P_CODDOC)
					and	m0.Cod_OJ is not null
					and	m0.dthrmov < V_DTHRINI);

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, 'NOVO_QUI' as Indicador, p.coddoc, tp.codext as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador
		from	TAB_XML_ANDAMENTO m, t_Processo p, TipoProcesso tp
		where	( (m.Cod_Tip_Mov_Num in (26, 36, 391, 393, 849, 981)) or
		          (m.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246,282,283,287,288,289,290,293,294,295,297,299,300,302,1033,1317,10943,10944,11037,11528)) or
		          (m.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52')
			)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (26, 36, 391, 393, 849, 981) and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246,282,283,287,288,289,290,293,294,295,297,299,300,302,1033,1317,10943,10944,11037,11528) and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52' and m2.DtHrMov < m.DtHrMov)
		and	m.Cod_Proc in (select a.Cod_Proc from VM_XML_ASSUNTO a where a.Cod_Assunto in (12031, 12825))
		and	m.CodDoc = p.CodDoc
		and	(P_CODDOC is null or p.CodDoc = P_CODDOC)
		and	p.codtipproc = tp.codtipproc
		and	p.codcompt not in (9,10,11,14,15,19,20,22)
		and	m.Cod_OJ is not null
		and	m.dthrmov < v_DTHRFIM_Meta_10
		and	not exists (	select	1
					from	TAB_XML_ANDAMENTO m0
					where	( (m0.Cod_Tip_Mov_Num in (	193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
										900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
										11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
										12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
										12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
										12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
										12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
										14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
										218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
										11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
										12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488)) or
						  (m0.Cod_Tip_Mov_Num = 123 and (Complemento like '18:motivo_da_remessa:38%' or Complemento like '18:motivo_da_remessa:194%'))
						)
					and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m0.CodDoc and m2.Cod_Tip_Mov_Num in (
										193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
										900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
										11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
										12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
										12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
										12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
										12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
										14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
										218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
										11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
										12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488) and m2.DtHrMov < m0.DtHrMov)
					and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m0.CodDoc and m2.Cod_Tip_Mov_Num = 123 and (m2.Complemento like '18:motivo_da_remessa:38%' or m2.Complemento like '18:motivo_da_remessa:194%') and m2.DtHrMov < m0.DtHrMov)
					and	m0.CodDoc = m.CodDoc
					and	(P_CODDOC is null or m0.CodDoc = P_CODDOC)
					and	m0.Cod_OJ is not null
					and	m0.dthrmov < V_DTHRINI);

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, decode(d.Indicador,'NOVO_AMB','SENT_AMB','NOVO_IND','SENT_IND','NOVO_QUI','SENT_QUI') as Indicador, m.coddoc, (select tp.codext from TipoProcesso tp, t_Processo p where tp.CodTipProc = p.CodTipProc and p.CodDoc = d.CodDoc) as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador
		from	MetasCNJ_DATAJUD d, TAB_XML_ANDAMENTO m
		where	( (m.Cod_Tip_Mov_Num in (	193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
							900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
							11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
							12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
							12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
							12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
							12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
							14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
							218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
							11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
							12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488)) or
		          (m.Cod_Tip_Mov_Num = 123 and (Complemento like '18:motivo_da_remessa:38%' or Complemento like '18:motivo_da_remessa:194%'))
			)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (
							193, 196, 208, 210, 212, 214, 219, 220, 221, 237, 238, 239, 240, 241, 242, 385, 442, 443, 444, 445, 446, 447, 448, 449, 450, 451, 452, 453, 455, 471,
							900, 901, 972, 973, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050, 10964, 10965, 11401, 11402, 11403, 11404, 11405, 11406, 11407, 11408,
							11409, 11411, 11795, 11796, 11801, 11876, 11877, 11878, 11879, 12032, 12187, 12252, 12253, 12254, 12257, 12258, 12321, 12322, 12323, 12324, 12326,
							12327, 12328, 12329, 12330, 12331, 12433, 12434, 12435, 12436, 12437, 12438, 12439, 12440, 12441, 12442, 12443, 12450, 12451, 12452, 12453, 12475,
							12615, 12616, 12649, 12650, 12651, 12652, 12653, 12654, 12660, 12661, 12662, 12663, 12664, 12665, 12666, 12667, 12668, 12669, 12670, 12671, 12672,
							12673, 12674, 12675, 12676, 12677, 12678, 12679, 12680, 12681, 12682, 12683, 12684, 12685, 12686, 12687, 12688, 12689, 12690, 12691, 12692, 12693,
							12694, 12695, 12696, 12697, 12698, 12699, 12700, 12701, 12702, 12703, 12704, 12705, 12706, 12707, 12708, 12735, 12792, 14210, 14211, 14213, 14214,
							14215, 14216, 14217, 14219, 14680, 14937, 15022, 15023, 15024, 15026, 15027, 15028, 15029, 15030, 15165, 15166, 15211, 15212, 15213, 15214, 466,
							218, 228, 230, 235, 236, 244, 454, 456, 457, 458, 459, 460, 461, 462, 463, 464, 465, 472, 473, 853, 10961, 11373, 11374, 11375, 11376, 11377, 11378,
							11379, 11380, 11381, 11394, 11396, 12184, 12256, 12298, 12319, 12325, 12458, 12459, 12617, 12709, 12710, 12711, 12712, 12713, 12714, 12715, 12716,
							12717, 12718, 12719, 12720, 12721, 12722, 12723, 12724, 14218, 14848, 246, 22, 488) and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num = 123 and (m2.Complemento like '18:motivo_da_remessa:38%' or m2.Complemento like '18:motivo_da_remessa:194%') and m2.DtHrMov < m.DtHrMov)
		and	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador in ('NOVO_AMB','NOVO_IND','NOVO_QUI')
		and	d.CodDoc = m.CodDoc
		and	(P_CODDOC is null or m.CodDoc = P_CODDOC)
		and	m.Cod_OJ is not null
		and	m.dthrmov between V_DTHRINI and V_DTHRFIM;

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, decode(d.Indicador,'NOVO_AMB','DESS_AMB','NOVO_IND','DESS_IND','NOVO_QUI','DESS_QUI') as Indicador, m.coddoc, (select tp.codext from TipoProcesso tp, t_Processo p where tp.CodTipProc = p.CodTipProc and p.CodDoc = d.CodDoc) as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador
		from	MetasCNJ_DATAJUD d, TAB_XML_ANDAMENTO m
		where	m.Cod_Tip_Mov_Num in (893, 12066)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (893, 12066) and m2.DtHrMov < m.DtHrMov)
		and	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador in ('NOVO_AMB','NOVO_IND','NOVO_QUI')
		and	d.CodDoc = m.CodDoc
		--INC2025.0065716 - Processos já julgados não retornam como DESS
		and	not exists (select 1 from MetasCNJ_DATAJUD d2 where d2.CodDoc = m.CodDoc and d2.Indicador in ('SENT_AMB','SENT_IND','SENT_QUI') and d2.DtHrMov < m.DtHrMov)
		and	(P_CODDOC is null or m.CodDoc = P_CODDOC)
		and	m.Cod_OJ is not null
		and	m.dthrmov between V_DTHRINI and V_DTHRFIM;

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, decode(d.Indicador,'NOVO_AMB','SUSP_AMB','NOVO_IND','SUSP_IND','NOVO_QUI','SUSP_QUI') as Indicador, m.coddoc, (select tp.codext from TipoProcesso tp, t_Processo p where tp.CodTipProc = p.CodTipProc and p.CodDoc = d.CodDoc) as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador
		from	MetasCNJ_DATAJUD d, TAB_XML_ANDAMENTO m
		where	m.Cod_Tip_Mov_Num in (12065)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (12065) and m2.DtHrMov < m.DtHrMov)
		and	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador in ('NOVO_AMB','NOVO_IND','NOVO_QUI')
		and	d.CodDoc = m.CodDoc
		--INC2025.0065716 - Processos já julgados não retornam como SUSP
		and	not exists (select 1 from MetasCNJ_DATAJUD d2 where d2.CodDoc = m.CodDoc and d2.Indicador in ('SENT_AMB','SENT_IND','SENT_QUI') and d2.DtHrMov < m.DtHrMov)
		and	(P_CODDOC is null or m.CodDoc = P_CODDOC)
		and	m.Cod_OJ is not null
		and	m.dthrmov between V_DTHRINI and V_DTHRFIM;

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, decode(d.Indicador,'NOVO_AMB','SAIDA_AMB','NOVO_IND','SAIDA_IND','NOVO_QUI','SAIDA_QUI') as Indicador, m.coddoc, (select tp.codext from TipoProcesso tp, t_Processo p where tp.CodTipProc = p.CodTipProc and p.CodDoc = d.CodDoc) as CodTipProcExt,
			m.DtHrMov, RELATOR_DATA(m.CodDoc, m.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(m.CodDoc, m.DtHrMov, 'S') as OrgaoJulgador

		from	MetasCNJ_DATAJUD d, TAB_XML_ANDAMENTO m
		where	( (m.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246)) or
		          (m.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52')
			)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (85) and Complemento = '19:tipo_de_peticao:52' and m2.DtHrMov < m.DtHrMov)
		and	not exists (select 1 from TAB_XML_ANDAMENTO m2 where m2.CodDoc = m.CodDoc and m2.Cod_Tip_Mov_Num in (10966) and Cod_Classe in (151,152,156,12078,12246) and m2.DtHrMov < m.DtHrMov)
		and	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador in ('NOVO_AMB','NOVO_IND','NOVO_QUI')
		and	d.CodDoc = m.CodDoc
		--INC2025.0065716 - Processos já julgados não retornam como SAIDA
		and	not exists (select 1 from MetasCNJ_DATAJUD d2 where d2.CodDoc = m.CodDoc and d2.Indicador in ('SENT_AMB','SENT_IND','SENT_QUI') and d2.DtHrMov < m.DtHrMov)
		and	(P_CODDOC is null or m.CodDoc = P_CODDOC)
		and	m.Cod_OJ is not null
		and	m.dthrmov between V_DTHRINI and V_DTHRFIM;

		INSERT INTO MetasCNJ_DATAJUD (DTHRGERA, ANO, META, INDICADOR, CODDOC, CODTIPPROCEXT, DTHRMOV, CODMAG, CODORGJULG)
		select	V_DTHRFIM as DtHrGera, v_DtHrIni as Ano, p_META AS META, 'NAO_JULG' as Indicador, d.coddoc, (select tp.codext from TipoProcesso tp, t_Processo p where tp.CodTipProc = p.CodTipProc and p.CodDoc = d.CodDoc) as CodTipProcExt,
			d.DtHrMov, RELATOR_DATA(d.CodDoc, d.DtHrMov, 'S') as Magistrado, ORGAOJULGADOR_DATA(d.CodDoc, d.DtHrMov, 'S') as OrgaoJulgador
		from	MetasCNJ_DATAJUD d
		where	d.DtHrGera = V_DTHRFIM
		and	d.Meta = p_META
		and	d.Indicador in ('NOVO_AMB','NOVO_IND','NOVO_QUI','DESS_AMB','DESS_IND','DESS_QUI')
		and	(P_CODDOC is null or d.CodDoc = P_CODDOC)
		--and	d.dthrmov between V_DTHRINI and V_DTHRFIM
		and	not exists (	select	1
					from	MetasCNJ_DATAJUD d2
					where	d2.CodDoc = d.CodDoc
					and	d2.DtHrGera = V_DTHRFIM
					and	d2.Meta = p_META
					and	d2.DtHrMov > d.DtHrMov
					and	d2.Indicador in ('SENT_AMB','SENT_IND','SENT_QUI','SAIDA_AMB','SAIDA_IND','SAIDA_QUI','SUSP_AMB','SUSP_IND','SUSP_QUI'));

	END IF;

	if nvl(sval_const('$$JobGeraMetasDataJudEnvMail'), 'NAO') = 'SIM' then

		v_RemetenteEmails := 'ejud.notificacao@tjrj.jus.br';
		v_DestinoEmails := nvl(sval_const('$$JobMetasCNJEmailsDestino'),'milton.takahashi@mps.com.br');

		PCK_EMAIL.prc_envia_email
		(	v_RemetenteEmails,
			pck_email.split(v_DestinoEmails,';'),
			null,
			to_char(systimestamp, 'YYYY/MM/DD hh24:mi:ss,ff6') || ' - ' || SYS_CONTEXT('USERENV','CURRENT_SCHEMA') || ' - DATAJUD - Geracao da Meta ' || nvl(to_char(P_META),'TODAS') || ' - FIM: '|| to_char(sysdate,'dd/mm/yyyy hh24:mi:ss') || ' - DataIni: ' || V_DTHRINI || '.',
			'FINAL da execucao da procedure METAS_CNJ_DATAJUD_2024_GERA: '|| to_char(sysdate,'dd/mm/yyyy hh24:mi:ss')
		);
	end if;

end;