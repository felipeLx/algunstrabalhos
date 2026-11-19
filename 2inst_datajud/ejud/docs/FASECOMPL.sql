create or replace function BUSCARDADOS_FASECOMPL_CNJ(

/* Criação: Myrna Okamoto Ziwich
   Decrição: Busca o código do último nível do CNJ com os seus complementos.
*/
	p_CodDoc	Processo.CodDoc%TYPE,
	p_DtHrMov	Movimento.DtHrMov%TYPE,
	p_CodFaseExt	IntegracaoFaseComplementoCNJ.CodFaseNivel2%TYPE,
	p_CodCompl1	IntegracaoFaseComplementoCNJ.CodCompl1Nivel3%TYPE := null,
	p_CodCompl2	IntegracaoFaseComplementoCNJ.CodCompl2Nivel4%TYPE := null,
	p_CodCompl3	IntegracaoFaseComplementoCNJ.CodCompl3Nivel5%TYPE := null

)
RETURN VARCHAR2
is
	Result			varchar2(500);
	v_CodFaseNivel2CNJ	IntegracaoFaseComplementoCNJ.CodFaseNivel2CNJ%TYPE;
	v_CodCompl1Nivel3CNJ	IntegracaoFaseComplementoCNJ.CodCompl1Nivel3CNJ%TYPE;
	v_CodCompl2Nivel4CNJ	IntegracaoFaseComplementoCNJ.CodCompl2Nivel4CNJ%TYPE;
	v_CodCompl3Nivel5CNJ	IntegracaoFaseComplementoCNJ.CodCompl3Nivel5CNJ%TYPE;
	v_CodIndicador		IntegracaoFaseCompl_eJUD_CNJ.CodIndicador%TYPE;
	v_CodTabelado		IntegracaoFaseCompl_eJUD_CNJ.CodTabelado%TYPE;
	v_UltimoNivelCNJ	IntegracaoFaseCompl_eJUD_CNJ.UltimoNivelCNJ%TYPE;
	v_CodMovUltimoNivel	IntegracaoFaseComplementoCNJ.CodFaseNivel2CNJ%TYPE;
	v_DescrIndicador	varchar2(200);
	v_DescrComplRetorno	varchar2(500);
	v_auxIndicador1 	IntegracaoFaseCompl_eJUD_CNJ.CodIndicador%TYPE;
	v_auxIndicador2		IntegracaoFaseCompl_eJUD_CNJ.CodIndicador%TYPE;
	v_auxTabelado1		IntegracaoFaseCompl_eJUD_CNJ.CodTabelado%TYPE;
	v_auxTabelado2		IntegracaoFaseCompl_eJUD_CNJ.CodTabelado%TYPE;
	v_ClassAnt		TipoProcesso.Descr%TYPE;
	v_ClasseNova		TipoProcesso.Descr%TYPE;
	v_Data			Date;
	v_PosIndic		number;
	v_DescrAux1		varchar2(100);
	v_DescrAux2		varchar2(100);
	v_DescrAux3		varchar2(100);
	v_Tipo			varchar2(50);
	v_CodFaseExt		FaseExterna.CodFaseExt%TYPE;
	v_CodCompl		Complemento.CodCompl%TYPE;
	v_CodAux1		number;
	v_CodAux2		number;
	v_TipoResp		number;
	v_CodMotivRed		EstudoProcesso.CodMotivRed%TYPE;
	v_Count			number;

BEGIN

--	Result := '';

	v_CodCompl1Nivel3CNJ := null;
	v_CodCompl2Nivel4CNJ := null;
	v_CodCompl3Nivel5CNJ := null;
	v_CodMovUltimoNivel := null;

	--Verifica o último nível possível para a fase
	select 	max(UltimoNivelCNJ)
	into 	v_UltimoNivelCNJ
	from	IntegracaoFaseCompl_eJUD_CNJ
  	where 	CodFaseNivel2 = p_CodFaseExt
	and	IndAtivo = 'S';

	--Se o último nível for o Complemento 3 (Nivel5), pegar o valor do De-Para
	if v_UltimoNivelCNJ = 5 then
		select	min(CodNivel1), min(CodCompl3Nivel5CNJ), min(CodIndicador), min(CodTabelado)
		into	v_TipoResp, v_CodMovUltimoNivel, v_CodIndicador, v_CodTabelado
		from	IntegracaoFaseCompl_eJUD_CNJ
		where 	CodFaseNivel2 = p_CodFaseExt
		and	CodCompl1Nivel3 = p_CodCompl1
		and	CodCompl2Nivel4 = p_CodCompl2
		and	((CodCompl3Nivel5 = p_CodCompl3)
			or (CodCompl3Nivel5 is null and CodCompl3Nivel5CNJ is not null))	       --para os casos de colocar o valor default caso seja nulo
		and	IndAtivo = 'S';

		if v_CodMovUltimoNivel is null then
			v_UltimoNivelCNJ := v_UltimoNivelCNJ -1;
		end if;
	end if;

	--Se o último nível for o Complemento 2 (Nivel4), pegar o valor do De-Para
	if (v_CodMovUltimoNivel is null) and (v_UltimoNivelCNJ = 4) then

		select	min(CodNivel1), min(CodCompl2Nivel4CNJ), min(CodIndicador), min(CodTabelado)
		into	v_TipoResp, v_CodMovUltimoNivel, v_CodIndicador, v_CodTabelado
		from	IntegracaoFaseCompl_eJUD_CNJ
		where 	CodFaseNivel2 = p_CodFaseExt
		and	CodCompl1Nivel3 = p_CodCompl1
		and	((CodCompl2Nivel4 = p_CodCompl2)
			or (CodCompl2Nivel4 is null and CodCompl2Nivel4CNJ is not null))  -- para os casos de ser informado somente no DexPara
		and	(CodCompl3Nivel5 is null or (CodCompl3Nivel5 <> nvl(p_CodCompl3,0)))    -- para os casos que o complemento deveria estar preenchido por não ser último nível, mas está nulo ou o complemeneto não tem na tabela.
		and	IndAtivo = 'S';

		if v_CodMovUltimoNivel is null then
			v_UltimoNivelCNJ := v_UltimoNivelCNJ -1;
		end if;
	end if;

	--Se o último nível for o Complemento 1 (Nivel3), pegar o valor do De-Para
	if (v_CodMovUltimoNivel is null) and (v_UltimoNivelCNJ = 3) then

		select	min(CodNivel1), min(CodCompl1Nivel3CNJ), min(CodIndicador), min(CodTabelado)
		into	v_TipoResp, v_CodMovUltimoNivel, v_CodIndicador, v_CodTabelado
		from	IntegracaoFaseCompl_eJUD_CNJ
		where 	CodFaseNivel2 = p_CodFaseExt
 		and	((CodCompl1Nivel3 = p_CodCompl1 )
			or (CodCompl1Nivel3 is null and CodCompl1Nivel3CNJ is not null)
			or (UltimoNivelCNJ = 3 and CodFaseNivel2 = 8 and CodCompl1Nivel3 is not null and CodCompl1Nivel3 <> p_CodCompl1 )) --ficou fixo o 8 -
		and	(CodCompl2Nivel4 is null or (CodCompl2Nivel4 <> nvl(p_CodCompl2,0)))
		and	IndAtivo = 'S';

 		if v_CodMovUltimoNivel is null then
			v_UltimoNivelCNJ := v_UltimoNivelCNJ -1;
		end if;
	end if;

	--Se o último nível for a própria Fase (Nivel2), pegar o valor do De-Para
	if (v_CodMovUltimoNivel is null) and (v_UltimoNivelCNJ = 2) then

		select	min(CodNivel1), min(CodFaseNivel2CNJ), min(CodIndicador), min(CodTabelado)
		into	v_TipoResp, v_CodMovUltimoNivel, v_CodIndicador, v_CodTabelado
		from	IntegracaoFaseCompl_eJUD_CNJ
		where 	CodFaseNivel2 = p_CodFaseExt
		and	(CodCompl1Nivel3 is null or (CodCompl1Nivel3 <> nvl(p_CodCompl1,0)))
		and	IndAtivo = 'S'
		and	UltimoNivelCNJ = 2;

	end if;

	-- SE for Intimação Eletrônica
	if v_CodMovUltimoNivel in (12263, 12264) then

		select	count(1)
		into	v_CodAux1
		from (	select	dt_hr_intim
			from	pc_IntimacaoEletronicaOrgao pc, DocumentoArquivo da, MovIntimEntidade mi
			where	mi.CodDoc = p_CodDoc
			and	mi.DtHrmov = p_DtHrmov
			and	da.CodDoc = mi.CodDoc
			and	da.Seq = mi.SeqCarta
			and	pc.Cod_Docto_Elet = hextoraw(da.ArqGed)
			union
			select	dt_hr_intim
			from	pc_IntimacaoEletronicaOrgao pc, DocumentoArquivo da, MovIntimAdvogado mi
			where	mi.CodDoc = p_CodDoc
			and	mi.DtHrmov = p_DtHrmov
			and	da.CodDoc = mi.CodDoc
			and	da.Seq = mi.SeqCarta
			and	pc.Cod_Docto_Elet = hextoraw(da.ArqGed)
			union
			select  dt_hr_intim
			from	pc_IntimacaoEletronicaOrgao pc, DocumentoArquivo da, MovIntimPJ mi
			where	mi.CodDoc = p_CodDoc
			and	mi.DtHrmov = p_DtHrmov
			and	da.CodDoc = mi.CodDoc
			and	da.Seq = mi.SeqCarta
			and	pc.Cod_Docto_Elet = hextoraw(da.ArqGed)
		);

		if v_CodAux1 > 0 then
			v_CodMovUltimoNivel := 12266; --confirmada
		else
		 	v_CodMovUltimoNivel := 12265; --enviada
		end if;

	end if;


	--Se não tiver indicador ou tabelado, retorna o código do último nível do movimento
	if (v_CodIndicador is null) and (v_CodTabelado is null) then

		--se for 14 - serventuário, deve colocar 0. 1 - Magistrado
		if v_TipoResp = 14 then
			v_TipoResp := 0;
		end if;

		if v_CodMovUltimoNivel is not null then
			result := v_TipoResp ||';'||v_CodMovUltimoNivel;
		else
			result := '';
		end if;

		return result;
	end if;

	--SE A FASE TIVER INDICADOR TABELADO
	if (v_CodTabelado is not null) then

		v_auxTabelado1 := '';
		v_auxTabelado2 := '';

	        v_PosIndic := instr(v_CodTabelado, ',');
		if v_PosIndic > 0 then
			v_auxTabelado1 := substr(v_CodTabelado, 1, v_PosIndic-1);
			v_auxTabelado2 := substr(v_CodTabelado, v_PosIndic+1, length(v_CodTabelado));
		else
			v_auxTabelado1	:= v_CodTabelado;
			v_auxTabelado2 := '';
		end if;

		-- Se for Acórdão, Informacoes/Avisos, ou Certidao
		if (v_CodMovUltimoNivel = 581) and p_CodFaseExt in (50100, 50004, 50002)  then

 			--Pega o código Complemento do tabelado 1
			select	 min(t.CodComplTabeladoCNJ), min(t.CodTabeladoCNJ), min(c.DSC_COMPLEMENTO)
			into	v_CodAux1, v_CodAux2, v_DescrAux1
			from	IntegracaoeJudComplTabeladoCNJ t, INT_PC_CNJ_SGT_COMPLEMENTO c
			where	t.UltimoNivelCNJ = v_CodMovUltimoNivel
			and	t.CodComplTabeladoCNJ = v_auxTabelado1
			and	t.CodFaseExt =  p_CodFaseExt
			and 	c.SEQ_COMPLEMENTO  =t.CodComplTabeladoCNJ;

	 		v_DescrComplRetorno := to_char(v_CodAux1)||':'||v_DescrAux1||':'||to_char(v_CodAux2);

		--se for REDISTRIBUICAO, tem dois motivos tabelados, o segundo não é complemento é o CodMotivRed da tabela EstudoProcesso
		elsif (v_CodMovUltimoNivel = 36) then

			--Buscar o motivo da redistribuicao
			select	min(mde.CodMotivRed)
			into	v_CodMotivRed
			from	Movdistribuicaoestudo mde
			where	mde.CodDoc = p_CodDoc
			and	mde.DtHrMov = p_DtHrMov;

			--Pega o código Complemento do tabelado 1
			select	 min(t.CodComplTabeladoCNJ), min(t.CodTabeladoCNJ), min(c.DSC_COMPLEMENTO)
			into	v_CodAux1, v_CodAux2, v_DescrAux1
			from	IntegracaoeJudComplTabeladoCNJ t, INT_PC_CNJ_SGT_COMPLEMENTO c
			where	t.UltimoNivelCNJ = v_CodMovUltimoNivel
			and	t.CodComplTabeladoCNJ = v_auxTabelado1
			and	t.CodCompl = v_CodMotivRed
			and 	c.SEQ_COMPLEMENTO = t.CodComplTabeladoCNJ;

			if v_DescrAux1 is not null then
				v_DescrComplRetorno :=  to_char(v_CodAux1)||':'||v_DescrAux1||':'||to_char(v_CodAux2);
			end if;

			if v_auxTabelado2 is not null then

				--Pega o código Complemento do tabelado 2
				select	 min(t.CodComplTabeladoCNJ), min(t.CodTabeladoCNJ),min(c.DSC_COMPLEMENTO)
				into	v_CodAux1, v_CodAux2, v_DescrAux1
				from	IntegracaoeJudComplTabeladoCNJ t, INT_PC_CNJ_SGT_COMPLEMENTO c
				where	t.UltimoNivelCNJ = v_CodMovUltimoNivel
				and	t.CodComplTabeladoCNJ = v_auxTabelado2
				and	((t.CodCompl = p_CodCompl1)
					or (t.CodCompl = p_CodCompl2)
					or (t.CodCompl is null and (p_CodCompl1 is null  or p_CodCompl2 is null )))
				and	c.SEQ_COMPLEMENTO = t.CodComplTabeladoCNJ;

				v_DescrComplRetorno := v_DescrComplRetorno ||';'|| to_char(v_CodAux1)||':'||v_DescrAux1||':'||to_char(v_CodAux2)||';';
			end if;

		elsif (p_CodFaseExt = 970) then

			--Audiência: Tabelado1 = complemento2 e Tabelado2 = complemento3
			--36-dirigida_por (codcompl2) e 15-situacao_da_audiencia (codcompl3)

			--Pega o código Complemento do tabelado 1
			select	min(t.CodComplTabeladoCNJ), min(t.CodTabeladoCNJ), min(c.DSC_COMPLEMENTO)
			into	v_CodAux1, v_CodAux2, v_DescrAux1
			from	IntegracaoeJudComplTabeladoCNJ t, INT_PC_CNJ_SGT_COMPLEMENTO c
			where	t.UltimoNivelCNJ = v_CodMovUltimoNivel
			and	t.CodComplTabeladoCNJ = v_auxTabelado1
			and	t.CodCompl = p_CodCompl2
			and 	c.SEQ_COMPLEMENTO  = t.CodComplTabeladoCNJ;

			if v_DescrAux1 is not null then
				v_DescrComplRetorno :=  to_char(v_CodAux1)||':'||v_DescrAux1||':'||to_char(v_CodAux2);
			end if;

			if v_auxTabelado2 is not null then

				--Pega o código Complemento do tabelado 2
				select	 min(t.CodComplTabeladoCNJ), min(t.CodTabeladoCNJ),min(c.DSC_COMPLEMENTO)
				into	v_CodAux1, v_CodAux2, v_DescrAux1
				from	IntegracaoeJudComplTabeladoCNJ t, INT_PC_CNJ_SGT_COMPLEMENTO c
				where	t.UltimoNivelCNJ = v_CodMovUltimoNivel
				and	t.CodComplTabeladoCNJ = v_auxTabelado2
				and	t.CodCompl = p_CodCompl3
				and 	c.SEQ_COMPLEMENTO  = t.CodComplTabeladoCNJ;

				v_DescrComplRetorno := v_DescrComplRetorno ||';'|| to_char(v_CodAux1)||':'||v_DescrAux1||':'||to_char(v_CodAux2)||';';
			end if;

		else

			--Pega o código Complemento do tabelado 1
			select	 min(t.CodComplTabeladoCNJ), min(t.CodTabeladoCNJ), min(c.DSC_COMPLEMENTO)
			into	v_CodAux1, v_CodAux2, v_DescrAux1
			from	IntegracaoeJudComplTabeladoCNJ t, INT_PC_CNJ_SGT_COMPLEMENTO c
			where	t.UltimoNivelCNJ = v_CodMovUltimoNivel
			and	t.CodComplTabeladoCNJ = v_auxTabelado1
			and	((t.CodCompl = p_CodCompl1)
				or (t.CodCompl = p_CodCompl2)
				or (t.CodCompl is null and (p_CodCompl1 is null  or p_CodCompl2 is null )))
			and 	c.SEQ_COMPLEMENTO  = t.CodComplTabeladoCNJ;

			if v_DescrAux1 is not null then
				v_DescrComplRetorno :=  to_char(v_CodAux1)||':'||v_DescrAux1||':'||to_char(v_CodAux2);
			end if;

			if v_auxTabelado2 is not null then

				--Pega o código Complemento do tabelado 2
				select	 min(t.CodComplTabeladoCNJ), min(t.CodTabeladoCNJ),min(c.DSC_COMPLEMENTO)
				into	v_CodAux1, v_CodAux2, v_DescrAux1
				from	IntegracaoeJudComplTabeladoCNJ t, INT_PC_CNJ_SGT_COMPLEMENTO c
				where	t.UltimoNivelCNJ = v_CodMovUltimoNivel
				and	t.CodComplTabeladoCNJ = v_auxTabelado2
				and	((t.CodCompl = p_CodCompl1)
					or (t.CodCompl = p_CodCompl2)
					or (t.CodCompl is null and (p_CodCompl1 is null  or p_CodCompl2 is null )))
				and	c.SEQ_COMPLEMENTO = t.CodComplTabeladoCNJ;

				v_DescrComplRetorno := v_DescrComplRetorno ||';'|| to_char(v_CodAux1)||':'||v_DescrAux1||':'||to_char(v_CodAux2)||';';
			end if;
		end if;
	end if;

	--SE A FASE TIVER INDICADOR
	if (v_CodIndicador is not null) then

		v_auxIndicador1 := '';
		v_auxIndicador2 := '';

	        v_PosIndic := instr(v_CodIndicador, ',');
		if v_PosIndic > 0 then
			v_auxIndicador1 := substr(v_CodIndicador, 1, v_PosIndic-1);
			v_auxIndicador2 := substr(v_CodIndicador, v_PosIndic+1, length(v_CodIndicador));
		else
			v_auxIndicador1	:= v_CodIndicador;
			v_auxIndicador2 := '';
		end if;



		--Se for 1 nome_da_parte
		if (v_auxIndicador1 = '1') or (v_auxIndicador2 = '1') then

			--Fase: 50193 (193) (Julgamento) Compl1: 218 (Sem Resolução de Mérito) Compl2: 235 (Não-Conhecimento)
			--235	Não conhecido o recurso de #{tipo_de_peticao} de #{nome_da_parte}
			if v_CodMovUltimoNivel = 235 then
				--RESPOSTA: Identificador nome da parte e tabelado tipo_de_peticao.   Verificar se é possivel identificar o nome do autor (polo ativo) do recurso, e qual o tipo de recurso.

				--DÚVIDA: teria como identificar o tipo de recurso? Além de identificar teria ainda que associar ao tipo tabelado tipo_de_peticao.
				--(ok, mandaram o DeXPara do Tipo de petição)
				select	min(Cod_Tip_Pet_CNJ)
				into	v_CodAux1
				from	IntegracaoTipoPet_eJud_CNJ tp, Peticao pe, Processo p
				where	p.CodDoc = p_CodDoc
				and	pe.CodDoc = p.CodDocPet
				and	tp.CodTipPet = pe.CodTipPet;

				if v_CodAux1 is not null then
					v_DescrComplRetorno :=PARTEPROCESSOS_AUTOR(p_CodDoc)||';19:tipo_de_peticao:'||to_char(v_CodAux1);

				else
					--Busca o autor, se tiver mais de um, será coloca e outros no final.
					v_DescrComplRetorno := PARTEPROCESSOS_AUTOR(p_CodDoc);
				end if;

			elsif v_CodMovUltimoNivel in (237,238,239,240,241,242) then
			--Fase: 50193 (193) (Julgamento) Compl1: 385 (Sem Resolução de Mérito) Compl2: 237 (Provimento)
			--Fase: 50193 (193) (Julgamento) Compl1: 385 (Sem Resolução de Mérito) Compl2: 238 (Provimento em Parte)
			--Fase: 50193 (193) (Julgamento) Compl1: 385 (Sem Resolução de Mérito) Compl2: 239 (Não-Provimento)
			--Fase: 50193 (193) (Julgamento) Compl1: 385 (Sem Resolução de Mérito) Compl2: 240 (Conhecimento em Parte e Provimento)
			--Fase: 50193 (193) (Julgamento) Compl1: 385 (Sem Resolução de Mérito) Compl2: 241 (Conhecimento em Parte e Provimento em Parte)
			--Fase: 50193 (193) (Julgamento) Compl1: 385 (Sem Resolução de Mérito) Compl2: 242 (Conhecimento em Parte e Não-Provimento)

			--237	Conhecido o recurso de #{nome_da_parte} e provido
			--238	Conhecido o recurso de #{nome_da_parte} e provido em parte
			--239	Conhecido o recurso de #{nome_da_parte} e não-provido
			--240	Conhecido em parte o recurso de #{nome_da_parte} e provido
			--241	Conhecido em parte o recurso de #{nome_da_parte} e provido em parte
			--242	Conhecido em parte o recurso de #{nome_da_parte} e não-provido

				--Resposta: Identificado nome_da_parte.   Colocar o nome(s) do(s) autor(es) do recurso
				v_DescrComplRetorno := PARTEPROCESSOS_AUTOR(p_CodDoc);

				if v_DescrComplRetorno is null then

					select	min(pe.Nome)
					into	v_DescrComplRetorno
					from 	Pessoa pe, Parte p
					where	p.CodDoc = p_CodDoc
					and	p.NumOrd = (	select  min(p2.NumOrd)
  								from	Parte p2, ParteProcesso pp, tipoparte tp
 								where 	p2.CodDoc = p_CodDoc
   								and 	p2.CodParte = pp.CodParte
   								and	pp.codtipparte = tp.codtipparte
   								and	tp.codtippolo  in (2,4))
					and 	pe.CodPess = p.CodPess;
				end if;

			elsif v_CodMovUltimoNivel in (334) then
 			--Fase: 60003 (Decisão em Sessão) Compl1: 968 (Não-Concessão) 334 (Assistência judiciária gratuita)
			--334	Assistência Judiciária Gratuita não concedida a #{nome_da_parte}.

				--Tem algum lugar que indica que houve o pedido de Assistência Judiciária Gratuita?
				--Não dá para enviar por enquanto


				select	min(NomeDecRefParte)
				into	v_DescrComplRetorno
				from   (
					select	pe.Nome as NomeDecRefParte
					from 	Pessoa pe, Parte p, SessaoJulgDocumento sjd, MovJulg mj
					where	mj.CodDoc = p_CodDoc
					and	mj.DtHrMov = p_DtHrMov
					and	sjd.CodOrgJulg = mj.CodOrgJulg
					and	sjd.DtHrSess = mj.DtHrSess
					and	sjd.CodDec = mj.CodDec
					and	sjd.CodDoc = mj.CodDoc
					and	p.CodDoc = sjd.CodDoc
					and	p.CodParte = sjd.CodParteReferDec
					and	pe.CodPess = p.CodPess
					UNION
					select 	pe.Nome as NomeDecRefParte
					from 	Pessoa pe, Parte p, DespachoProcesso dp
					where	dp.CodDoc = p_CodDoc
					and	dp.DtHrMov = p_DtHrMov
					and	p.CodDoc = dp.CodDoc
					and	p.CodParte = dp.CodParteReferDec
					and	pe.CodPess = p.CodPess
					);

				if v_DescrComplRetorno is null then

					v_DescrComplRetorno := PARTEPROCESSOS_AUTOR(p_CodDoc);

					if v_DescrComplRetorno is null then

						select	min(pe.Nome)
						into	v_DescrComplRetorno
						from 	Pessoa pe, Parte p
						where	p.CodDoc = p_CodDoc
						and	p.NumOrd = (	select  min(p2.NumOrd)
  									from	Parte p2, ParteProcesso pp, tipoparte tp
 									where 	p2.CodDoc = p_CodDoc
   									and 	p2.CodParte = pp.CodParte
   									and	pp.codtipparte = tp.codtipparte
   									and	tp.codtippolo  in (2,4))
						and 	pe.CodPess = p.CodPess;
					end if;
				end if;

			elsif v_CodMovUltimoNivel in (352,353) then
			--Fase: 3 (Decisão) Compl1: 108 (Decretação de Prisão Criminal) Compl2: 352 (Temporária)  -- não tem caso em produção
			--Fase: 3 (Decisão) Compl1: 108 (Decretação de Prisão Criminal) Compl2: 353 (Preventiva)   -- ex em produção coddoc: 16382816
			--352	Decretada a prisão temporária de #{nome_da_parte}
			--353	Decretada a prisão preventiva de #{nome_da_parte}.


				--Por enquanto não tem como pegar essa informação.
			--	v_DescrComplRetorno := '';

				--RT 78475
				select	min(NomeDecRefParte)
				into	v_DescrComplRetorno
				from   (
					select	pe.Nome as NomeDecRefParte
					from 	Pessoa pe, Parte p, SessaoJulgDocumento sjd, MovJulg mj
					where	mj.CodDoc = p_CodDoc
					and	mj.DtHrMov = p_DtHrMov
					and	sjd.CodOrgJulg = mj.CodOrgJulg
					and	sjd.DtHrSess = mj.DtHrSess
					and	sjd.CodDec = mj.CodDec
					and	sjd.CodDoc = mj.CodDoc
					and	p.CodDoc = sjd.CodDoc
					and	p.CodParte = sjd.CodParteReferDec
					and	pe.CodPess = p.CodPess
					UNION
					select 	pe.Nome as NomeDecRefParte
					from 	Pessoa pe, Parte p, DespachoProcesso dp
					where	dp.CodDoc = p_CodDoc
					and	dp.DtHrMov = p_DtHrMov
					and	p.CodDoc = dp.CodDoc
					and	p.CodParte = dp.CodParteReferDec
					and	pe.CodPess = p.CodPess
					);


			elsif v_CodMovUltimoNivel in (354) then
			--Fase: 3 (Decisão) Compl1: 113	(Decretação de Prisão Civil) Compl2: 354 (Alimentos)
			--354	Decretada a prisão de devedor de alimentos a #{nome_da_parte}
			--Resposta: não enviar o complemento para o Datajud.  Cuidar para que o ejud exija o complemento como obrigatório para futuros registros.

				--Resposta: Sugiro que provisoriamente o nome da parte seja buscado no eventual 'mandado de prisão', enquanto não for criado campo próprio para preenchimento.
				--Os casos da base não tem mandado de prisão associado.
				--v_DescrComplRetorno := '';

				--RT 78475
				select	min(NomeDecRefParte)
				into	v_DescrComplRetorno
				from   (
					select	pe.Nome as NomeDecRefParte
					from 	Pessoa pe, Parte p, SessaoJulgDocumento sjd, MovJulg mj
					where	mj.CodDoc = p_CodDoc
					and	mj.DtHrMov = p_DtHrMov
					and	sjd.CodOrgJulg = mj.CodOrgJulg
					and	sjd.DtHrSess = mj.DtHrSess
					and	sjd.CodDec = mj.CodDec
					and	sjd.CodDoc = mj.CodDoc
					and	p.CodDoc = sjd.CodDoc
					and	p.CodParte = sjd.CodParteReferDec
					and	pe.CodPess = p.CodPess
					UNION
					select 	pe.Nome as NomeDecRefParte
					from 	Pessoa pe, Parte p, DespachoProcesso dp
					where	dp.CodDoc = p_CodDoc
					and	dp.DtHrMov = p_DtHrMov
					and	p.CodDoc = dp.CodDoc
					and	p.CodParte = dp.CodParteReferDec
					and	pe.CodPess = p.CodPess
					);

				if v_DescrComplRetorno is null then
					select	min(pe.Nome)
					into	v_DescrComplRetorno
					from	Pessoa pe, Parte pa --, TipoParte tp, ParteProcesso pp
					where	pa.CodDoc = p_CodDoc
					and	pa.NumOrd = (	select	min(Numord)
								from	Parte pa2, TipoParte tp, ParteProcesso pp
								where 	pa2.CodDoc = p_CodDoc
								and	tp.CodTipParte = pp.CodTipParte
								and	tp.CodTipPolo in (2,4)
								and	pa2.CodParte = pp.CodParte
								)
					and	pe.CodPess = pa.CodPess;
				end if;
			elsif v_CodMovUltimoNivel in (787) then
			--Fase: 3 - Decisão Compl1: 817 - Concessão Compl2: 787 - Assistência judiciária gratuita
       			--Fase do CNJ: 787 - Gratuidade da justiça
			--787	Concedida a gratuidade da justiça a #{nome_da_parte}.

				--Pegar o nome da parte que está na decisão
				select	min(NomeDecRefParte)
				into	v_DescrComplRetorno
				from   (
					select	pe.Nome as NomeDecRefParte
					from 	Pessoa pe, Parte p, SessaoJulgDocumento sjd, MovJulg mj
					where	mj.CodDoc = p_CodDoc
					and	mj.DtHrMov = p_DtHrMov
					and	sjd.CodOrgJulg = mj.CodOrgJulg
					and	sjd.DtHrSess = mj.DtHrSess
					and	sjd.CodDec = mj.CodDec
					and	sjd.CodDoc = mj.CodDoc
					and	p.CodDoc = sjd.CodDoc
					and	p.CodParte = sjd.CodParteReferDec
					and	pe.CodPess = p.CodPess
					UNION
					select 	pe.Nome as NomeDecRefParte
					from 	Pessoa pe, Parte p, DespachoProcesso dp
					where	dp.CodDoc = p_CodDoc
					and	dp.DtHrMov = p_DtHrMov
					and	p.CodDoc = dp.CodDoc
					and	p.CodParte = dp.CodParteReferDec
					and	pe.CodPess = p.CodPess
					);

				--se não foi informado a parte, pegar a parte do processo que tiver o check IndJustGrati = 'S'
				if v_DescrComplRetorno is null then
					select	min(pe.Nome)
					into	v_DescrComplRetorno
					from	Pessoa pe, Parte pa
					where	pa.CodDoc = p_CodDoc
					and	pa.NumOrd = (	select	min(Numord)
								from	Parte pa2, ParteProcesso pp
								where 	pa2.CodDoc = p_CodDoc
								and	pa2.CodParte = pp.CodParte
								and	pp.IndJustGrati = 'S'
								)
					and	pe.CodPess = pa.CodPess;

					--se não tiver marcação do check IndJustGrati, pegar o primeiro autor
					if v_DescrComplRetorno is null then
						select	min(pe.Nome)
						into	v_DescrComplRetorno
						from	Pessoa pe, Parte pa
						where	pa.CodDoc = p_CodDoc
						and	pa.NumOrd = (	select	min(Numord)
									from	Parte pa2, TipoParte tp, ParteProcesso pp
									where 	pa2.CodDoc = p_CodDoc
									and	tp.CodTipParte = pp.CodTipParte
									and	tp.CodTipPolo in (1)	 --***conferir se deveria ser o autor nesse caso de justiça gratuita
									and	pa2.CodParte = pp.CodParte
									)
						and	pe.CodPess = pa.CodPess;
					end if;
				end if;

			elsif v_CodMovUltimoNivel in (388, 389, 391, 393) then

			--Fase: 3 (Decisão) Compl1: 160 (Recebimento) Compl2: 388 (Aditamento da denúncia)
			--Fase: 3 (Decisão) Compl1: 160 (Recebimento) Compl2: 389 (Aditamento da queixa)
			--Fase: 3 (Decisão) Compl1: 160 (Recebimento) Compl2: 391 (Denúncia)
			--Fase: 3 (Decisão) Compl1: 160 (Recebimento) Compl2: 393 (Queixa)

			--388	Recebido aditamento à denúncia contra #{nome_da_parte}
			--389	Recebido aditamento à queixa contra #{nome_da_parte}
			--391	Recebida a denúncia contra #{nome_da_parte}
			--393	Recebida a queixa contra #{nome_da_parte}

			--Resposta:  Informar o nome do réu do processo do 2 grau (parte passiva), apenas na hipótese de existir um único réu.  Se forem mais de 1 réu (parte passiva),
			-- não enviar o complemento para o Datajud.  Cuidar para que o ejud exija o complemento como obrigatório para futuros registros.

			--Resposta: Para os casos com mais de 1 réu, seria possível provisoriamente concatenar o nome dos réus?
			--Caso contrário, pegar o primeiro. Entendo que seria menos prejudicial do que a ausência do envio.


				select 	count(1)
				into	v_CodAux1
				from	Parte pa, TipoParte tp, ParteProcesso pp
				where	pa.CodDoc = p_CodDoc
				and	tp.CodTipParte = pp.CodTipParte
				and	tp.CodTipPolo in (2,4)
				and	pa.CodParte = pp.CodParte;

				if v_CodAux1 = 1 then

					select	min(pe.Nome)
					into	v_DescrComplRetorno
					from	Pessoa pe, Parte pa, TipoParte tp, ParteProcesso pp
					where	pa.CodDoc = p_CodDoc
					and	tp.CodTipParte = pp.CodTipParte
					and	tp.CodTipPolo in (2,4)
					and	pa.CodParte = pp.CodParte
					and	pe.CodPess = pa.CodPess;
				else
					v_DescrComplRetorno := PARTEPROCESSOS_REU(p_CodDoc);
				end if;

		       	elsif v_CodMovUltimoNivel in (804) then
  			--Fase: 3 (Decisão) Compl1: 163 (Não-Recebimento) Compl2: 804 (Recurso)
			--804	Não recebido o recurso de #{nome_da_parte}.
			--Resposta:  Informar o autor ou autores dos recursos ou processos (partes ativas do processo do 2 grau).  São os autores do processo do 2 grau.

				v_DescrComplRetorno := PARTEPROCESSOS_AUTOR(p_CodDoc);


			elsif v_CodMovUltimoNivel in (11792) then
			--Fase: 3 (Decisão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 11792 (Livramento Condicional)
			--Fase: 60003 (3) (Decisão em Sessão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 11792 (Livramento Condicional)
			--11792	Suspenso o Livramento Condicional de #{nome_da_parte}

			--RESPOSTA: Indicador nome_da_parte. No ejud tem essa informação para enviar ao CNJ?  Se tiver favor enviar.  Se não tiver, enviar sem o complemento,
			--mas avaliar a criação de campo para o usuário informar no e-jud, a fim de sanarmos esse problema em futuros envios de informação ao CNJ.

			       v_DescrComplRetorno := '';

			elsif v_CodMovUltimoNivel in (818, 823, 11415,11554) then
    			--Fase: 3 (Decisão) Compl1: 817	(Concessão) Compl2: 818	(Liberdade provisória)
			--Fase: 60003 (3) (Decisão em Sessão) Compl1: 817 (Concessão) Compl2: 818 (Liberdade provisória)
			--818	Concedida a Liberdade provisória de #{nome_da_parte}.

			--Fase: 3 (Decisão) Compl1: 117 (Decretação de Internação) Compl2: 823 (Provisória)
			--Fase: 60003 (3) (Decisão em Sessão) Compl1: 117 (Decretação de Internação) Compl2: 823 (Provisória)
			--823	Decretada a Internação provisória de #{nome_da_parte}.

			--Fase: 60003 (Decisão em Sessão) Compl1: 817 (Concessão) Compl2: 11415	(Comutação de Pena)
			--11415	Concedida comutação de pena a #{nome_da_parte}

 			--Fase: 60003 (Decisão em Sessão) Compl1: 817 (Concessão) Compl2:  11554 (Indulto)
			--11554	Concedido o indulto a #{nome_da_parte}

				--Por enquanto não tem como pegar essa informação.
				--v_DescrComplRetorno := '';

				--Neste caso não estava buscando o nome da parte, podemos inserir a mesma regra do PROVIMENTO (237) que seria: colocar o nome da parte autora do recurso.
				--Se tiver mais de uma parte autora, colocar todas as partes autoras (polo ativo).
				--Porém foi levantado no PROVIMENTO (237) o problema de não ter o autor, então a regra que for aplicada lá, será aplicada aqui também.


				v_DescrComplRetorno := PARTEPROCESSOS_AUTOR(p_CodDoc);

				if v_DescrComplRetorno is null then

					select	min(pe.Nome)
					into	v_DescrComplRetorno
					from 	Pessoa pe, Parte p
					where	p.CodDoc = p_CodDoc
					and	p.NumOrd = (	select  min(p2.NumOrd)
  								from	Parte p2, ParteProcesso pp, tipoparte tp
 								where 	p2.CodDoc = p_CodDoc
   								and 	p2.CodParte = pp.CodParte
   								and	pp.codtipparte = tp.codtipparte
   								and	tp.codtippolo  in (2,4))
					and 	pe.CodPess = p.CodPess;
				end if;


			elsif v_CodMovUltimoNivel in (869) then
			--Fase: 869 (Entrega definitiva dos autos)
			--869	Entrega Definitiva dos Autos a #{nome_da_parte}
			--RESPOSTA: Identificador nome_da_parte.  Colocar o nome pra quem foi entregue os autos

				 v_DescrComplRetorno := ''; -- Por enquanto não tem registro dessa fase na tabela Movimento em Produção.

			elsif v_CodMovUltimoNivel in (1051) then
			--Fase: 1051 (Decurso de Prazo)
			--1051	Decorrido prazo de #{nome_da_parte} em #{data}.
			--RESPOSTA: Identificador nome_da_parte e data.   Colocar o nome da parte pra qual decorreu o prazo.  Se não tiver esse registro, deixar sem e criar para futuros registros.

				v_DescrComplRetorno := '';

			elsif v_CodMovUltimoNivel in (12456) then
			--Fase: 12456 (Recurso Ordinário)
			--12456	Não admitido o Recurso ordinário de #{nome_da_parte}.

				v_DescrComplRetorno := PARTEPROCESSOS_AUTOR(p_CodDoc);

				if v_DescrComplRetorno is null then

					select	min(pe.Nome)
					into	v_DescrComplRetorno
					from 	Pessoa pe, Parte p
					where	p.CodDoc = p_CodDoc
					and	p.NumOrd = (	select  min(p2.NumOrd)
  								from	Parte p2, ParteProcesso pp, tipoparte tp
 								where 	p2.CodDoc = p_CodDoc
   								and 	p2.CodParte = pp.CodParte
   								and	pp.codtipparte = tp.codtipparte
   								and	tp.codtippolo  in (2,4))
					and 	pe.CodPess = p.CodPess;
				end if;

			elsif v_CodMovUltimoNivel in (450,451,452) then
          		--Fase: 193 (Julgamento) 385 (Com Resolução do Mérito) 214 (Concessão em Parte) 450 (Segurança)
          		--Fase: 193 (Julgamento) 385 (Com Resolução do Mérito) 214 (Concessão em Parte) 451 (Habeas corpus)
          		--Fase: 193 (Julgamento) 385 (Com Resolução do Mérito) 214 (Concessão em Parte) 452 (Habeas data)
          		--Fase: 50193 (Julgamento Monocrático) 385 (Com Resolução do Mérito) 214 (Concessão em Parte) 450 (Segurança)
          		--Fase: 50193 (Julgamento Monocrático) 385 (Com Resolução do Mérito) 214 (Concessão em Parte) 451 (Habeas corpus)
        		--Fase: 50193 (Julgamento Monocrático) 385 (Com Resolução do Mérito) 214 (Concessão em Parte) 452 (Habeas data)
			--450	Concedida em parte a Segurança a #{nome_da_parte}.
			--451	Concedido em parte o Habeas Corpus a #{nome_da_parte}
			--452	Concedido em parte o Habeas Data a #{nome_da_parte}

				v_DescrComplRetorno := '';
			end if;

			if v_DescrComplRetorno is not null then
				v_DescrComplRetorno := '1:nome_da_parte:'||v_DescrComplRetorno;
			end if;

		--Se for 7 destino
		elsif ((v_auxIndicador1 = '7') or (v_auxIndicador2 = '7')) then

			--123	Remetidos os Autos (#{motivo_da_remessa}) para #{destino}
		       	if v_CodMovUltimoNivel = 123 then

				--Buscar o destino
				select	min(Descr)
				into	v_DescrIndicador
				from	Complemento c, Movimento m
				where	m.CodDoc = p_CodDoc
				and	m.DtHrMov = p_DtHrMov
				and	c.CodCompl = m.CodCompl1; --Destino

				if v_DescrComplRetorno is not null then
					v_DescrComplRetorno := v_DescrComplRetorno ||';7:destino:'|| v_DescrIndicador;
				else
					v_DescrComplRetorno := '7:destino:'|| v_DescrIndicador;

				end if;
			end if;

		--Se for 8 numero_do_processo
		elsif (v_auxIndicador1 = '8') or (v_auxIndicador2 = '8') then

			--135	Apensado ao processo #{numero_do_processo}
			--137	Desapensado do processo #{numero_do_processo}

			--Busca o Número do processo apenso
			select	min(mj.NumProc)
			into	v_DescrIndicador
			from	MovJunta mj
			where	mj.CodDoc = p_CodDoc
			and	mj.DtHrMov = p_DtHrMov;

			v_DescrComplRetorno := '8:numero_do_processo:'|| v_DescrIndicador;

		--Se for 10 tipo_da_decisao_anterior
		elsif (v_auxIndicador1 = '10') or (v_auxIndicador2 = '10') then

			--Fase: 60003 (3) (Decisão em Sessão) Compl1: 190 (Reforma de decisão anterior)
			--Fase: 3 (Decisão) Compl1: 190 (Reforma de decisão anterior)
			--Fase: 60003 (Decisão em Sessão) Compl1: 157 (Revogação) Compl2: 945 (Decisão anterior)
			--Fase: 3 (Decisão) Compl1: 157 (Revogação) Compl2: 945 (Decisão anterior)

			--190	Reformada decisão anterior datada de #{data}
			--945	Revogada decisão anterior datada de #{data}

			--DÚVIDA: Deve pegar a data aonde? SErá que deve pegar a decisão anterior? Por exemplo: 0019178-37.2017.8.19.0000
			--O que fazer nos casos que não tem uma decisão anterior ex: 0052418-34.2016.8.19.0038
			--(??? Confirmar com o João Paulo)


			--Buscar a data da última decisão
			select	min(DtHrMov), min(fe.CodFaseExt)
			into	v_Data, v_CodFaseExt
			from	FaseExterna fe, Movimento m
			where	m.CodDoc = p_CodDoc
			and	m.DtHrMov= ( 	select  max(m2.DtHrMov)
						from	Movimento m2, FaseExterna fe2
						where	m2.CodDoc = p_CodDoc
						and	m2.DtHrMov < p_DtHrMov
						and	m2.CodFase = fe2.CodFase
						and	fe2.CodFaseExt in (60003, 3))
			and	fe.CodFase = m.CodFase;

			--caso não encontre a Decisão, ver se a data foi informada na Sessão de Julgamento
			if v_Data is null then
				--Buscar a data na Sessão de Julgamento
				select	min(DtDecRefor)
				into	v_Data
				from	(
					select	sjd.DtDecRefor as DtDecRefor
					from	SessaoJulgDocumento sjd, MovJulg mj
					where	mj.CodDoc = p_CodDoc
					and	mj.DtHrMov = p_DtHrMov
					and	sjd.CodDoc = mj.CodDoc
					and	sjd.CodOrgJulg = mj.CodOrgJulg
					and	sjd.DtHrSess = mj.DtHrSess
					UNION
					select 	dp.DtDecRefor as DtDecRefor
					from 	DespachoProcesso dp
					where	dp.CodDoc = p_CodDoc
					and	dp.DtHrMov = p_DtHrMov
					);

				v_CodFaseExt := 0; --Não se sabe a fase, só foi informa a data
			end if;

			--se encontrou uma decisão anterior, mandar os dados
			if v_Data is not null then

				if v_CodFaseExt = 60003 then
 					v_Tipo := 'Decisão em Sessão';
				else
					v_Tipo := 'Decisão Monocrática';
				end if;

				v_DescrComplRetorno := '10:tipo_da_decisao_anterior:'||v_Tipo||';14:data:' || to_char(v_Data,'dd/mm/yyyy');
			else
				v_DescrComplRetorno := '';
			end if;

		--Se for 13 destinatario (Deve ser consignado o nome do destinatário dos autos, e não o Órgão.)
		elsif (v_auxIndicador1 = '13') or (v_auxIndicador2 = '13') then
			--Fase 493 (Entrega em carga/vista)
			-- 493	Autos entregues em carga ao #{destinatario}.

			--**DÚVIDA: Talvez não possa ser o complemento 1, pois não é para mandar o Órgão
			--RESPOSTA: Identificador destinatario (Compl1 - destinatário).  Informar no indicador quem pegou o processo em carga

			--493	Autos entregues em carga ao #{destinatario}.
			if v_CodMovUltimoNivel = 493 then

				select	min(Descr)
				into 	v_DescrIndicador
				from	Complemento c, Movimento m
				where	m.CodDoc = p_CodDoc
				and	m.DtHrMov = p_DtHrMov
				and	c.CodCompl = m.CodCompl1;

				v_DescrComplRetorno := '13:destinatario:'||v_DescrIndicador;

			else
				v_DescrComplRetorno :=	'';
			end if;

 		--Se for 20 nome_do_juizo
		elsif (v_auxIndicador1 = '20') or (v_auxIndicador2 = '20') then
			--Fase: 50193 (193) (Julgamento Monocrático) Compl1: 385 (Com Resolução do Mérito) Compl2: 900 (11796) (Declaração de competência em conflito)
			--Fase: 193 (Julgamento) Compl1: 385 (Com Resolução do Mérito) Compl2: 900 (11796) (Declaração de competência em conflito)
			--11796	Declarado competetente o #{nome_do_juizo}

			--RESPOSTA: Identificado nome_do_juizo.  Enviar sem identificador para o passado.  Para o futuro criar o identificador no e-jud

		       	--v_DescrComplRetorno := '';

			--RT 78475 - REQ2022.0100237
			if v_CodMovUltimoNivel = 11796 then

				select	min(NomeJuizoCompet)
				into	v_DescrAux1
				from   (
					select	sjd.NomeJuizoCompet as NomeJuizoCompet
					from 	SessaoJulgDocumento sjd, MovJulg mj
					where	mj.CodDoc = p_CodDoc
					and	mj.DtHrMov = p_DtHrMov
					and	sjd.CodOrgJulg = mj.CodOrgJulg
					and	sjd.DtHrSess = mj.DtHrSess
					and	sjd.CodDec = mj.CodDec
					and	sjd.CodDoc = mj.CodDoc
					UNION
					select 	gp.NomeJuizoCompet as NomeJuizoCompet
					from 	DespachoProcesso gp
					where	gp.CodDoc = p_CodDoc
					and	gp.DtHrMov = p_DtHrMov
					);


				if v_DescrAux1 is not null then
					v_DescrComplRetorno := '20:nome_do_juizo:'||Upper(v_DescrAux1);
				else
					v_DescrComplRetorno :=	'';
				end if;
			end if;

		--Se for 26 classe_anterior
		--Se for 27 classe_nova
		elsif (v_auxIndicador1 = '26') or (v_auxIndicador2 = '26') or
		      (v_auxIndicador1 = '27') or (v_auxIndicador2 = '27') then

			--10966	Classe Processual alterada de #{classe_anterior} para #{classe_nova}
			select  min(tp2.Descr), min(tp.Descr)
			into	v_ClassAnt, v_ClasseNova
			from	TipoProcesso tp2, TipoProcesso tp, MovMudClass m
			where 	m.CodDoc = p_CodDoc
			and	m.DtHrmov = p_DtHrMov
			and	tp.CodTipProc = m.CodTipProcNovo
			and	tp2.CodTipProc = m.CodTipProcAntig;

			v_DescrComplRetorno := '26:classe_anterior:'|| to_char(v_ClassAnt) || ';27:classe_nova:' || to_char(v_ClasseNova);

		--Se for 29 numero_tema_controversia_STF
		-- (Corresponde ao número do tema ou controvérsia, no ambito do STF, que justifica a suspensão do processo onde foi lançado o movimento.
		-- Corresponde às tabelas de temas e controversias fornecidas pelo STF.)
		elsif (v_auxIndicador1 = '29') or (v_auxIndicador2 = '29') then

			--***O CNJ ALTEROU O COMPLEMENTO DESSA FASE 265, AGORA O COMPLEMENTO É '50' numero_tema_RG
			--Fase: 3 (Decisão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 265 (Recurso Extraordinário com repercussão geral)
			--Fase: 60003 (3) (Decisão em Sessão) Compl1: 25 (Suspensão ou Sobrestamento) 265 (Recurso Extraordinário com repercussão geral)

			--Indicador de complemento
			--265	Processo Suspenso por Recurso Extraordinário com repercussão geral (#{tipo_tema_controversia} #{numero_tema_controversia_STF})

			--Tabelado	28	tipo_tema_controversia
						--105	Controvérsia
						--106	Tema

			--RESPOSTA: Identificador numero_tema_controversia_STF e tabelado tipo_tema_controversia.  Inserir a informação se tiver no e-JUD

			--(??? confirmar se tem como pegar, parece que não)
			--OBSERVAÇÃO: Tem registro na movimento, mas esses movimentos não tem o coddoc na tabela Repercussão

			--Verifica se tem repercussao para o processo
			select 	decode( min(CodTipReper), 2, '106', '105'), min(NumReper)
			into	v_DescrAux1, v_DescrAux2
			from	Repercussao r
			where	r.CodDoc = p_CodDoc
			and	r.CodTrib = 91; --(codpess = 22 - Supremo Tribunal Federal)


			if v_DescrAux2 is null then

				select	decode( min(CodTipReper), 2, '106', '105'), FormataRepercussao(Null, min(r.CodTipReper), min(r.NumReper), min(r.IndNumReperProv), null)   --NumReper
				into	v_DescrAux1, v_DescrAux2
				from	MovSobrestamento ms, Repercussao r
				where	ms.CodDoc = p_CodDoc
				and	ms.DtHrMov < p_DtHrMov
				and     r.CodReper = ms.CodReper
			--	and     r.IndAtivo = 'S'
			--	and     ms.CodSitSobre not in (select CodSitSobrePara from ConfigRepercussao)
				and	r.CodTrib = 91; --(codpess = 22 - Supremo Tribunal Federal)

			end if;

			if v_DescrAux2 is null then

				v_DescrIndicador := '';
			else
				--Já acrescenta o tabelado 28 tipo_tema_controversia
				v_DescrIndicador := '28:tipo_tema_controversia:'||v_DescrAux1||';29:tipo_tema_controversia:'||v_DescrAux2;
			end if;


/* agora será 12065 que não precisa de complemento
			--Fase: 50040 (265) Repercussão Geral/Repetitivos
			if p_CodFaseExt = 50040 then

				--DÚVIDA: Verificar se não for tema (2), considerar como Controvérsia?
				select	decode( CodTipReper, 2, '106', '105'), FormataRepercussao(Null, r.CodTipReper, r.NumReper, r.IndNumReperProv, null)   --NumReper
				into	v_DescrAux1, v_DescrAux2
				from	MovSobrestamento ms, Repercussao r
				where	ms.CodDoc = p_CodDoc
				and	ms.DtHrMov = p_DtHrMov
				and     ms.CodReper = r.CodReper
				and     r.IndAtivo = 'S'
				and     ms.CodSitSobre not in (select CodSitSobrePara from ConfigRepercussao)
				and	r.CodTrib = 91; --(codpess = 22 - Supremo Tribunal Federal)

			       	v_DescrIndicador := '28:tipo_tema_controversia:'|| v_DescrAux1 || ';29:numero_tema_controversia_STF:' || v_DescrAux2;
 */
			--DÚVIDA: Como pegar a repercussão geral das fases abaixo? Pois a tabela MovJulg e MovDespa não tem associação com repercussão geral
			--Fase: 3 (Decisão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 265 (Recurso Extraordinário com repercussão geral)
			--Fase: 60003 (3) (Decisão em Sessão) Compl1: 25 (Suspensão ou Sobrestamento) 265 (Recurso Extraordinário com repercussão geral)
--			else

			/*	select
				into	v_DescrAux1, v_DescrAux2
				from	MovJulg mj
				where	mj.CodDoc = p_CodDoc
				and	mj.DtHrMov = p_DtHrMov
			  */


--			end if;

		--Se for 30 numero_tema_controversia_tribunal_superior (Corresponde ao número do tema ou controvérsia, no ambito do Tribunal (STJ, TSE ou TST), que justifica a suspensão do processo. Os valores correspondem às tabelas de temas e controversias fornecidas pelos referidos Tribunais, quando houver.)
		elsif (v_auxIndicador1 = '30') or (v_auxIndicador2 = '30') then

			--tribunal: Identifica o tribunal onde tramita(m) o(s) recurso(s) repetitivo(s) vinculados ao Tema ou Controvérsia.
			--tipo_tema_controversia: Corresponde à identificação de se é um tema ou controvérsia que justifica a suspensão o processo. Os valores possíveis são "Tema" ou "Controvérsia".
			--numero_tema_controversia: corresponde ao númeo do tema ou controvérsia, no âmbito do Tribunal (STJ, TSE ou TST), que justifica a suspensã do processo.  Os valores correspondem às tabelas de temas e controvérsias fornecidas pelos referidos Tribunais, quando houver.

			--Fase: 3 (Decisão) Conpl1: 25 (Suspensão ou Sobrestamento) Compl2: 11555 (11975) (Recurso Especial repetitivo)
			--Fase 60003 (3) (Decisão em Sessão) Compl1: 25	(Suspensão ou Sobrestamento) Compl2: 11975 (Recurso Especial repetitivo)
			-- 11975 Processo suspenso por Recurso Especial Repetitivo (#{tribunal} - #{tipo_tema_controversia} #{numero_tema_controversia})

			--RESPOSTA: Identificador numero_tema_controversia_tribunal_superior e tabelado Tribunal e tipo_tema_controversia. No ejud tem essa informação para enviar ao CNJ?
			--Se tiver favor enviar.  Se não tiver, enviar sem o complemento, mas avaliar a criação de campo para o usuário informar no e-jud, a fim de sanarmos esse problema
			--em futuros envios de informação ao CNJ.

--2	tribunal	Tabelado
--					94	STJ	 --> **Só tem este cadastrado na tabela Tribunal com CodTrib = 92
--					95	TSE
---					93	TST

		       --(??? confirmar se tem como pegar, parece que não)
			--Verifica se tem repercussao para o processo
			select 	decode( min(CodTipReper), 2, '106', '105'), min(NumReper), decode(min(t.CodTrib), 92, 94, null)	--STJ
			into	v_DescrAux1, v_DescrAux2, v_DescrAux3
			from	Repercussao r, Tribunal t
			where	r.CodDoc = p_CodDoc
			and	t.CodTrib = r.CodTrib
			and	t.IndReper = 'S';


			if v_DescrAux2 is null then

				select	decode( min(CodTipReper), 2, '106', '105'), FormataRepercussao(Null, min(r.CodTipReper), min(r.NumReper), min(r.IndNumReperProv), null),   --NumReper
					decode(min(t.CodTrib), 92, 94, null)	--STJ
				into	v_DescrAux1, v_DescrAux2, v_DescrAux3
				from	MovSobrestamento ms, Repercussao r, Tribunal t
				where	ms.CodDoc = p_CodDoc
				and	ms.DtHrMov < p_DtHrMov
				and     r.CodReper = ms.CodReper
			--	and     r.IndAtivo = 'S'
			--	and     ms.CodSitSobre not in (select CodSitSobrePara from ConfigRepercussao)
				and	t.CodTrib = r.CodTrib
				and	t.IndReper = 'S';


			end if;

			if v_DescrAux2 is null then

				v_DescrIndicador := '';
			else
				--Já acrescenta o tabelado 28 tipo_tema_controversia
				v_DescrIndicador := '28:tipo_tema_controversia:'||v_DescrAux1||';22:tribunal:'||v_DescrAux1||';29:tipo_tema_controversia:'||v_DescrAux2;
			end if;


		--Se for 33 nome_do_incidente_repetitivo - Indicador Livre
		elsif (v_auxIndicador1 = '33') or (v_auxIndicador2 = '33') then
     			--Fase: 193 (Julgamento) Compl1: 385 (Com Resolução do Mérito) Compl2: 70142 (Definição de Tese Jurídica)
     			--12257	Definição de tese jurídica no incidente repetitivo #{nome_do_incidente_repetitivo}

			--**DÚVIDA: Como pegar o Incidente repetitivo?
			--(??? confirmarse tem como pegar, parece que não)
			v_DescrComplRetorno := '';


		--Se for 35 numero_tema_controversia_tribunal
		--(Corresponde ao número do tema ou controversia, no ambito do tribunal, que justifica a suspensão do processo onde foi lançado o movimento. Corresponde às tabelas de temas e controversias fornecidas pelo tribunal.)
		elsif (v_auxIndicador1 = '35') or (v_auxIndicador2 = '35') then
 			--Fase: 3 (Decisão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 12098 (Por Incidente de Resolução de Demandas Repetitivas)
 			--Fase: 60003 (3) (Decisão em Sessão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 12098 (Por Incidente de Resolução de Demandas Repetitivas)
			--12098	Processo Suspenso por Incidente de Resolução de Demandas Repetitivas (#{tipo_tema_controversia} #{numero_tema_controversia_tribunal})

			--DÚVIDA: Como pegar o numero_tema_controversia_tribunal
			--RESPOSTA: Identificador numero_tema_controversia_tribunal_superior e tabelado Tribunal e tipo_tema_controversia. No ejud tem essa informação para enviar ao CNJ?  Se tiver favor enviar.
			--Se não tiver, enviar sem o complemento, mas avaliar a criação de campo para o usuário informar no e-jud, a fim de sanarmos esse problema em futuros envios de informação ao CNJ.

			--Verifica se tem repercussao para o processo
			select  min(NumReper)
			into	v_DescrAux1
			from	Repercussao r, Tribunal t
			where	r.CodDoc = p_CodDoc
			and	t.CodTrib = r.CodTrib
			and	t.IndReper = 'S';


			if v_DescrAux1 is null then

				select	FormataRepercussao(Null, min(r.CodTipReper), min(r.NumReper), min(r.IndNumReperProv), null)   --NumReper
				into	v_DescrAux1
				from	MovSobrestamento ms, Repercussao r, Tribunal t
				where	ms.CodDoc = p_CodDoc
				and	ms.DtHrMov < p_DtHrMov
				and     r.CodReper = ms.CodReper
			--	and     r.IndAtivo = 'S'
			--	and     ms.CodSitSobre not in (select CodSitSobrePara from ConfigRepercussao)
				and	t.CodTrib = r.CodTrib
				and	t.IndReper = 'S';

			end if;

			if v_DescrAux1 is null then

				v_DescrIndicador := '';
			else
				--Já acrescenta o tabelado 28 tipo_tema_controversia
				v_DescrIndicador := '35:numero_tema_controversia_tribunal:'||v_DescrAux1;
			end if;


		--Se for 41 numero_da_SIRDR
		elsif (v_auxIndicador1 = '41') or (v_auxIndicador2 = '41') then
			--Fase: 3 (Decisão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 12099 (Por decisão do Presidente do STJ - IRDR)
			--Fase: 3 (Decisão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 12100 (Por decisão do Presidente do STF - IRDR)
			--Fase: 60003 (3) (Decisão em Sessão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 12099 (Por decisão do Presidente do STJ - IRDR)
			--Fase: 60003 (3) (Decisão em Sessão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 12100 (Por decisão do Presidente do STF - IRDR)

			--12100	Suspensão por Decisão do Presidente do STF em razão da SIRDR número #{numero_da_SIRDR}
			--12099	Suspensão por Decisão do Presidente do STJ em razão da SIRDR número #{numero_da_SIRDR}

			--DÚVIDA: como pegar o nÚmero da SIRDR?	   --Suspensão em Incidente de Resolução de Demandas Repetitivas (SIRDR)
			--RESPOSTA: Indicador numero_da_SIRDR. No ejud tem essa informação para enviar ao CNJ?  Se tiver favor enviar.  Se não tiver, enviar sem o complemento,
			--mas avaliar a criação de campo para o usuário informar no e-jud, a fim de sanarmos esse problema em futuros envios de informação ao CNJ.

			--(??? confirmar com o João Paulo se tem como pegar, parece que não)
			--OBSERVAÇÃO: Tem registro na movimento, mas esses movimentos não tem o coddoc na tabela Repercussão
			select 	min(NumReper)
			into	v_DescrAux1
			from	Repercussao r
			where	r.CodDoc = p_CodDoc
			and	r.CodTipReper = nval_const('$$TipReperSIRDR'); -- 6 SIRDR - Suspensão em Incidente de Resolução de Demanda Repetitiva

			if v_DescrAux1 is not null then
				v_DescrComplRetorno := '41:numero_da_SIRDR:'||v_DescrAux1;
			else
				v_DescrComplRetorno := '';
			end if;

		elsif (v_auxIndicador1 = '50') or (v_auxIndicador2 = '50') then

			--***O CNJ ALTEROU O COMPLEMENTO DESSA FASE 265, AGORA O COMPLEMENTO É '50' numero_tema_RG
			--Fase: 3 (Decisão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 265 (Recurso Extraordinário com repercussão geral)
			--Fase: 60003 (3) (Decisão em Sessão) Compl1: 25 (Suspensão ou Sobrestamento) 265 (Recurso Extraordinário com repercussão geral)

			--Indicador de complemento
			--265	Processo Suspenso por Recurso Extraordinário com repercussão geral #{numero_tema_RG}

			--Verifica se tem repercussao para o processo
			select 	min(NumReper)
			into	v_DescrAux1
			from	Repercussao r
			where	r.CodDoc = p_CodDoc
			and	r.CodTrib = 91; --(codpess = 22 - Supremo Tribunal Federal)


			if v_DescrAux1 is null then

				select	FormataRepercussao(Null, min(r.CodTipReper), min(r.NumReper), min(r.IndNumReperProv), null)   --NumReper
				into	v_DescrAux1
				from	MovSobrestamento ms, Repercussao r
				where	ms.CodDoc = p_CodDoc
				and	ms.DtHrMov < p_DtHrMov
				and	ms.DtHrMov = (select max(DtHrMov) from MovSobrestamento where CodDoc = p_CodDoc and DtHrMov < p_DtHrMov )
				and     r.CodReper = ms.CodReper
			--	and     r.IndAtivo = 'S'
			--	and     ms.CodSitSobre not in (select CodSitSobrePara from ConfigRepercussao)
				and	r.CodTrib = 91; --(codpess = 22 - Supremo Tribunal Federal)

			end if;

			v_DescrIndicador := '50:numero_tema_RG:'||v_DescrAux1;

		--Se for 52 numero_tema_IRDR
		elsif (v_auxIndicador1 = '52') or (v_auxIndicador2 = '52') then
 			--Fase: 3 (Decisão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 12098 (Por Incidente de Resolução de Demandas Repetitivas)
 			--Fase: 60003 (3) (Decisão em Sessão) Compl1: 25 (Suspensão ou Sobrestamento) Compl2: 12098 (Por Incidente de Resolução de Demandas Repetitivas)
			--12098	Processo Suspenso por Incidente de Resolução de Demandas Repetitivas (#{tipo_tema_controversia} #{numero_tema_controversia_tribunal})

			--Verifica se tem repercussao para o processo
			select  min(NumReper)
			into	v_DescrAux1
			from	Repercussao r, Tribunal t
			where	r.CodDoc = p_CodDoc
			and	t.CodTrib = r.CodTrib
			and	t.IndReper = 'S';


			if v_DescrAux1 is null then

				select	FormataRepercussao(Null, min(r.CodTipReper), min(r.NumReper), min(r.IndNumReperProv), null)   --NumReper
				into	v_DescrAux1
				from	MovSobrestamento ms, Repercussao r, Tribunal t
				where	ms.CodDoc = p_CodDoc
				and	ms.DtHrMov < p_DtHrMov
				and     r.CodReper = ms.CodReper
				and	ms.DtHrMov = (select max(DtHrMov) from MovSobrestamento where CodDoc = p_CodDoc and DtHrMov < p_DtHrMov	)
			--	and     r.IndAtivo = 'S'
			--	and     ms.CodSitSobre not in (select CodSitSobrePara from ConfigRepercussao)
				and	t.CodTrib = r.CodTrib
				and	t.IndReper = 'S';

			end if;

			if v_DescrAux1 is null then

				v_DescrIndicador := '';
			else
				--Já acrescenta o tabelado 28 tipo_tema_controversia
				v_DescrIndicador := '52:numero_tema_IRDR:'||v_DescrAux1;
			end if;

		end if;

		--Se for somente 14 data
		if (v_auxIndicador1 = '14') and (v_auxIndicador2 is null) then

			--848	Transitado em Julgado em #{data}
			if v_CodMovUltimoNivel = 848 then

				--**DÚVIDA: Se a data do Transito em Julgado não estiver preenchido pode mandar o DtHrMov?
				--RESPOSTA: Identificador Data.  Informar a data do transito em julgado
				--(??? apenas confirmar se podemos passar o dthrmov caso a data do transito em julgado esteja nulo)
				--Resposta: sim , se estiver nula , colocar a data do movimento.  Verificar para que essa fase tenha o complemento data como obrigatório daqui pra frente.

				select	nvl(DtTransJulg, DtHrMov)
				into	v_Data
				from	MovTransJulg
				where	CodDoc = p_CodDoc
				and	DtHrMov = p_DtHrMov;

				v_DescrComplRetorno := '14:data:' || to_char(v_Data,'dd/mm/yyyy');

			end if;

		end if;

		--Se for 12, 21 - Data_hora e Local
		if (v_auxIndicador1 = '12') and (v_auxIndicador2 = '21') then

			--970	Audiência #{tipo_de_audiencia} #{situacao_da_audiencia} conduzida por #{dirigida_por} em/para #{data_hora}, #{local}.
			--if v_CodMovUltimoNivel = 970 then
			if v_CodMovUltimoNivel in (12739,15050, 15049,15045, 12624, 14096, 12741, 12740, 12742, 12749, 12750,
			  			   12743, 12751, 12744, 12752, 12745, 12746, 12747, 12753) then

				v_DescrComplRetorno := v_DescrComplRetorno||'12:data:'|| to_char(p_DtHrMov,'dd/mm/yyyy hh24:mi')||';21:Local:'||PROCESSOS_LOCAL(p_CodDoc);
			elsif v_CodMovUltimoNivel = 970 then
				v_DescrComplRetorno := '12:data:'|| to_char(p_DtHrMov,'dd/mm/yyyy hh24:mi')||';21:Local:'||PROCESSOS_LOCAL(p_CodDoc);
			end if;

			-- 311 - Leilão só tem um movimento, porém de 2012, portanto não será tratado.
		end if;

		--Se for ato_publicado e data
		if (v_auxIndicador1 = '5') and (v_auxIndicador2 = '14') then

			--92	Publicado #{ato_publicado} em #{data}.
			--928	Republicado #{ato_publicado} em #{data}.
			if v_CodMovUltimoNivel in (92, 928 ) then

				--Tem identificador Ato_publicado (definir pelo compl1) e data.  A data, pega da data de publicação do sistema.
				-- O identificador do ato, como não foi tabelado pelo CNJ, colocar o texto do identificador do EJUD, quando existir.  Se não existir colocar "outras publicações".

				select	nvl(min(trim(c.Descr)),'Outras publicações'), min(DtHrMov)
				into	v_DescrAux1, v_Data
				from	Complemento c, Movimento mp --MovPubl mp  --alguns casos não tinha dados na MovPubl. ex: coddoc = 5716869
				where	mp.CodDoc = p_CodDoc
				and	mp.DtHrMov = p_DtHrMov
				and	c.CodCompl = mp.CodCompl1;

				v_DescrComplRetorno := '5:ato_publicado:'||v_DescrAux1||';14:data:'|| to_char(v_Data,'dd/mm/yyyy hh24:mi');

			else
				v_DescrComplRetorno := '';
			end if;

		end if;

	end if;

	--se for 14 - serventuário colocar 0, 1 é magistrado
	if v_TipoResp = 14 then
		v_TipoResp := 0;
	end if;

	select	count(1)
	into	v_Count
	from	DataJudUsuarioMovAutom d, Movimento m
	where	m.CodDoc = p_CodDoc
	and	m.DtHrMov = p_DtHrMov
	and	d.IdUsu = m.UsuIncl;

	if v_Count > 0 then
		v_TipoResp := 2; --Automático
	end if;

	if v_DescrComplRetorno is not null then
		Result := v_TipoResp ||';'||v_CodMovUltimoNivel||';'||v_DescrComplRetorno;
	elsif v_CodMovUltimoNivel is not null then
		Result := v_TipoResp ||';'||v_CodMovUltimoNivel;
	end if;

	RETURN Result;
END;