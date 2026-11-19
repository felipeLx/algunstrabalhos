create or replace FUNCTION BuscarOrgaoJulgadorMovimento (
--Criação: 06/05/2024 - Myrna Okamoto Ziwich
--Objetivo: Buscar o órgão julgador do processo no momento que foi feito o movimento (Datajud)

	p_CodDoc	Processo.CodDoc%TYPE,
	p_DtHrMov	Movimento.DtHrMov%TYPE,
	p_CodFaseExt	FaseExterna.CodFaseExt%TYPE := null
)

RETURN number
IS
	v_Count			number;
	v_CodFaseExt		FaseExterna.CodFaseExt%TYPE;
	v_CodOrgJulgCNJ		OrgaoJulgador.CodOrgJulgCNJ%TYPE;
	v_DtHrMovDistr		Movimento.DtHrMov%TYPE;
	v_CodCompt		Processo.CodCompt%TYPE;
begin

	if p_CodFaseExt is null then

	       select	min(CodFaseExt)
	       into	v_CodFaseExt
	       from	FaseExterna fc, Movimento m
	       where	m.CodDoc = p_CodDoc
	       and	m.DtHrMov = p_DtHrMov
	       and	fc.CodFase = m.CodFase;

	else
		v_CodFaseExt := p_CodFaseExt;
	end if;

	-- se for Cancelamento da Distribuicao
	if v_CodFaseExt = 488 then

		--busca o órgão da distribuição cancelada
		select 	min(oj.CodOrgJulgCNJ)
		into	v_CodOrgJulgCNJ
		from	OrgaoJulgador oj, CamaraCivelEspecializada ce, MovimentoRelator mr, MovCanceDistr mc
		where	mc.CodDoc = p_CodDoc
		and 	mc.DtHrMov = p_DtHrMov
		and 	mr.CodDoc = mc.CodDoc
		and	mr.DtHrMov = mc.DtHrMovDistrCance
		and	mr.CodOrgJulg = ce.CodOrgJulgCiv (+)
		and	nvl(ce.CodOrgJulgEspec, mr.CodOrgJulg) = oj.CodOrgJulg;

	else

		--Pegar a data da ultima distribuicao que seja menor que o movimento e que não esteja cancelado
	       	select	max(md.DtHrMov)
		into	v_DtHrMovDistr
		from	MovimentoRelator m, MovDistribuicao md
		where	md.CodDoc = p_CodDoc
		and	md.DtHrMov <= p_DtHrMov
		and	m.CodDoc = md.CodDoc
		and	m.DtHrMov = md.DtHrMov
		and	m.CodOrgJulg is not null
		and	not exists (	select 	1
					from	MovCanceDistr mc
					where	mc.CodDoc = md.CodDoc
					and	mc.DtHrMov < p_DtHrMov
					and	mc.DtHrMovDistrCance = md.DtHrMov);

		if v_DtHrMovDistr is not null then

			select	min(CodOrgJulgCNJ)
			into	v_CodOrgJulgCNJ
			from	OrgaoJulgador oj, MovimentoRelator mr, CamaraCivelEspecializada ce
			where	mr.CodDoc = p_CodDoc
			and	mr.DtHrMov = v_DtHrMovDistr
			and	mr.CodOrgJulg = ce.CodOrgJulgCiv (+)
			and	nvl(ce.CodOrgJulgEspec, mr.CodOrgJulg) = oj.CodOrgJulg;

		end if;

	end if;

	if  v_CodOrgJulgCNJ is null then

		--para os movimentos que foram feitos antes da primeira distribuição.
		select	min(CodOrgJulgCNJ)
		into	v_CodOrgJulgCNJ
		from	OrgaoJulgador oj, MovimentoRelator mr, CamaraCivelEspecializada ce
		where	mr.CodDoc = p_CodDoc
		and	mr.DtHrMov = (	select	min(md.DtHrMov)
					from	MovimentoRelator m, MovDistribuicao md
					where	md.CodDoc = p_CodDoc
					and	md.DtHrMov > p_DtHrMov
					and	m.CodDoc = md.CodDoc
					and	m.DtHrMov = md.DtHrMov
					and	m.CodOrgJulg is not null)
		and	mr.CodOrgJulg = ce.CodOrgJulgCiv (+)
		and	nvl(ce.CodOrgJulgEspec, mr.CodOrgJulg) = oj.CodOrgJulg;

	end if;

	if v_CodOrgJulgCNJ is null then

		--verifica se tem ou já teve alguma distribuição, se já teve, não pegar o órgão único pois a distribuição pode ter sido cancelada, e os movimentos posteriores não serão enviados
		select	count(1)
		into	v_count
		from	MovimentoRelator m, MovDistribuicao md
		where	md.CodDoc = p_CodDoc
		and	m.CodDoc = md.CodDoc
		and	m.DtHrMov = md.DtHrMov
		and	m.CodOrgJulg is not null;

		if v_count = 0 then
			--Verifica se a competência pertence à um órgão julgador.
			select	CodCompt
			into    v_CodCompt
			from	Documento d, t_Processo p
			where 	p.CodDoc = p_CodDoc
			and	d.CodDoc = p.CodDoc
			and	d.IndAtivo = 'S';

			select  count(1)
			into    v_count
			from    t_Processo p, OrgaoJulgador oj, Pessoa pe
			where   p.CodDoc = p_CodDoc
			and     p.CodCompt = oj.CodCompt
			and	pe.CodPess = oj.CodPess
			and	pe.IndAtivo = 'S';

			if v_count = 1 then

				select	oj.CodOrgJulgCNJ
				into 	v_CodOrgJulgCNJ
				from	OrgaoJulgador oj, Pessoa pe
				where 	oj.CodCompt = v_CodCompt
				and	pe.CodPess = oj.CodPess
				and	pe.IndAtivo = 'S';

			end if;
		end if;
	end if;

 	return v_CodOrgJulgCNJ;
end;