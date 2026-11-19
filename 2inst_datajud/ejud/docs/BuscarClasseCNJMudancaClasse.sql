create or replace FUNCTION BuscarClasseCNJMudancaClasse (
--Buscar a classe do processo no momento que foi feito o movimento (Datajud)

	p_CodDoc	Processo.CodDoc%TYPE,
	p_DtHrMov	Movimento.DtHrMov%TYPE
)

RETURN number
IS
	v_Count		number;
	v_result	number;
begin

	--verifica se houve mudança de classe
	select	count(1)
	into	v_Count
	from	MovMudClass
	where	CodDoc = p_CodDoc;

	if v_Count > 0 then

		--se passou o próprio movimento de mudança de classe
		select	count(1)
		into	v_Count
		from	MovMudClass
		where	CodDoc = p_CodDoc
		and	DtHrMov = p_DtHrMov;

		if v_Count = 1 then

			select	min(tp.CodExtCNJ)
			into	v_Result
			from	TipoProcesso tp, MovMudClass mc
			where	mc.CodDoc = p_CodDoc
			and	mc.DtHrMov = p_DtHrMov
			and	tp.CodTipProc = mc.CodTipProcNovo;

		else

			--buscar a classe
			select	min(tp.CodExtCNJ)
			into	v_Result
			from	TipoProcesso tp, MovMudClass mc
			where	mc.CodDoc = p_CodDoc
			and	mc.DtHrMov = (select min(DtHrMov) from MovMudClass where CodDoc = p_CodDoc and DtHrMov > p_DtHrMov)
			and	tp.CodTipProc = mc.CodTipProcAntig;

		end if;
	end if;

	if v_Result is null then

		select	tp.CodExtCNJ
		into	v_Result
		from	TipoProcesso tp, t_Processo p
		where	p.CodDoc = p_CodDoc
		and	tp.CodTipProc = p.CodTipProc;
	end if;

 	return v_Result;
end;