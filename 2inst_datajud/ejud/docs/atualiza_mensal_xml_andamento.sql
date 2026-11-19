create or replace PROCEDURE atualiza_mensal_xml_andamento
--Popular a tabela com os movimentos do mês anterior à execução do JOb ATUALIZAR_VM_XML_DADOS_CNJ
IS

	v_DataInicial	Date;
	v_DataFinal 	Date;
	v_Count		number;

begin

	v_DataFinal := trunc(sysdate, 'MM');
	v_DataInicial := add_months(v_DataFinal,-1);

	--exclui os registros que podem ter sido inseridos a mais pelo script de atualizacao diário.
	delete	tab_xml_andamento
	where	DtHrMov > v_DataInicial;
	commit;

	--só inserir movimentos com OJ informado
	insert into tab_xml_andamento (Coddoc, Cod_Proc, Dthrmov, Ident_Mov, Nivel_Sigilo, Tipo_Resp, Cod_Tip_Mov, Complemento, Descricao, Ind_Nacional, COD_OJ, DESCR_OJ, COD_CLASSE, Cod_Tip_Mov_Num)
	select	CodDoc, Cod_Proc, DtHrMov, Ident_Mov, Nivel_Sigilo, Tipo_Resp, Cod_Tip_Mov, DescrCompl, Descricao, Ind_Nacional, Cod_OJ,
		RETORNA_NOMEOJ_MNI(Cod_OJ) as Nomeorgao, Cod_Classe, to_number(nvl(Cod_Tip_Mov,'0'))
	from
	(
	select	CodDoc, Cod_Proc, DtHrMov, Ident_Mov, Nivel_Sigilo,
		SEPARAFASECOMPLEMENTOCNJ(x.Resultado, 'S','N','N') as Tipo_Resp,
		SEPARAFASECOMPLEMENTOCNJ(x.Resultado, 'N','S','N') as Cod_Tip_Mov,
		SEPARAFASECOMPLEMENTOCNJ(x.Resultado, 'N','N','S') as DescrCompl,
		Descricao,
		Ind_Nacional,
		BUSCARORGAOJULGADORMOVIMENTO(x.CodDoc, x.DtHrMov, Ident_Mov ) as Cod_OJ,
		BUSCARCLASSECNJMUDANCACLASSE(x.CodDoc, x.DtHrMov ) as Cod_Classe
	from	(
		select	m.CodDoc, pc.Cod_Proc, m.Dthrmov, fe.CodFaseExt as Ident_Mov,
			decode(p.IndSuperSigil, 'S', 5, decode(p.IndSigil,'S',4,decode(p.IndSegrJust,'S',1,0))) AS Nivel_Sigilo,
			BUSCARDADOS_FASECOMPL_CNJ(m.CodDoc, m.DtHrMov, fe.CodFaseExt, m.CodCompl1, m.CodCompl2, m.CodCompl3 ) as Resultado,
			Movimento_Descricao(p.CodDoc, m.DtHrMov, 'N', 'S') as Descricao,
			decode((	select 	min(m.Codfase)
					from 	dual
					where 	fe.CodFaseExt < 50000
					and	f.CodFase not in (4,7,10,23,25)), null, 'N', 'S') Ind_Nacional,
			tp.CodExtCNJ
		from    Movimento m, Fase f, faseexterna fe, Documento d, T_Processo p, pc_Processo pc, TipoProcesso tp
		where	m.DtHrMov  between v_DataInicial and v_DataFinal
		and	p.CodDoc = m.CodDoc
		and	d.CodDoc = p.CodDoc
		and	d.IndAtivo = 'S'
		and	m.Codfase = f.Codfase
		and	f.Codfase = fe.Codfase
		and 	fe.CodFaseExt not in (50999,60009,60010,60022)
		and	p.idproccorp = pc.id_proc
		and	fe.IndFaseInvis = 'N'
		and	tp.CodTipProc = p.CodTipProc
	 	) x
	where	x.Resultado is not null
	and	not exists (select 1 from tab_xml_andamento where CodDoc = x.CodDoc and DtHrMov = x.DtHrMov)
	) y
	where 	y.Cod_OJ is not null;


	commit;

        select 	count(1)
	into	v_count
	from 	tab_xml_andamento
	where	DtHrMov  between v_DataInicial and  v_DataFinal;

	INSERT INTO LogErro_tab_xml_andamento(Datahora, Qtde, Dataini, Datafim, Descr)
	VALUES(sysdate,v_count ,v_DataInicial, v_DataFinal, 'Atualização mensal pelo Job.');
	commit;



	--ajustar o nível de sigilo dos movimentos que se baseia no processo
	update	tab_xml_andamento
	set	Nivel_Sigilo = 5
	where	(CodDoc, DtHrMov) in (	select	t.CodDoc, t.DtHrMov
					from	tab_xml_andamento t, t_processo p
					where	p.IndSuperSigil = 'S'
					and	t.CodDoc = p.CodDoc
					and	t.Nivel_Sigilo <> 5);
	commit;

	update	tab_xml_andamento
	set	Nivel_Sigilo = 4
	where	(CodDoc, DtHrMov) in (	select	t.CodDoc, t.DtHrMov
					from	tab_xml_andamento t, t_processo p
					where	p.IndSuperSigil = 'N'
					and	p.IndSigil = 'S'
					and	t.CodDoc = p.CodDoc
					and	t.Nivel_Sigilo <> 4);
	commit;

	update	tab_xml_andamento
	set	Nivel_Sigilo = 1
	where	(CodDoc, DtHrMov) in (	select	t.CodDoc, t.DtHrMov
					from	tab_xml_andamento t, t_processo p
					where	p.IndSuperSigil = 'N'
					and	p.IndSigil = 'N'
					and	p.IndSegrJust = 'S'
					and	t.CodDoc = p.CodDoc
					and	t.Nivel_Sigilo <> 1);
	commit;

	update	tab_xml_andamento
	set	Nivel_Sigilo = 0
	where	(CodDoc, DtHrMov) in (	select	t.CodDoc, t.DtHrMov
					from	tab_xml_andamento t, t_processo p
					where	p.IndSuperSigil = 'N'
					and	p.IndSigil = 'N'
					and	p.IndSegrJust = 'N'
					and	t.CodDoc = p.CodDoc
					and	t.Nivel_Sigilo <> 0);
	commit;


end;