create or replace PROCEDURE ATUALIZA_DIARIO_XML_ANDAMENTO
-- Atualizar a tabela tab_xml_andamento	com as alterações ocorridas no mês corrente referente aos movimentos do mês anterior

IS

	v_DataInicial	Date;
	v_DataFinal 	Date;
	--v_DataMov	Date;
	v_Count		number;

begin

	-- Se pode executar a atualização diariamente
	if  ( sval_const('$$PodeExecXMLAndamDia') = 'S') then

		v_Count := 0; --quantidade de processos (usado apenas no log)
		v_DataInicial := trunc(sysdate)-1;
		v_DataFinal := trunc(sysdate);

		--comentado para permitir inserir os movimentos do mês corrente (02/04/2025)
		--v_DataMov := trunc(sysdate, 'MM'); --será inserido os movimentos até essa data

		--passará a atualizar todos os movimentos alterados no dia anterior.
		for v_log in (	select	distinct lm.CodDoc
				from	t_Processo p, LogMovimento lm
				where	lm.DtHrAlt between v_DataInicial and v_DataFinal
				and	lm.DtHrMov < v_DataFinal
				and	p.CodDoc = lm.CodDoc
		     	)
		loop
			--exclui todos os movimentos do processo para reinserir
			delete	tab_xml_andamento
			where	CodDoc = v_log.CodDoc;

			insert into tab_xml_andamento (Coddoc, Cod_Proc, Dthrmov, Ident_Mov, Nivel_Sigilo, Tipo_Resp, Cod_Tip_Mov,
							Complemento, Descricao, Ind_Nacional, COD_OJ, DESCR_OJ, COD_CLASSE, Cod_Tip_Mov_Num)
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
			from (
				select	m.CodDoc, pc.Cod_Proc, m.Dthrmov, fe.CodFaseExt as Ident_Mov,
					decode(p.IndSuperSigil, 'S', 5, decode(p.IndSigil,'S',4,decode(p.IndSegrJust,'S',1,0))) AS Nivel_Sigilo,
					BUSCARDADOS_FASECOMPL_CNJ(m.CodDoc, m.DtHrMov, fe.CodFaseExt, m.CodCompl1, m.CodCompl2, m.CodCompl3 ) as Resultado,
					Movimento_Descricao(p.CodDoc, m.DtHrMov, 'N', 'S') as Descricao,
					decode((	select 	min(m.Codfase)
							from 	dual
							where 	fe.CodFaseExt < 50000
							and	f.CodFase not in (4,7,10,23,25)), null, 'N', 'S') Ind_Nacional, tp.CodExtCNJ
				from    Movimento m, Fase f, faseexterna fe, T_Processo p, pc_Processo pc, TipoProcesso tp
				where	m.CodDoc = v_log.CodDoc
			--	and	m.DtHrMov < v_DataMov	 --comentado para atualizar também o mês corrente
				and	p.CodDoc = m.CodDoc
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

			v_Count := v_Count + 1;
		end loop;
		commit;


		INSERT INTO LogErro_tab_xml_andamento(Datahora, Qtde, Dataini, Datafim, Descr)
		VALUES(sysdate,v_count ,v_DataInicial, v_DataFinal, 'Atualização diária.');
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


	end if;
end;