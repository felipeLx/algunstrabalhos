create or replace function RETORNA_NOMEOJ_MNI
(
p_Codigo	number
)

RETURN VARCHAR2

IS

v_Result	VARCHAR2(4000);

BEGIN

	select	min(Nome)
	into	v_Result
	from	OrgaoJulgadorDescrCNJ
	where	CodOrgJulgCNJ = p_Codigo;

	if v_Result is null and p_Codigo is not null then
		select	min(OrgaoJulgador_nome(CodOrgJulg))
		into	v_Result
		from	OrgaoJulgador
		where	CodOrgJulgCNJ = p_Codigo;
	end if;

	return v_Result;

END;

create or replace FUNCTION ORGAOJULGADOR_NOME /*************************************************************************
* Objetivo    : Retorna nome da Orgao Julgador
* Criacao     : 18/09/2002
* Autor       : Joao Paulo Gabardo
* Observacoes : Utilizada nas Estatisticas
*************************************************************************/

(
p_CodOrgJulg	OrgaoJulgador.CodOrgJulg%TYPE
)

RETURN VARCHAR2

IS

v_Retorno	Pessoa.Nome%TYPE;

BEGIN

	select	min(p.Nome)
	into	v_Retorno
	from	Pessoa p, OrgaoJulgador v
	where	v.CodOrgJulg = p_CodOrgJulg
	and	v.CodPess = p.CodPess;

	if v_Retorno is null then
		v_Retorno := '<Órgão Julgador não Informado>';
	end if;

	return	NVL(v_Retorno, '');

END;




