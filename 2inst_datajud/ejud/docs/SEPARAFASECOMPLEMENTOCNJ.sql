create or replace FUNCTION SEPARAFASECOMPLEMENTOCNJ
/*	Funcao:	Retorna Fase e Complementos  */
/*	Autor:	MOZ - 28.05.2021 */
(	p_Valor 		varchar2,
	p_RetornaTipoResp	char := 'N',
	p_RetornaFase		char := 'N',
	p_RetornaCompl		char := 'N'
)
RETURN
	VarChar2

IS
	v_pos			number;
	v_Valor			VarChar2(1000);
	v_Fase			Fase.CodFase%TYPE;
	v_DescrCompl		varchar2(1000);
	v_ParteValor		VarChar2(1000);
	v_TipoResp		char(1);

BEGIN
	v_TipoResp := '';
	v_Fase := '';
        v_DescrCompl := '';
	v_ParteValor := p_Valor;
 	v_pos := instr(v_ParteValor, ';');

 	if v_pos > 0 then
		v_TipoResp := substr(v_ParteValor,0,v_pos-1);
		v_ParteValor := substr(v_ParteValor, v_pos+1, length(v_ParteValor));
		v_pos := instr(v_ParteValor, ';');

		if v_pos > 0 then
         		v_Fase := substr(v_ParteValor,0,v_pos-1);
         		v_DescrCompl := substr(v_ParteValor, v_pos+1, length(v_ParteValor));
		else
			v_Fase := v_ParteValor;
		end if;
	end if;

	if nvl(p_RetornaTipoResp, 'N') = 'S' then
		return v_TipoResp;
	elsif nvl(p_RetornaFase, 'N') = 'S' then
		return v_Fase;
	elsif nvl(p_RetornaCompl, 'N') = 'S' then
		return v_DescrCompl;
	end if;

END;