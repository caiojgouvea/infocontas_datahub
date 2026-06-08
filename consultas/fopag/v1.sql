SELECT 
	SER.cd_CPF as cpf,
	SER.de_Nome as nome_servidor,	
	mat.matricula as matricula,
CASE

	WHEN T.tipo_norm LIKE '%INSTITUIDOR%'
	  OR X.cargo_norm LIKE '%INSTITUIDOR%'
	THEN 'INSTITUIDOR DE PENSAO'

	WHEN T.tipo_norm LIKE '%PENSIONISTA%'
	  OR X.cargo_norm LIKE '%PENSIONISTA%'
	  OR X.cargo_norm LIKE '%PENSAO%'
	THEN 'PENSIONISTA'

	WHEN T.tipo_norm LIKE '%INATIVOS/PENSIONISTAS%'
	  OR T.tipo_norm LIKE '%INATIVO/PENSIONISTA%'
	THEN
		CASE
			WHEN X.cargo_norm LIKE '%PENSIONISTA%'
			  OR X.cargo_norm LIKE '%PENSAO%'
			  OR X.cargo_norm LIKE '%BENEFICIARIO%'
			THEN 'PENSIONISTA'

			WHEN X.cargo_norm LIKE '%APOSENTADO%'
			  OR X.cargo_norm LIKE '%APOSENTADA%'
			  OR X.cargo_norm LIKE '%INATIVO%'
			  OR X.cargo_norm LIKE '%REFORMADO%'
			  OR X.cargo_norm LIKE '%RESERVA%'
			THEN 'APOSENTADO'

			ELSE 'APOSENTADO'
		END

	WHEN T.tipo_norm LIKE '%APOSEN%'
	  OR T.tipo_norm LIKE '%INATIVO%'
	  OR T.tipo_norm LIKE '%REFORMADO%'
	  OR X.cargo_norm LIKE '%APOSENTADO%'
	  OR X.cargo_norm LIKE '%APOSENTADA%'
	  OR X.cargo_norm LIKE '%INATIVO%'
	  OR X.cargo_norm LIKE '%REFORMADO%'
	  OR (ESF.de_Esfera = 'Estadual' AND X.cargo_norm LIKE '%RESERVA%')
	THEN 'APOSENTADO'

	WHEN T.tipo_norm LIKE '%ELETIVO%'
	  OR X.cargo_norm LIKE '%VEREADOR%'
	  OR X.cargo_norm LIKE '%PREFEITO%'
	  OR X.cargo_norm LIKE '%VICE-PREFEITO%'
	THEN 'ELETIVO'

	WHEN T.tipo_norm LIKE '%CLT%'
	  OR T.tipo_norm LIKE '%EMPREGO%'
	THEN 'CELETISTA'

	WHEN T.tipo_norm LIKE '%COMISSION%'
	  OR T.tipo_norm LIKE '%COMIS%'
	  OR T.tipo_norm LIKE '%CONFIANCA%'
	  OR X.cargo_norm LIKE '%COMISSIONADO%'
	THEN 'COMISSIONADO'

	WHEN T.tipo_norm LIKE '%CONTRATO%'
	  OR T.tipo_norm LIKE '%TEMPORARIO%'
	  OR T.tipo_norm LIKE '%PREST%'
	  OR T.tipo_norm LIKE '%Contratação por excepcional interesse público%'
	  OR T.tipo_norm LIKE '%EMERG%'
	THEN 'CONTRATADO'

	WHEN T.tipo_norm LIKE '%ESTAGI%'
	  OR X.cargo_norm LIKE '%ESTAGIARIO%'
	THEN 'ESTAGIARIO'

	WHEN T.tipo_norm LIKE '%EFETIVO%'
	  OR T.tipo_norm LIKE '%ESTAT%'
	THEN 'ESTATUTARIO'

	ELSE 'OUTRO'

END as tipo_vinculo,
	ESF.de_Esfera as esfera,
	'PB' as sigla_uf,
	ENTE.de_Ente as descricao_ente_politico,
	ENTE.cd_IBGE as codigo_ibge,
	poder.de_Poder as poder,

	UG.de_Ugestora as descricao_uj_provimento, 
	UG.cd_Ugestora as codigo_uj_provimento,
	UG.CNPJ as cnpj_uj_provimento,

	UG.de_Ugestora as descricao_uj_lotacao, 
	UG.cd_Ugestora as codigo_uj_lotacao,
	UG.CNPJ as cnpj_uj_lotacao,

	UO.de_Unidade_Orcamentaria as descricao_uj_pagamento, 
	UO.cd_Unidade_Orcamentaria as codigo_uj_pagamento,
	'' as cnpj_uj_pagamento,

	ORG_DISP.de_orgao as descricao_uj_cedido, 
	'' as codigo_uj_cedido,
	'' as cnpj_uj_cedido,	
	ISNULL(NULLIF(LTRIM(RTRIM(cargo.de_Cargo)), ''), 'NAO IDENTIFICADO') as descricao_cargo,
	cargo.codigo as codigo_cargo,
	cargo.cd_CBO as cbo_cargo,

CASE
    WHEN ESF.de_Esfera = 'Estadual'
         AND TIPO.de_TipoCargo COLLATE Latin1_General_CI_AI LIKE '%MILITAR%'
    THEN 'MILITAR'

    WHEN ESF.de_Esfera = 'Estadual'
         AND (
               X.cargo_norm LIKE '%POLICIAL MILITAR%'
            OR X.cargo_norm LIKE '%BOMBEIRO MILITAR%'
            OR X.cargo_norm LIKE '%SOLDADO%'
            OR X.cargo_norm LIKE '%SARGENTO%'
            OR X.cargo_norm LIKE '%SUBTENENTE%'
            OR X.cargo_norm LIKE '%SUB TENENTE%'
            OR X.cargo_norm LIKE '%SUB-TENENTE%'
            OR X.cargo_norm LIKE '%TENENTE%'
            OR X.cargo_norm LIKE '%CAPITAO%'
            OR X.cargo_norm LIKE '%CORONEL%'
         )
         AND X.cargo_norm NOT LIKE '%GUARDA CIVIL%'
         AND X.cargo_norm NOT LIKE '%BOMBEIRO CIVIL%'
         AND X.cargo_norm NOT LIKE '%JUNTA%'
    THEN 'MILITAR'

    WHEN ESF.de_Esfera = 'Estadual'
         AND (
               X.cargo_norm = 'MAJOR'
            OR X.cargo_norm = 'CABO'
            OR X.cargo_norm LIKE 'CABO DA RESERVA%'
            OR X.cargo_norm LIKE 'CABO PM%'
            OR X.cargo_norm LIKE 'CABO BM%'
         )
    THEN 'MILITAR'
    ELSE 'CIVIL'
END as regime,
	DAT_ADM.DATA as data_nomeacao,
	'' as data_posse,
	'' as data_exercicio,
	DAT_apos.DATA as data_aposentadoria,
	CASE
    WHEN esc.de_escolaridade IS NULL
      OR LTRIM(RTRIM(esc.de_escolaridade)) = ''
    THEN 'NAO IDENTIFICADO'

    WHEN UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI IN (
        'NAO INFORMADO', 'NAO APLICADO', 'DESCONHECIDO',
        'SEM INFORMACAO', 'SEM EXIGENCIA', 'OUTRO', 'OUTROS',
        'OR                      F'
    )
    THEN 'NAO IDENTIFICADO'

    WHEN UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI LIKE '%MESTRADO%'
    THEN 'MESTRADO'

    WHEN UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI LIKE '%ESPECIALIZACAO%'
    THEN 'ESPECIALIZACAO'

    WHEN UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI LIKE '%POS GRADUACAO%'
      OR UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI LIKE '%POS-GRADUACAO%'
      OR UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI LIKE '%PÓS GRADUAÇÃO%'
      OR UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI LIKE '%PÓS-GRADUAÇÃO%'
    THEN 'POS-GRADUACAO'

    WHEN UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI LIKE '%ENSINO MEDIO TECNICO%'
    THEN 'TECNICO'

    WHEN UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI IN (
        'SUPERIOR COMPLETO', 'SUPERIOR', '0SUPERIOR',
        'UPERIOR                      F'
    )
    THEN 'SUPERIOR COMPLETO'

    WHEN UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI = 'SUPERIOR INCOMPLETO'
    THEN 'SUPERIOR INCOMPLETO'

    WHEN UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI IN (
        'ENSINO MEDIO COMPLETO', 'ENSINO MÉDIO', 'MEDIO'
    )
    THEN 'MEDIO COMPLETO'

    WHEN UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI = 'ENSINO MEDIO INCOMPLETO'
    THEN 'MEDIO INCOMPLETO'

    WHEN UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI IN (
        'ENSINO FUNDAMENTAL COMPLETO',
        'ENSINO FUNDAMENTAL',
        'COM 5 SERIE COMPLETA ENS FUNDA',
        '999BASICO',
        'BASICO',
        'ALFABETIZACAO'
    )
    THEN 'FUNDAMENTAL COMPLETO'

    WHEN UPPER(esc.de_escolaridade) COLLATE Latin1_General_CI_AI IN (
        'ATE 5 SERIE INCOMPLETA ENS FUN',
        'DA 6 A 9 SERIE INCOMPLE ENS FU'
    )
    THEN 'FUNDAMENTAL INCOMPLETO'

    ELSE 'NAO IDENTIFICADO'
END AS escolaridade_cargo,

	fato.carga_horaria_semanal as jornada_semanal_trabalho,

	DAT_REF.ANO as ano_competencia,
	DAT_REF.MES as mes_competencia,
	DAT_REF.ANO as ano_pagamento,
	DAT_REF.MES as mes_pagamento,	

	'TESTE' as situacao,
	fato.vl_Vantagem as remuneracao_bruta,
	fato.vl_Desconto as total_descontos

FROM DW.DBO.FATO_FOLHA_2020 FATO 

LEFT JOIN DW.DBO.DIM_SERVIDOR SER ON
	SER.id_Servidor = FATO.id_Servidor

LEFT JOIN DW.DBO.DIM_DATA DAT_REF ON
	DAT_REF.ID_DATA = FATO.id_Data_Referencia

LEFT JOIN DW.DBO.DIM_DATA DAT_adm ON
	DAT_adm.ID_DATA = FATO.id_Data_Admissao

LEFT JOIN DW.DBO.DIM_DATA DAT_apos ON
	DAT_apos.ID_DATA = FATO.id_Data_Aposentadoria

LEFT JOIN DW.DBO.DIM_UNIDADE_GESTORA_SAGRES ug ON
	ug.Id_Unidade_Gestora_Sagres = FATO.id_Unidade_Gestora_Sagres

LEFT JOIN DW.DBO.DIM_UNIDADE_ORCAMENTARIA UO ON
	UO.Id_Unidade_Orcamentaria = FATO.id_Unidade_Orcamentaria

LEFT JOIN DW.DBO.DIM_TIPOCARGO TIPO ON
	TIPO.id_TipoCargo = FATO.id_TipoCargo

LEFT JOIN DW.DBO.DIM_ESFERA ESF ON
	ESF.id_Esfera = FATO.id_Esfera

LEFT JOIN DW.DBO.DIM_CARGO CARGO ON
	CARGO.id_Cargo = FATO.id_Cargo

LEFT JOIN DW.DBO.DIM_MATRICULA_2025 MAT ON
	MAT.id_Matricula_2025 = FATO.id_Matricula_2025

LEFT JOIN dw.dbo.DIM_ENTE ente ON
	ente.Id_Ente = fato.id_ente

LEFT JOIN dw.dbo.DIM_ENTIDADE entidade ON
	entidade.id_entidade = fato.id_entidade

LEFT JOIN dw.dbo.DIM_PODER poder ON
	poder.id_Poder = fato.id_Poder

LEFT JOIN dw.dbo.DIM_UNIDADE_TRABALHO unid_trab ON
	unid_trab.id_unidade = fato.id_unidade_trabalho

LEFT JOIN dw.dbo.DIM_ORGAO_A_DISPOSICAO ORG_DISP ON
	ORG_DISP.id_orgao = fato.id_orgao_a_disposicao

LEFT JOIN DW.DBO.DIM_ESCOLARIDADE esc ON
	esc.id_escolaridade = FATO.id_escolaridade

CROSS APPLY (
	SELECT UPPER(ISNULL(cargo.de_Cargo, '')) COLLATE Latin1_General_CI_AI AS cargo_norm
) X

CROSS APPLY (
	SELECT UPPER(ISNULL(TIPO.de_TipoCargo, '')) COLLATE Latin1_General_CI_AI AS tipo_norm
) T

WHERE 
	DAT_REF.ANO = ?
ORDER BY DAT_REF.MES
