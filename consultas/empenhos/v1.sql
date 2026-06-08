SELECT 	
	datEmpenho.DATA as data_empenho,
	numEmpenho.cd_Numero as numero_empenho,
	CASE
        WHEN credor.tp_Pessoa  = 1 THEN credor.tp_Pessoa 
        WHEN credor.tp_Pessoa  = 2 THEN credor.tp_Pessoa 
    ELSE 4 end as tipo_credor,	
	credor.de_Nome as nome_credor,
	CASE
        WHEN credor.tp_Pessoa  = 1 THEN right(credor.cd_Credor,11)
    ELSE credor.cd_Credor end as cpf_cnpj_credor,
	'' as cpf_ordenador_despesa,	
	ente.cd_IBGE as codigo_ibge_municipio,	
	ug.CNPJ as cnpj_ug,
	ug.cd_Ugestora as codigo_ug, 
	ug.de_Ugestora as descricao_ug,
	orca.cd_Unidade_Orcamentaria as codigo_uo,
    orca.de_Unidade_Orcamentaria as descricao_uo,
	'PB' as sigla_uf,	
	esfera.sigla_Esfera as esfera,	
	CONCAT(funcao.cd_Funcao_str,subFuncao.cd_SubFuncao_str) as codigo_classificacao_funcional,
	CONCAT(CAT.cd_categoria_economica,natureza.cd_natureza_despesa,modalAplic.cd_modalidade_aplicacao,elemento.cd_Elemento) as codigo_classificacao_natureza,
	REPLACE(subElemento.cd_SubElemento_Str,'-1','') as codigo_subelemento_despesa,		
	subElemento.de_SubElemento as descricao_subelemento_despesa,
    fonte.cd_Fonte_Recursos_Sagres as codigo_fonte_recursos,
	null as codigo_co,
	REPLACE(programa.cd_Programa,'-1','') as codigo_programa,
	programa.de_Programa as descricao_programa,
    REPLACE(acao.cd_acao,'-1','') as codigo_acao,
	acao.de_acao as descricao_acao,
	LEFT(fato.historico,800) as historico,
    CAST(fato.vl_Empenho AS DECIMAL(18,2)) AS vl_empenhado,
    CAST(fato.vl_Pagamento AS DECIMAL(18,2)) AS vl_liquidado,
    CAST(fato.vl_Pagamento AS DECIMAL(18,2)) AS vl_pago
FROM dw.dbo.FATO_DESPESA_DIARIO fato
	LEFT JOIN dw.dbo.DIM_NUMERO_DESPESA_DIARIO numEmpenho on
		numEmpenho.Id_Numero = fato.Id_Numero
	LEFT JOIN dw.dbo.DIM_UNIDADE_GESTORA_SAGRES ug on
		ug.Id_Unidade_Gestora_Sagres = fato.Id_Unidade_Gestora_Sagres
	LEFT JOIN dw.dbo.DIM_UNIDADE_ORCAMENTARIA orca on
		orca.Id_Unidade_Orcamentaria = fato.Id_Unidade_Orcamentaria
	LEFT JOIN DW.DBO.DIM_FONTE_RECURSOS_SAGRES fonte on
		fonte.Id_Fonte_Recursos_Sagres = fato.Id_Fonte_Recursos
	LEFT JOIN dw.dbo.DIM_CREDOR credor on
		credor.id_Credor = fato.Id_Credor
	LEFT JOIN dw.dbo.DIM_ESFERA esfera on
		esfera.id_Esfera = fato.Id_Esfera
	LEFT JOIN dw.dbo.DIM_ENTE ente on
		ente.Id_Ente = fato.Id_Ente
	LEFT join dw.dbo.DIM_ELEMENTO elemento on
		elemento.id_Elemento = fato.Id_Elemento
	LEFT join dw.dbo.DIM_SUBELEMENTO subElemento on
		subElemento.Id_SubElemento = fato.Id_SubElemento
	LEFT join dw.dbo.DIM_DATA datEmpenho on
		datEmpenho.ID_DATA = fato.Id_DataEmissao
	LEFT join dw.dbo.DIM_FUNCAO funcao on
		funcao.Id_Funcao = fato.Id_Funcao
	LEFT join dw.dbo.DIM_SUBFUNCAO subFuncao on
		subFuncao.Id_SubFuncao = fato.Id_SubFuncao
	LEFT JOIN DW.DBO.DIM_NUMERO_LICITACAO numLicit on
		numLicit.Id_Nr_Licitacao =  fato.Id_Nr_Licitacao
	LEFT JOIN DW.DBO.DIM_MODALIDADE modalLicit on
		modalLicit.Id_Modalidade = fato.Id_Modalidade
	LEFT JOIN DW.DBO.DIM_CATEGORIA_ECONOMICA cat on
		cat.id_categoria_economica = fato.Id_Categoria_Economica
	LEFT JOIN DW.DBO.DIM_NATUREZA_DESPESA natureza on
		natureza.id_natureza_despesa = fato.Id_Natureza_Despesa
	LEFT JOIN dw.dbo.DIM_MODALIDADE_APLICACAO modalAplic on
		modalAplic.id_modalidade_aplicacao =  fato.Id_Modalidade_Aplicacao
	LEFT JOIN DW.DBO.DIM_PROGRAMA programa on
		programa.id_Programa =  fato.Id_Programa
	LEFT JOIN DW.DBO.DIM_ACAO acao on
		acao.id_Acao =  fato.Id_Acao
	LEFT JOIN dw.dbo.DIM_META meta on
		meta.id_Meta = fato.Id_Meta
WHERE 
	fato.ano_emissao= ? and fato.vl_Empenho>0