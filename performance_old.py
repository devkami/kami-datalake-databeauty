import os
import logging
from pyathena import connect
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from pyathena.pandas.util import as_pandas
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date, timedelta
from streamlit_plotly_events import plotly_events
import numpy as np



# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Carregar variáveis de ambiente
load_dotenv()

# Configurações de conexão Athena
ATHENA_S3_STAGING_DIR = os.environ.get('ATHENA_S3_STAGING_DIR', 's3://databeautykamico/Athena/')
ATHENA_REGION = os.environ.get('ATHENA_REGION', 'us-east-1')

logging.info(f"Usando ATHENA_S3_STAGING_DIR: {ATHENA_S3_STAGING_DIR}")
logging.info(f"Usando ATHENA_REGION: {ATHENA_REGION}")

def query_athena(query):
    try:
        logging.info("Iniciando conexão com Athena")
        conn = connect(s3_staging_dir=ATHENA_S3_STAGING_DIR, region_name=ATHENA_REGION)
        cursor = conn.cursor()
        logging.info("Executando query")
        cursor.execute(query)
        logging.info("Convertendo resultado para DataFrame")
        df = as_pandas(cursor)
        logging.info(f"Query executada com sucesso. Retornando DataFrame com {len(df)} linhas.")
        return df
    except Exception as e:
        logging.error(f"Erro ao executar query no Athena: {str(e)}")
        st.error(f"Erro ao executar query no Athena: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_channels_and_ufs(cod_colaborador, start_date, end_date):
    query = f"""
    SELECT DISTINCT 
        pedidos.canal_venda,
        empresa_pedido.uf_empresa_faturamento
    FROM
        "databeautykami"."vw_distribuicao_pedidos" pedidos
    LEFT JOIN "databeautykami"."vw_distribuicao_empresa_pedido" AS empresa_pedido 
        ON pedidos."cod_pedido" = empresa_pedido."cod_pedido"
    WHERE
        date(pedidos."dt_faturamento") BETWEEN date('{start_date}') AND date('{end_date}')
        {'AND empresa_pedido.cod_colaborador_atual = ' + f"'{cod_colaborador}'" if cod_colaborador else ''}
    """
    df = query_athena(query)
    return df['canal_venda'].unique().tolist(), df['uf_empresa_faturamento'].unique().tolist()

@st.cache_data
def get_monthly_revenue_cached(cod_colaborador, start_date, end_date, selected_channels, selected_ufs, selected_brands=None):
    return get_monthly_revenue(cod_colaborador, start_date, end_date, selected_channels, selected_ufs, selected_brands)

@st.cache_data
def get_brand_data_cached(cod_colaborador, start_date, end_date, selected_channels, selected_ufs):
    return get_brand_data(cod_colaborador, start_date, end_date, selected_channels, selected_ufs)       

def get_monthly_revenue(cod_colaborador, start_date, end_date, selected_channels, selected_ufs, selected_brands=None):
    brand_filter = ""
    if selected_brands:
        brands_str = "', '".join(selected_brands)
        brand_filter = f"AND item_pedidos.marca IN ('{brands_str}')"
    
    channel_filter = ""
    if selected_channels:
        channels_str = "', '".join(selected_channels)
        channel_filter = f"AND pedidos.canal_venda IN ('{channels_str}')"
    
    uf_filter = ""
    if selected_ufs:
        ufs_str = "', '".join(selected_ufs)
        uf_filter = f"AND empresa_pedido.uf_empresa_faturamento IN ('{ufs_str}')"
    
    colaborador_filter = ""
    group_by_cols = "1, fator"
    group_by_cols_acum = "1"
    select_cols_subquery = ""
    select_cols_main = ""
    if cod_colaborador:
        colaborador_filter = f"AND empresa_pedido.cod_colaborador_atual = '{cod_colaborador}'"
        group_by_cols = "1, 2, 3, fator"
        group_by_cols_acum = "1, 2, 3"
        select_cols_subquery = """
        empresa_pedido.nome_colaborador_atual vendedor,
        empresa_pedido.cod_colaborador_atual cod_colaborador,
        """
        select_cols_main = """
        f.vendedor,
        f.cod_colaborador,
        """
    
    query = f"""
    WITH bonificacao AS (
        SELECT 
            mes_ref,
            {select_cols_subquery}
            ROUND(SUM(valor_bonificacao_ajustada),2) valor_bonificacao
    FROM (
            SELECT
                DATE_TRUNC('month', dt_faturamento) mes_ref,
                {select_cols_subquery}
                CASE WHEN fator IS NULL Then ROUND(SUM(item_pedidos.preco_total), 2)
                Else ROUND(SUM(item_pedidos.preco_total)/fator,2) END AS valor_bonificacao_ajustada
            FROM
                "databeautykami"."vw_distribuicao_pedidos" pedidos
            LEFT JOIN "databeautykami"."vw_distribuicao_item_pedidos" AS item_pedidos 
                ON pedidos."cod_pedido" = item_pedidos."cod_pedido"
            LEFT JOIN "databeautykami"."vw_distribuicao_empresa_pedido" AS empresa_pedido 
                ON pedidos."cod_pedido" = empresa_pedido."cod_pedido"
            LEFT JOIN "databeautykami".tbl_distribuicao_bonificacao bonificacao
                ON cast(bonificacao.cod_empresa as varchar) = empresa_pedido.cod_empresa_faturamento 
                and date(bonificacao.mes_ref) = DATE_TRUNC('month', dt_faturamento)
            LEFT JOIN "databeautykami".tbl_varejo_marca marca ON marca.cod_marca = bonificacao.cod_marca
                and upper(trim(marca.desc_abrev)) = upper(trim(item_pedidos.marca))
            WHERE
                upper(pedidos."desc_abrev_cfop") LIKE '%BONIFICA%'
                AND pedidos.operacoes_internas = 'N'
                {colaborador_filter}
                {channel_filter}
                {uf_filter}
                {brand_filter}
            GROUP BY {group_by_cols}
        )   boni 
    group by {group_by_cols_acum}
    )
    SELECT
        f.mes_ref,
        {select_cols_main}
        f.faturamento_bruto,
        f.faturamento_liquido,
        f.desconto,
        COALESCE(b.valor_bonificacao, 0) AS valor_bonificacao,
        f.custo_total,
        f.positivacao,
        f.qtd_pedido,
        f.qtd_itens,
        f.qtd_sku,
        f.qtd_marcas,
        f.Ticket_Medio_Positivacao,
        f.Ticket_Medio_Pedidos,
        CASE 
            WHEN f.custo_total > 0 THEN ((f.faturamento_liquido - f.custo_total) / f.custo_total) * 100 
            ELSE 0 
        END AS markup_percentual
    FROM (
    SELECT
        DATE_TRUNC('month', pedidos.dt_faturamento) mes_ref,
        {select_cols_subquery}
        ROUND(SUM(item_pedidos."preco_total"), 2) AS "faturamento_bruto",
        ROUND(SUM(item_pedidos."preco_desconto_rateado"), 2) AS "faturamento_liquido",
        ROUND(SUM(item_pedidos.preco_total) - SUM(item_pedidos.preco_desconto_rateado), 2) AS desconto,
        ROUND(SUM(COALESCE(cmv.custo_medio, 0) * item_pedidos.qtd), 2) AS custo_total,
        COUNT(DISTINCT pedidos.cpfcnpj) AS "positivacao",
        COUNT(DISTINCT pedidos.cod_pedido) AS "qtd_pedido",
        SUM(item_pedidos.qtd) AS qtd_itens,
        COUNT(DISTINCT item_pedidos.cod_produto) AS qtd_sku,
        COUNT(DISTINCT item_pedidos.marca) AS qtd_marcas,
        ROUND(SUM(item_pedidos."preco_desconto_rateado") / NULLIF(COUNT(DISTINCT pedidos.cpfcnpj), 0), 2) AS Ticket_Medio_Positivacao,
        ROUND(SUM(item_pedidos."preco_desconto_rateado") / NULLIF(COUNT(DISTINCT pedidos.cod_pedido), 0), 2) AS Ticket_Medio_Pedidos
    FROM
        "databeautykami"."vw_distribuicao_pedidos" pedidos
    LEFT JOIN "databeautykami"."vw_distribuicao_item_pedidos" AS item_pedidos 
        ON pedidos."cod_pedido" = item_pedidos."cod_pedido"
    LEFT JOIN "databeautykami"."vw_distribuicao_empresa_pedido" AS empresa_pedido 
        ON pedidos."cod_pedido" = empresa_pedido."cod_pedido"
    LEFT JOIN (
SELECT
            cod_pedido,
            cod_produto,
            mes_ref,
            SUM(custo_medio) as custo_medio
    FROM (
            SELECT 
                cod_pedido,
                cod_produto,
                DATE_TRUNC('month', dt_faturamento) mes_ref,
                CASE WHEN fator IS NULL Then ROUND(SUM(qtd * custo_unitario) / NULLIF(SUM(qtd), 0), 2)
                Else ROUND(SUM(qtd * (custo_unitario/fator)) / NULLIF(SUM(qtd), 0), 2)  END custo_medio
            FROM "databeautykami".tbl_varejo_cmv left join "databeautykami".tbl_distribuicao_bonificacao
            ON tbl_varejo_cmv.cod_marca = tbl_distribuicao_bonificacao.cod_marca
            and tbl_varejo_cmv.cod_empresa = cast(tbl_distribuicao_bonificacao.cod_empresa as varchar)
            and DATE_TRUNC('month', dt_faturamento) = date(tbl_distribuicao_bonificacao.mes_ref)
            GROUP BY 1, 2, 3, fator 
            UNION ALL 
            SELECT
                cod_pedido,
                codprod,
                DATE_TRUNC('month', dtvenda) mes_ref,
                CASE WHEN fator IS NULL Then ROUND(SUM(quant * custo) / NULLIF(SUM(quant), 0), 2)
                Else ROUND(SUM(quant * (custo/fator)) / NULLIF(SUM(quant), 0), 2)  END custo_medio
            FROM "databeautykami".tbl_salao_pedidos_salao left join "databeautykami".tbl_distribuicao_bonificacao
            ON DATE_TRUNC('month', dtvenda) = date(tbl_distribuicao_bonificacao.mes_ref)
            AND ( trim(upper(tbl_salao_pedidos_salao.categoria)) = trim(upper(tbl_distribuicao_bonificacao.marca))
                  OR substring(replace(upper(tbl_salao_pedidos_salao.categoria),'-',''),1,4) = upper(tbl_distribuicao_bonificacao.marca)
                  )
            where fator is not null
            GROUP BY 1, 2, 3 , fator   
            ) cmv_aux
            group by 1,2,3
    ) cmv ON pedidos.cod_pedido = cmv.cod_pedido 
        AND item_pedidos.sku = cmv.cod_produto 
        AND DATE_TRUNC('month', pedidos.dt_faturamento) = cmv.mes_ref
    WHERE
        pedidos."desc_abrev_cfop" IN (
            'VENDA', 'VENDA DE MERC.SUJEITA ST', 'VENDA DE MERCADORIA P/ NÃO CONTRIBUINTE',
            'VENDA DO CONSIGNADO', 'VENDA MERC. REC. TERCEIROS DESTINADA A ZONA FRANCA DE MANAUS',
            'VENDA MERC.ADQ. BRASIL FORA ESTADO', 'VENDA MERCADORIA DENTRO DO ESTADO',
            'Venda de mercadoria sujeita ao regime de substituição tributária',
            'VENDA MERCADORIA FORA ESTADO', 'VENDA MERC. SUJEITA AO REGIME DE ST'
        )
        AND date(pedidos."dt_faturamento") BETWEEN date('{start_date}') AND date('{end_date}')
        AND pedidos.operacoes_internas = 'N'
        {colaborador_filter}
        {channel_filter}
        {uf_filter}
        {brand_filter}
    GROUP BY 1
    ) f
    LEFT JOIN bonificacao b ON f.mes_ref = b.mes_ref {' AND f.cod_colaborador = b.cod_colaborador' if cod_colaborador else ''}
    ORDER BY f.mes_ref{', f.vendedor' if cod_colaborador else ''}
    """
    
    logging.info(f"Query executada: {query}")
    
    df = query_athena(query)
    return df

def get_brand_data(cod_colaborador, start_date, end_date, selected_channels, selected_ufs):
    try:
        colaborador_filter = f"AND empresa_pedido.cod_colaborador_atual = '{cod_colaborador}'" if cod_colaborador else ""
        
        channel_filter = ""
        if selected_channels:
            channels_str = "', '".join(selected_channels)
            channel_filter = f"AND pedidos.canal_venda IN ('{channels_str}')"
        
        uf_filter = ""
        if selected_ufs:
            ufs_str = "', '".join(selected_ufs)
            uf_filter = f"AND empresa_pedido.uf_empresa_faturamento IN ('{ufs_str}')"
        
        query = f"""
        SELECT
            item_pedidos.marca,
            COUNT(DISTINCT pedidos.cpfcnpj) AS clientes_unicos,
            SUM(item_pedidos.preco_desconto_rateado) AS faturamento,
            COUNT(DISTINCT pedidos.cod_pedido) AS qtd_pedido,
            COUNT(DISTINCT item_pedidos.cod_produto) AS qtd_sku,
            ROUND(SUM(item_pedidos.preco_desconto_rateado) / NULLIF(COUNT(DISTINCT pedidos.cpfcnpj), 0), 2) AS Ticket_Medio_Positivacao,
            CASE 
                WHEN SUM(COALESCE(cmv.custo_medio, 0) * item_pedidos.qtd) > 0 
                THEN ((SUM(item_pedidos.preco_desconto_rateado) - SUM(COALESCE(cmv.custo_medio, 0) * item_pedidos.qtd)) / SUM(COALESCE(cmv.custo_medio, 0) * item_pedidos.qtd)) * 100 
                ELSE 0 
            END AS markup_percentual
        FROM
            "databeautykami"."vw_distribuicao_pedidos" pedidos
        LEFT JOIN "databeautykami"."vw_distribuicao_item_pedidos" AS item_pedidos 
            ON pedidos."cod_pedido" = item_pedidos."cod_pedido"
        LEFT JOIN "databeautykami"."vw_distribuicao_empresa_pedido" AS empresa_pedido 
            ON pedidos."cod_pedido" = empresa_pedido."cod_pedido"
        LEFT JOIN (
            SELECT 
                cod_pedido,
                cod_produto,
                ROUND(SUM(qtd * custo_unitario) / NULLIF(SUM(qtd), 0), 2) AS custo_medio
            FROM "databeautykami".tbl_varejo_cmv
            GROUP BY cod_pedido, cod_produto
        ) cmv ON pedidos.cod_pedido = cmv.cod_pedido 
            AND item_pedidos.sku = cmv.cod_produto 
        WHERE
            pedidos."desc_abrev_cfop" IN (
                'VENDA', 'VENDA DE MERC.SUJEITA ST', 'VENDA DE MERCADORIA P/ NÃO CONTRIBUINTE',
                'VENDA DO CONSIGNADO', 'VENDA MERC. REC. TERCEIROS DESTINADA A ZONA FRANCA DE MANAUS',
                'VENDA MERC.ADQ. BRASIL FORA ESTADO', 'VENDA MERCADORIA DENTRO DO ESTADO',
                'Venda de mercadoria sujeita ao regime de substituição tributária',
                'VENDA MERCADORIA FORA ESTADO', 'VENDA MERC. SUJEITA AO REGIME DE ST'
            )
            AND pedidos.operacoes_internas = 'N'
            AND date(pedidos."dt_faturamento") BETWEEN date('{start_date}') AND date('{end_date}')
            {colaborador_filter}
            {channel_filter}
            {uf_filter}
        GROUP BY item_pedidos.marca
        ORDER BY faturamento DESC
        """
        logging.info(f"Executando query para dados de marca: {query}")
        df = query_athena(query)
        logging.info(f"Query para dados de marca executada com sucesso. Retornando DataFrame com {len(df)} linhas.")
        logging.info(f"Colunas retornadas: {df.columns.tolist()}")
        logging.info(f"Tipos de dados das colunas:\n{df.dtypes}")
        logging.info(f"Primeiras linhas do DataFrame:\n{df.head().to_string()}")
        return df
    except Exception as e:
        logging.error(f"Erro ao obter dados de marca: {str(e)}", exc_info=True)
        return pd.DataFrame()

def get_rfm_data(cod_colaborador, start_date, end_date):
    colaborador_filter = f"AND b.cod_colaborador_atual = '{cod_colaborador}'" if cod_colaborador else ""
    query = f"""
    WITH rfm_base AS (
        SELECT
            a.Cod_Cliente,
            a.Nome_Cliente,
            a.Recencia,
            a.Positivacao AS Frequencia,
            a.ticket_medio_posit as Monetario,
            a.ticket_medio_posit,
            b.cod_colaborador_atual
        FROM
            databeautykami.vw_analise_perfil_cliente a
        LEFT JOIN
            databeautykami.vw_distribuicao_cliente_vendedor b ON a.Cod_Cliente = b.cod_cliente
        WHERE 1 = 1 
        {colaborador_filter}
    ),
    rfm_scores AS (
        SELECT
            *,
            CASE
                WHEN Recencia BETWEEN 0 AND 1 THEN 5
                WHEN Recencia BETWEEN 2 AND 3 THEN 4
                WHEN Recencia BETWEEN 4 AND 5 THEN 3
                WHEN Recencia BETWEEN 6 AND 6 THEN 2
                ELSE 1
            END AS R_Score,
            CASE
                WHEN Frequencia >= 13 THEN 5
                WHEN Frequencia BETWEEN 10 AND 12 THEN 4
                WHEN Frequencia BETWEEN 7 AND 9 THEN 3
                WHEN Frequencia BETWEEN 4 AND 6 THEN 2
                ELSE 1
            END AS F_Score,
            NTILE(5) OVER (ORDER BY Monetario DESC) AS M_Score
        FROM
            rfm_base
    ),
    rfm_segments AS (
        SELECT
            *,
            CONCAT(CAST(R_Score AS VARCHAR), CAST(F_Score AS VARCHAR), CAST(M_Score AS VARCHAR)) AS RFM_Score,
            CASE
                WHEN R_Score = 5 AND F_Score = 5 AND M_Score = 5 THEN 'Campeões'
                WHEN R_Score = 5 AND F_Score >= 3 AND M_Score >= 3 THEN 'Clientes fiéis'
                WHEN R_Score = 4 AND F_Score >= 3 AND M_Score >= 3 THEN 'Fiéis em potencial'
                WHEN R_Score = 5 AND F_Score <= 2 THEN 'Novos clientes'
                WHEN R_Score = 5 AND M_Score <= 2 THEN 'Promessas'
                WHEN R_Score = 3 AND F_Score >= 3 AND M_Score >= 3 THEN 'Clientes precisando de atenção'
                WHEN R_Score <= 2 AND F_Score <= 2 AND M_Score <= 2 THEN 'Quase dormentes'
                WHEN R_Score = 2 AND F_Score >= 3 AND M_Score >= 3 THEN 'Em risco'
                WHEN R_Score = 1 AND F_Score >= 4 AND M_Score >= 4 THEN 'Não pode perder'
                WHEN R_Score <= 2 AND F_Score <= 2 AND M_Score >= 3 THEN 'Hibernando'
                ELSE 'Perdidos'
            END AS Segmento
        FROM
            rfm_scores
    )
    SELECT
        Cod_Cliente,
        Nome_Cliente,
        Recencia,
        Frequencia,
        Monetario,
        ticket_medio_posit ,
        R_Score,
        F_Score,
        M_Score,
        RFM_Score,
        Segmento,
        cod_colaborador_atual
    FROM
        rfm_segments
    ORDER BY
        Segmento, Monetario DESC;
    """
    logging.info(f"Executando query para dados de marca: {query}")
    return query_athena(query)

def get_rfm_summary(cod_colaborador, start_date, end_date, selected_channels, selected_ufs):
    colaborador_filter = f"AND b.cod_colaborador_atual = '{cod_colaborador}'" if cod_colaborador else ""
    
    channel_filter = ""
    if selected_channels:
        channels_str = "', '".join(selected_channels)
        channel_filter = f"AND a.Canal_Venda IN ('{channels_str}')"
    
    uf_filter = ""
    if selected_ufs:
        ufs_str = "', '".join(selected_ufs)
        uf_filter = f"AND a.uf_empresa IN ('{ufs_str}')"

    query = f"""
    WITH rfm_base AS (
        SELECT
            a.Cod_Cliente,
            a.uf_empresa,
            a.Canal_Venda,
            a.Recencia,
            a.Positivacao AS Frequencia,
            a.Monetario,
            b.cod_colaborador_atual
        FROM
            databeautykami.vw_analise_perfil_cliente a
        LEFT JOIN
            databeautykami.vw_distribuicao_cliente_vendedor b ON a.Cod_Cliente = b.cod_cliente
        WHERE 1 = 1         
        {colaborador_filter}
        {channel_filter}
        {uf_filter}
    ),
    rfm_scores AS (
        SELECT
            *,
            CASE
                WHEN Recencia BETWEEN 0 AND 1 THEN 5
                WHEN Recencia BETWEEN 2 AND 2 THEN 4
                WHEN Recencia BETWEEN 3 AND 3 THEN 3
                WHEN Recencia BETWEEN 4 AND 6 THEN 2
                ELSE 1
            END AS R_Score,
            CASE
                WHEN Frequencia >= 10 THEN 5
                WHEN Frequencia BETWEEN 7 AND 9 THEN 4
                WHEN Frequencia BETWEEN 3 AND 6 THEN 3
                WHEN Frequencia BETWEEN 2 AND 2 THEN 2
                ELSE 1
            END AS F_Score,
            NTILE(5) OVER (ORDER BY Monetario DESC) AS M_Score
        FROM
            rfm_base
    ),
    rfm_segments AS (
        SELECT
            *,
            CASE
                WHEN R_Score = 5 AND F_Score = 5  THEN 'Campeões'
                WHEN R_Score >= 4 AND F_Score >= 4  THEN 'Clientes fiéis'
                WHEN R_Score = 5 AND F_Score <= 2 THEN 'Novos clientes'
                WHEN R_Score <= 2 AND F_Score <= 3 THEN 'Em risco'
                WHEN R_Score = 1 AND F_Score = 1 THEN 'Perdidos'
                WHEN R_Score = 3 AND F_Score = 1  THEN 'Atenção'
                ELSE 'Outros'
            END AS Segmento
        FROM
            rfm_scores
    )
    SELECT
        Segmento,
        Canal_Venda,
        uf_empresa as Regiao,
        COUNT(*) AS Numero_Clientes,
        SUM(Monetario) AS Valor_Total,
        AVG(Monetario) AS Valor_Medio,
        AVG(R_Score) AS R_Score_Medio,
        AVG(F_Score) AS F_Score_Medio,
        AVG(M_Score) AS M_Score_Medio
    FROM
        rfm_segments
    GROUP BY
        Segmento, Canal_Venda, uf_empresa
    ORDER BY
        Valor_Total DESC, Canal_Venda;
    """
    return query_athena(query)

def get_rfm_segment_clients(cod_colaborador, start_date, end_date, segment, selected_channels, selected_ufs):
    colaborador_filter = f"AND b.cod_colaborador_atual = '{cod_colaborador}'" if cod_colaborador else ""
    channel_filter = f"AND a.Canal_Venda IN ('{','.join(selected_channels)}')" if selected_channels else ""
    uf_filter = f"AND a.uf_empresa IN ('{','.join(selected_ufs)}')" if selected_ufs else ""

    query = f"""
    WITH rfm_base AS (
        SELECT
            a.Cod_Cliente,
            a.Nome_Cliente,
            a.uf_empresa,
            a.Canal_Venda,
            a.Recencia,
            a.Positivacao AS Frequencia,
            a.Monetario,
            a.ticket_medio_posit,
            b.cod_colaborador_atual,
            a.Maior_Mes as Mes_Ultima_Compra,
            a.Ciclo_Vida as Life_Time
        FROM
            databeautykami.vw_analise_perfil_cliente a
        LEFT JOIN
            databeautykami.vw_distribuicao_cliente_vendedor b ON a.Cod_Cliente = b.cod_cliente
        WHERE 1 = 1 
        {colaborador_filter}
        {channel_filter}
        {uf_filter}
    ),
    rfm_scores AS (
        SELECT
            *,
            CASE
                WHEN Recencia BETWEEN 0 AND 1 THEN 5
                WHEN Recencia BETWEEN 2 AND 2 THEN 4
                WHEN Recencia BETWEEN 3 AND 3 THEN 3
                WHEN Recencia BETWEEN 4 AND 6 THEN 2
                ELSE 1
            END AS R_Score,
            CASE
                WHEN Frequencia >= 10 THEN 5
                WHEN Frequencia BETWEEN 7 AND 9 THEN 4
                WHEN Frequencia BETWEEN 3 AND 6 THEN 3
                WHEN Frequencia BETWEEN 2 AND 2 THEN 2
                ELSE 1
            END AS F_Score,
            NTILE(5) OVER (ORDER BY Monetario DESC) AS M_Score
        FROM
            rfm_base
    ),
    rfm_segments AS (
        SELECT
            *,
            CASE
                WHEN R_Score = 5 AND F_Score = 5  THEN 'Campeões'
                WHEN R_Score >= 3 AND F_Score >= 3  THEN 'Clientes fiéis'
                WHEN R_Score = 5 AND F_Score <= 2 THEN 'Novos clientes'
                WHEN R_Score <= 2 AND F_Score <= 3 THEN 'Em risco'
                WHEN R_Score = 1 AND F_Score = 1 THEN 'Perdidos'
                WHEN R_Score = 3 AND F_Score = 1  THEN 'Atenção'
                ELSE 'Outros'
            END AS Segmento
        FROM
            rfm_scores
    )
    SELECT
        Cod_Cliente,
        Nome_Cliente,
        uf_empresa,
        Canal_Venda,
        Recencia,
        Frequencia,
        Monetario,
        ticket_medio_posit,
        R_Score,
        F_Score,
        M_Score,
        Mes_Ultima_Compra,
        Life_time,
        Segmento
    FROM
        rfm_segments
    WHERE
        Segmento = '{segment}'
    ORDER BY
        Monetario DESC, Canal_Venda;
    """
    return query_athena(query)

def create_rfm_heatmap(rfm_summary):
    st.write("Dados do RFM Summary:")
    st.write(rfm_summary)

    # Verificar se as colunas necessárias existem
    required_columns = ['R_Score_Medio', 'F_Score_Medio', 'Numero_Clientes']
    missing_columns = [col for col in required_columns if col not in rfm_summary.columns]
    if missing_columns:
        st.error(f"Colunas ausentes no DataFrame: {', '.join(missing_columns)}")
        return None

    # Criando a matriz de contagem
    heatmap_data = pd.DataFrame(index=range(1, 6), columns=range(1, 6))
    heatmap_data = heatmap_data.fillna(0)

    try:
        for _, row in rfm_summary.iterrows():
            r_score = int(round(row['R_Score_Medio']))
            f_score = int(round(row['F_Score_Medio']))
            num_clients = row['Numero_Clientes']
            
            # Garantir que os scores estão dentro do intervalo 1-5
            r_score = max(1, min(5, r_score))
            f_score = max(1, min(5, f_score))
            
            heatmap_data.at[r_score, f_score] += num_clients

        st.write("Mapa de calor gerado:")
        st.write(heatmap_data)

        # Criando o mapa de calor
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_data.values,
            x=heatmap_data.columns,
            y=heatmap_data.index,
            colorscale='YlOrRd',
            hovertemplate='Recência: %{y}<br>Frequência: %{x}<br>Número de Clientes: %{z:.0f}<extra></extra>'
        ))

        # Adicionando o texto com o número de clientes em cada célula
        for i, row in heatmap_data.iterrows():
            for j, value in row.items():
                if value > 0:
                    fig.add_annotation(
                        x=j,
                        y=i,
                        text=str(int(value)),
                        showarrow=False,
                        font=dict(color="black" if value < heatmap_data.values.max()/2 else "white")
                    )

        fig.update_layout(
            title='Matriz RFM',
            xaxis_title='Frequência',
            yaxis_title='Recência',
            xaxis=dict(tickmode='array', tickvals=list(range(1, 6)), ticktext=[str(i) for i in range(1, 6)]),
            yaxis=dict(tickmode='array', tickvals=list(range(1, 6)), ticktext=[str(i) for i in range(1, 6)])
        )

        return fig
    except Exception as e:
        st.error(f"Erro ao criar o mapa de calor: {str(e)}")
        return None   

def create_dashboard(cod_colaborador, start_date, end_date, selected_channels, selected_ufs, selected_brands, show_additional_info):
    if cod_colaborador:
        st.title(f'Dashboard de Vendas - Colaborador {cod_colaborador}')
    else:
        st.title('Dashboard de Vendas - Todos os Colaboradores')
    
    with st.spinner('Carregando dados...'):
        df = get_monthly_revenue_cached(cod_colaborador, start_date, end_date, selected_channels, selected_ufs, selected_brands)
        brand_data = get_brand_data_cached(cod_colaborador, start_date, end_date, selected_channels, selected_ufs)
    
    if df.empty:
        st.warning("Não há dados para o período e/ou filtros selecionados.")
        return

    # Convertendo a coluna mes_ref para datetime e ordenando o DataFrame
    df['mes_ref'] = pd.to_datetime(df['mes_ref'])
    df = df.sort_values('mes_ref')

    # Criando um DataFrame com todos os meses no intervalo
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
    all_months = pd.DataFrame({'mes_ref': date_range})

    # Agrupando os dados por mês
    monthly_data = df.groupby('mes_ref').agg({
        'faturamento_liquido': 'sum',
        'positivacao': 'sum'
    }).reset_index()

    # Mesclando com todos os meses para garantir que todos os meses apareçam
    monthly_data = pd.merge(all_months, monthly_data, on='mes_ref', how='left').fillna(0)

    # Obtendo o mês mais recente
    latest_month = df['mes_ref'].max()
    latest_data = df[df['mes_ref'] == latest_month].groupby('mes_ref').sum().iloc[0]

    # Métricas
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:        
        st.metric("Faturamento", f"R$ {latest_data['faturamento_liquido']:,.2f}")
    with col2:
        st.metric("Desconto", f"R$ {latest_data['desconto']:,.2f}")
    with col3:
        st.metric("Bonificação", f"R$ {latest_data['valor_bonificacao']:,.2f}")        
    with col4:
        st.metric("Clientes Únicos", f"{latest_data['positivacao']:,}")
    with col5:
        st.metric("Pedidos", f"{latest_data['qtd_pedido']:,}")

    col_markup = st.columns(1)[0]
    with col_markup:        
        # Ajustando o formato do markup para ser igual ao da tabela
        markup_value = latest_data['markup_percentual'] / 100 + 1
        st.metric("Markup", f"{markup_value:.2f}")

    # Gráfico de Faturamento e Positivações ao longo do tempo
    fig_time = make_subplots(specs=[[{"secondary_y": True}]])

    # Adicionar barras de faturamento
    fig_time.add_trace(
        go.Bar(
            x=monthly_data['mes_ref'], 
            y=monthly_data['faturamento_liquido'], 
            name="Faturamento",
            marker_color='lightblue',
            text=monthly_data['faturamento_liquido'].apply(lambda x: f"R$ {x:,.0f}"),
            textposition='outside',
            hovertemplate="Mês: %{x|%B %Y}<br>Faturamento: R$ %{y:,.2f}<extra></extra>"
        ),
        secondary_y=False
    )

    # Adicionar linha de clientes únicos
    fig_time.add_trace(
        go.Scatter(
            x=monthly_data['mes_ref'], 
            y=monthly_data['positivacao'], 
            name="Clientes Únicos",
            mode='lines+markers+text',
            line=dict(color='red', width=2),
            marker=dict(size=8),
            text=monthly_data['positivacao'].apply(lambda x: f"{x:,.0f}"),
            textposition='top center',
            hovertemplate="Mês: %{x|%B %Y}<br>Clientes Únicos: %{y:,.0f}<extra></extra>"
        ),
        secondary_y=True
    )

    fig_time.update_layout(
        title_text="Evolução de Clientes Únicos e Faturamento",
        xaxis_title="Mês",
        xaxis=dict(
            tickformat="%b %Y",
            tickangle=45,
            tickmode='array',
            tickvals=monthly_data['mes_ref']
        ),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=20, t=60, b=20),
        hovermode="x unified"
    )

    # Atualizando os títulos dos eixos y
    fig_time.update_yaxes(title_text="Faturamento (R$)", secondary_y=False)
    fig_time.update_yaxes(title_text="Clientes Únicos", secondary_y=True)

    st.plotly_chart(fig_time, use_container_width=True)

    if not brand_data.empty and 'marca' in brand_data.columns:
        st.write("Dados por marca:")
        
        # Calculando o total de faturamento para o share
        total_faturamento = brand_data['faturamento'].sum()
        
        # Calculando o share e formatando o markup
        brand_data['share'] = brand_data['faturamento'] / total_faturamento
        brand_data['markup'] = (brand_data['markup_percentual'] / 100 + 1).apply(lambda x: f"{x:.2f}")
        
        # Ordenando por faturamento
        brand_data = brand_data.sort_values('faturamento', ascending=False)
        
        # Definindo as colunas que queremos exibir
        desired_columns = ['marca', 'faturamento', 'share', 'clientes_unicos', 'qtd_pedido', 'qtd_sku', 'Ticket_Medio_Positivacao', 'markup']
        
        # Criando um novo DataFrame com as colunas desejadas
        display_data = pd.DataFrame(columns=desired_columns)
        
        # Preenchendo o novo DataFrame com os dados disponíveis
        for col in desired_columns:
            if col in brand_data.columns:
                display_data[col] = brand_data[col]
            else:
                display_data[col] = ''
        
        # Formatando as colunas numéricas
        numeric_columns = ['faturamento', 'Ticket_Medio_Positivacao']
        for col in numeric_columns:
            if col in display_data.columns and display_data[col].dtype != 'object':
                display_data[col] = display_data[col].apply(lambda x: f"R$ {x:,.2f}" if pd.notnull(x) else '')
        
        # Formatando o share como porcentagem
        display_data['share'] = display_data['share'].apply(lambda x: f"{x:.2%}" if pd.notnull(x) else '')
        
        st.dataframe(display_data)

        # Mostrando quais colunas estão faltantes
        missing_columns = [col for col in desired_columns if col not in brand_data.columns]
        if missing_columns:
            st.warning(f"As seguintes colunas não estão disponíveis nos dados originais: {', '.join(missing_columns)}")
    else:
        st.warning("Não há dados por marca disponíveis para o período e/ou marcas selecionadas.")

    # Após a tabela de marcas, adicionamos a Matriz RFM
    st.subheader("Análise RFM")
    
    with st.spinner('Carregando dados RFM...'):
        rfm_summary = get_rfm_summary(cod_colaborador, start_date, end_date, selected_channels, selected_ufs)
    
    if not rfm_summary.empty:
        # Exibindo estatísticas dos segmentos
        st.subheader("Estatísticas dos Segmentos RFM")
        st.dataframe(rfm_summary.style.format({
            'Numero_Clientes': '{:,.0f}',
            'Valor_Total': 'R$ {:,.2f}',
            'Valor_Medio': 'R$ {:,.2f}',
            'R_Score_Medio': '{:.2f}',
            'F_Score_Medio': '{:.2f}',
            'M_Score_Medio': '{:.2f}'
        }))

        # Criar mapa de calor usando todos os dados
        try:
            fig_rfm = create_rfm_heatmap(rfm_summary)
            if fig_rfm is not None:
                st.plotly_chart(fig_rfm, use_container_width=True)
            else:
                st.error("Não foi possível criar o mapa de calor RFM.")
        except Exception as e:
            st.error(f"Erro ao criar o mapa de calor RFM: {e}")
            st.write("Dados do RFM Summary:")
            st.write(rfm_summary)

        # Lista de segmentos RFM
        segmentos_rfm = ['Todos'] + rfm_summary['Segmento'].tolist()
        
        # Radio buttons para seleção do segmento
        segmento_selecionado = st.radio("Selecione um segmento RFM para ver os clientes:", segmentos_rfm)

        # Análise de Clientes por Segmento RFM
        st.subheader("Análise de Clientes por Segmento RFM")
        
        if segmento_selecionado != 'Todos':
            with st.spinner(f'Carregando clientes do segmento {segmento_selecionado}...'):
                clientes_segmento = get_rfm_segment_clients(cod_colaborador, start_date, end_date, segmento_selecionado, selected_channels, selected_ufs)
                
                if not clientes_segmento.empty:
                    st.write(f"Clientes do segmento: {segmento_selecionado}")
                    
                    # Formatando as colunas numéricas
                    clientes_segmento['Monetario'] = clientes_segmento['Monetario'].apply(lambda x: f"R$ {x:,.2f}")
                    clientes_segmento['ticket_medio'] = clientes_segmento['ticket_medio_posit'].apply(lambda x: f"R$ {x:,.2f}")
                    
                    # Exibindo a tabela de clientes
                    st.dataframe(clientes_segmento[['Cod_Cliente', 'Nome_Cliente', 'Recencia', 'Frequencia', 'Monetario', 'ticket_medio','Mes_Ultima_Compra']])
                    
                    st.write(f"Total de clientes no segmento: {len(clientes_segmento)}")
                else:
                    st.warning(f"Não há clientes no segmento {segmento_selecionado} para o período e/ou colaborador selecionado.")
        else:
            st.info("Selecione um segmento específico para ver os detalhes dos clientes.")

    if show_additional_info:
        with st.expander("Informações Adicionais"):
            st.dataframe(df)

def main():
    st.set_page_config(page_title="Dashboard de Vendas", layout="wide")
    
    st.sidebar.title('Configurações do Dashboard')
    
    cod_colaborador = st.sidebar.text_input("Código do Colaborador (deixe em branco para todos)", "")
    
    today = date.today()
    start_date = st.sidebar.date_input("Data Inicial", date(2024, 1, 1))
    end_date = st.sidebar.date_input("Data Final", today)
    
    # Carregando canais de venda e UFs disponíveis
    channels, ufs = get_channels_and_ufs(cod_colaborador, start_date, end_date)
    
    # Filtro de canal de venda
    selected_channels = st.sidebar.multiselect("Selecione os canais de venda", options=channels)
    
    # Filtro de UF
    selected_ufs = st.sidebar.multiselect("Selecione as UFs", options=ufs)
    
    # Carregando dados para obter as marcas disponíveis
    brand_data = get_brand_data_cached(cod_colaborador, start_date, end_date, selected_channels, selected_ufs)
    available_brands = brand_data['marca'].unique().tolist() if not brand_data.empty else []
    
    # Filtro de marcas
    selected_brands = st.sidebar.multiselect("Selecione as marcas (deixe vazio para todas)", options=available_brands)
    
    show_additional_info = st.sidebar.checkbox("Mostrar informações adicionais", False)
    
    # Gerando o dashboard automaticamente
    with st.spinner('Gerando dashboard...'):
        create_dashboard(cod_colaborador, start_date, end_date, selected_channels, selected_ufs, selected_brands, show_additional_info)

    st.sidebar.markdown("---")
    st.sidebar.info(
        "Este dashboard mostra métricas de vendas para o(s) colaborador(es) selecionado(s). "
        "Use as configurações acima para personalizar a visualização."
    )

if __name__ == "__main__":
    main()