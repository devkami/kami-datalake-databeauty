import streamlit as st
import pandas as pd
from datetime import date
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from utils import (
    get_monthly_revenue, 
    get_brand_data, 
    get_channels_and_ufs, 
    get_colaboradores, 
    get_client_status,
    create_client_status_chart
)

@st.cache_data
def get_monthly_revenue_cached(cod_colaborador, start_date, end_date, selected_channels, selected_ufs, selected_brands, selected_nome_colaborador):
    return get_monthly_revenue(cod_colaborador, start_date, end_date, selected_channels, selected_ufs, selected_brands,selected_nome_colaborador)

@st.cache_data
def get_brand_data_cached(cod_colaborador, start_date, end_date, selected_channels, selected_ufs, selected_nome_colaborador):
    return get_brand_data(cod_colaborador, start_date, end_date, selected_channels, selected_ufs, selected_nome_colaborador)

@st.cache_data
def get_channels_and_ufs_cached(cod_colaborador, start_date, end_date):
    return get_channels_and_ufs(cod_colaborador, start_date, end_date)

@st.cache_data
def get_colaboradores_cached(start_date, end_date, selected_channels, selected_ufs):
    return get_colaboradores(start_date, end_date, selected_channels, selected_ufs)

def create_dashboard(df, brand_data, client_status_data, cod_colaborador, start_date, end_date, selected_channels, selected_ufs, selected_brands, selected_colaboradores, show_additional_info):
    if cod_colaborador:
        st.title(f'Dashboard de Vendas - Colaborador {cod_colaborador}')
    else:
        st.title('Dashboard de Vendas 📈')
    
    if df.empty:
        st.warning("Não há dados para o período e/ou filtros selecionados.")
        return

    # Aplicar filtro de marcas ao DataFrame principal
    if selected_brands and 'marca' in df.columns:
        df = df[df['marca'].isin(selected_brands)]

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

    # Cálculo dos percentuais
    desconto_percentual = (latest_data['desconto'] / latest_data['faturamento_bruto']) * 100 if latest_data['faturamento_bruto'] != 0 else 0
    bonificacao_percentual = (latest_data['valor_bonificacao'] / latest_data['faturamento_liquido']) * 100 if latest_data['faturamento_liquido'] != 0 else 0

    # Métricas
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:        
        st.metric("Faturamento", f"R$ {latest_data['faturamento_liquido']:,.2f}")
    with col2:
        st.metric("Desconto", f"R$ {latest_data['desconto']:,.2f}")
        st.markdown(f"<p style='font-size: medium; color: green;'>({desconto_percentual:.2f}% do faturamento bruto)</p>", unsafe_allow_html=True)
    with col3:
        st.metric("Bonificação", f"R$ {latest_data['valor_bonificacao']:,.2f}")
        st.markdown(f"<p style='font-size: medium; color: green;'>({bonificacao_percentual:.2f}% do faturamento líquido)</p>", unsafe_allow_html=True)
    with col4:
        st.metric("Clientes Únicos", f"{latest_data['positivacao']:,}")
    with col5:
        st.metric("Pedidos", f"{latest_data['qtd_pedido']:,}")
     
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:        
        # Ajustando o formato do markup para ser igual ao da tabela
        markup_value = latest_data['markup_percentual'] / 100 + 1
        st.metric("Markup", f"{markup_value:.2f}")

    # Gráfico de Faturamento e Positivações ao longo do tempo
    fig_time = make_subplots(specs=[[{"secondary_y": True}]])

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

    fig_time.update_yaxes(title_text="Faturamento (R$)", secondary_y=False)
    fig_time.update_yaxes(title_text="Clientes Únicos", secondary_y=True)

    st.plotly_chart(fig_time, use_container_width=True)
    st.divider()

    # Dados por marca
    if not brand_data.empty and 'marca' in brand_data.columns:
        st.write("Dados por marca:")
        
        # Aplicar filtro de marcas selecionadas
        if selected_brands:
            brand_data = brand_data[brand_data['marca'].isin(selected_brands)]
        
        # Calculando o total de faturamento para o share
        total_faturamento = brand_data['faturamento'].sum()
        
        # Calculando o share e formatando o markup
        brand_data['share'] = brand_data['faturamento'] / total_faturamento
        brand_data['markup'] = brand_data['markup_percentual'].apply(lambda x: f"{(x/100 + 1):.2f}")
        
        # Ordenando por faturamento
        brand_data = brand_data.sort_values('faturamento', ascending=False)
        
        # Definindo as colunas que queremos exibir
        desired_columns = ['marca', 'faturamento', 'share', 'clientes_unicos', 'qtd_pedido', 'qtd_sku', 'Ticket_Medio_Positivacao', 'markup']
        
        # Criando um novo DataFrame com as colunas desejadas
        display_data = brand_data[desired_columns].copy().set_index('marca')
        
        # Formatando as colunas numéricas
        display_data['faturamento'] = display_data['faturamento'].apply(lambda x: f"R$ {x:,.2f}")
        display_data['Ticket_Medio_Positivacao'] = display_data['Ticket_Medio_Positivacao'].apply(lambda x: f"R$ {x:,.2f}")
        display_data['share'] = display_data['share'].apply(lambda x: f"{x:.2%}")
        
        st.dataframe(display_data,
                     column_config={
                         "share": st.column_config.ProgressColumn(
                             "share"
                         )
                     })
    else:
        st.warning("Não há dados por marca disponíveis para o período e/ou filtros selecionados.")

    # Adicionar o gráfico de status do cliente
    
    st.subheader("Status dos Clientes")
    if client_status_data is not None and not client_status_data.empty:
        client_status_chart = create_client_status_chart(client_status_data)
        if client_status_chart:
            st.plotly_chart(client_status_chart, use_container_width=True)
    else:
        st.warning("Não há dados disponíveis para o gráfico de status do cliente.")

    if show_additional_info:
        with st.expander("Informações Adicionais"):
            st.dataframe(df)

def main():
    try:
        st.set_page_config(page_title="Dashboard de Vendas", layout="wide", )

        # Inicialização do estado da sessão
        if 'initialized' not in st.session_state:
            st.session_state['initialized'] = True
            st.session_state['cod_colaborador'] = ""
            st.session_state['start_date'] = date(2024, 1, 1)
            st.session_state['end_date'] = date.today()
            st.session_state['selected_channels'] = []
            st.session_state['selected_ufs'] = []
            st.session_state['selected_colaboradores'] = []
            st.session_state['selected_brands'] = []
            st.session_state['data_needs_update'] = True
            st.session_state['client_status_data'] = None

        st.sidebar.title('Configurações do Dashboard')
        
        # Código do Colaborador
        new_cod_colaborador = st.sidebar.text_input("Código do Colaborador (deixe em branco para todos)", st.session_state['cod_colaborador'])
        if new_cod_colaborador != st.session_state['cod_colaborador']:
            st.session_state['cod_colaborador'] = new_cod_colaborador
            st.session_state['data_needs_update'] = True

        # Datas
        new_start_date = st.sidebar.date_input("Data Inicial", st.session_state['start_date'])
        new_end_date = st.sidebar.date_input("Data Final", st.session_state['end_date'])
        if new_start_date != st.session_state['start_date'] or new_end_date != st.session_state['end_date']:
            st.session_state['start_date'] = new_start_date
            st.session_state['end_date'] = new_end_date
            st.session_state['data_needs_update'] = True

        # Atualizar canais e UFs
        channels, ufs = get_channels_and_ufs_cached(st.session_state['cod_colaborador'], st.session_state['start_date'], st.session_state['end_date'])
        
        # Canais de Venda
        new_selected_channels = st.sidebar.multiselect("Selecione os canais de venda", options=channels, default=st.session_state['selected_channels'])
        if new_selected_channels != st.session_state['selected_channels']:
            st.session_state['selected_channels'] = new_selected_channels
            st.session_state['data_needs_update'] = True

        # UFs
        new_selected_ufs = st.sidebar.multiselect("Selecione as UFs", options=ufs, default=st.session_state['selected_ufs'])
        if new_selected_ufs != st.session_state['selected_ufs']:
            st.session_state['selected_ufs'] = new_selected_ufs
            st.session_state['data_needs_update'] = True

        # Colaboradores
        if not st.session_state['cod_colaborador']:
            colaboradores_df = get_colaboradores_cached(st.session_state['start_date'], st.session_state['end_date'], st.session_state['selected_channels'], st.session_state['selected_ufs'])
            available_colaboradores = colaboradores_df['nome_colaborador'].tolist()
            new_selected_colaboradores = st.sidebar.multiselect("Selecione os colaboradores (deixe vazio para todos)", options=available_colaboradores, default=st.session_state['selected_colaboradores'])
            if new_selected_colaboradores != st.session_state['selected_colaboradores']:
                st.session_state['selected_colaboradores'] = new_selected_colaboradores
                st.session_state['data_needs_update'] = True
        else:
            st.session_state['selected_colaboradores'] = []

        # Carregar ou recarregar dados se necessário
        if st.session_state['data_needs_update']:
            progress_text = "Operação em andamento. Aguarde..."
            my_bar = st.progress(0, text=progress_text)

            try:
                # Carregando dados de receita mensal
                my_bar.progress(10, text="Carregando dados de receita mensal...")
                st.session_state['df'] = get_monthly_revenue_cached(
                    st.session_state['cod_colaborador'],
                    st.session_state['start_date'],
                    st.session_state['end_date'],
                    st.session_state['selected_channels'],
                    st.session_state['selected_ufs'],
                    st.session_state['selected_brands'],
                    st.session_state['selected_colaboradores']
                )

                # Carregando dados de marca
                my_bar.progress(40, text="Carregando dados de marca...")
                st.session_state['brand_data'] = get_brand_data_cached(
                    st.session_state['cod_colaborador'],
                    st.session_state['start_date'],
                    st.session_state['end_date'],
                    st.session_state['selected_channels'],
                    st.session_state['selected_ufs'],
                    st.session_state['selected_colaboradores']
                )

                # Carregando dados de status do cliente
                my_bar.progress(70, text="Carregando dados de status do cliente...")
                st.session_state['client_status_data'] = get_client_status(
                    start_date=st.session_state['start_date'].strftime('%Y-%m-%d'),
                    end_date=st.session_state['end_date'].strftime('%Y-%m-%d'),
                    cod_colaborador=st.session_state['cod_colaborador'],
                    selected_channels=st.session_state['selected_channels'],
                    selected_ufs=st.session_state['selected_ufs'],
                    selected_colaboradores=st.session_state['selected_colaboradores']
                )

                my_bar.progress(100, text="Carregamento concluído!")
                time.sleep(1)  # Pausa breve para que o usuário veja a conclusão
                my_bar.empty()  # Remove a barra de progresso

            except Exception as e:
                my_bar.empty()  # Remove a barra de progresso em caso de erro
                st.error(f"Erro ao carregar dados: {str(e)}")
                st.error("Por favor, verifique se os filtros aplicados são compatíveis com o código do colaborador selecionado.")
                return

            st.session_state['data_needs_update'] = False

        df = st.session_state['df']
        brand_data = st.session_state['brand_data']
        client_status_data = st.session_state['client_status_data']
        
        # Marcas
        available_brands = brand_data['marca'].unique().tolist() if not brand_data.empty else []
        new_selected_brands = st.sidebar.multiselect("Selecione as marcas (deixe vazio para todas)", options=available_brands, default=st.session_state['selected_brands'])
        if new_selected_brands != st.session_state['selected_brands']:
            st.session_state['selected_brands'] = new_selected_brands
            st.session_state['data_needs_update'] = True

        show_additional_info = st.sidebar.checkbox("Mostrar informações adicionais", False)
        
        if st.session_state['data_needs_update']:
            with st.spinner('Atualizando dados...'):
                try:
                    st.session_state['df'] = get_monthly_revenue_cached(
                        st.session_state['cod_colaborador'],
                        st.session_state['start_date'],
                        st.session_state['end_date'],
                        st.session_state['selected_channels'],
                        st.session_state['selected_ufs'],
                        st.session_state['selected_brands'],
                        st.session_state['selected_colaboradores']
                    )
                    st.session_state['brand_data'] = get_brand_data_cached(
                        st.session_state['cod_colaborador'],
                        st.session_state['start_date'],
                        st.session_state['end_date'],
                        st.session_state['selected_channels'],
                        st.session_state['selected_ufs'],
                        st.session_state['selected_colaboradores']
                    )
                    # Atualizar dados de status do cliente
                    st.session_state['client_status_data'] = get_client_status(
                        start_date=st.session_state['start_date'].strftime('%Y-%m-%d'),
                        end_date=st.session_state['end_date'].strftime('%Y-%m-%d'),
                        cod_colaborador=st.session_state['cod_colaborador'],
                        selected_channels=st.session_state['selected_channels'],
                        selected_ufs=st.session_state['selected_ufs'],
                        selected_colaboradores=st.session_state['selected_colaboradores']
                )

                except Exception as e:
                    st.error(f"Erro ao carregar dados: {str(e)}")
                    st.error("Por favor, verifique se os filtros aplicados são compatíveis com o código do colaborador selecionado.")
                    return

            st.session_state['data_needs_update'] = False
        
        df = st.session_state['df']
        brand_data = st.session_state['brand_data']
        client_status_data = st.session_state['client_status_data']

        create_dashboard(
            df, 
            brand_data, 
            client_status_data,
            st.session_state['cod_colaborador'], 
            st.session_state['start_date'], 
            st.session_state['end_date'], 
            st.session_state['selected_channels'], 
            st.session_state['selected_ufs'], 
            st.session_state['selected_brands'], 
            st.session_state['selected_colaboradores'], 
            show_additional_info
        )

    except Exception as e:
        st.error(f"Ocorreu um erro ao carregar o dashboard: {str(e)}")
        st.write("Detalhes do erro:")
        st.write(e)
        st.write("Por favor, verifique se todos os dados necessários foram carregados corretamente na página inicial.")

if __name__ == "__main__":
    main()