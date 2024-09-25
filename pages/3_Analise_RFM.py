import streamlit as st
import pandas as pd
from datetime import date
from utils import get_rfm_summary, get_rfm_segment_clients, create_rfm_heatmap

@st.cache_data
def get_rfm_summary_cached(cod_colaborador, start_date, end_date, selected_channels, selected_ufs):
    return get_rfm_summary(cod_colaborador, start_date, end_date, selected_channels, selected_ufs)

@st.cache_data
def get_rfm_segment_clients_cached(cod_colaborador, start_date, end_date, segmento, selected_channels, selected_ufs):
    return get_rfm_segment_clients(cod_colaborador, start_date, end_date, segmento, selected_channels, selected_ufs)

def main():
    st.title('Dashboard de Vendas - Análise RFM')

    st.sidebar.title('Configurações do Dashboard')

    # Usar os filtros do session_state
    cod_colaborador = st.sidebar.text_input("Código do Colaborador (deixe em branco para todos)", st.session_state.get('cod_colaborador', ""))
    start_date = st.sidebar.date_input("Data Inicial", st.session_state.get('start_date', date(2024, 1, 1)))
    end_date = st.sidebar.date_input("Data Final", st.session_state.get('end_date', date.today()))
    
    channels = st.session_state.get('channels', [])
    selected_channels = st.sidebar.multiselect("Selecione os canais de venda", options=channels, default=st.session_state.get('selected_channels', []))
    
    ufs = st.session_state.get('ufs', [])
    selected_ufs = st.sidebar.multiselect("Selecione as UFs", options=ufs, default=st.session_state.get('selected_ufs', []))

    # Atualizar o session_state com os valores atuais
    st.session_state['cod_colaborador'] = cod_colaborador
    st.session_state['start_date'] = start_date
    st.session_state['end_date'] = end_date
    st.session_state['selected_channels'] = selected_channels
    st.session_state['selected_ufs'] = selected_ufs

    with st.spinner('Carregando dados RFM...'):
        rfm_summary = get_rfm_summary_cached(cod_colaborador, start_date, end_date, selected_channels, selected_ufs)
    
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

        # Criar mapa de calor
        fig_rfm = create_rfm_heatmap(rfm_summary)
        if fig_rfm is not None:
            st.plotly_chart(fig_rfm, use_container_width=True)
        else:
            st.error("Não foi possível criar o mapa de calor RFM.")

        # Lista de segmentos RFM
        segmentos_rfm = ['Todos'] + rfm_summary['Segmento'].unique().tolist()
        
        # Radio buttons para seleção do segmento
        segmento_selecionado = st.radio("Selecione um segmento RFM para ver os clientes:", segmentos_rfm)

        # Análise de Clientes por Segmento RFM
        st.subheader("Análise de Clientes por Segmento RFM")
        
        if segmento_selecionado != 'Todos':
            with st.spinner(f'Carregando clientes do segmento {segmento_selecionado}...'):
                clientes_segmento = get_rfm_segment_clients_cached(cod_colaborador, start_date, end_date, segmento_selecionado, selected_channels, selected_ufs)
                
                if not clientes_segmento.empty:
                    st.write(f"Clientes do segmento: {segmento_selecionado}")
                    
                    # Formatando as colunas numéricas
                    clientes_segmento['Monetario'] = clientes_segmento['Monetario'].apply(lambda x: f"R$ {x:,.2f}")
                    clientes_segmento['ticket_medio'] = clientes_segmento['ticket_medio_posit'].apply(lambda x: f"R$ {x:,.2f}")
                    
                    # Exibindo a tabela de clientes
                    st.dataframe(clientes_segmento[['Cod_Cliente', 'Nome_Cliente', 'Recencia', 'Frequencia', 'Monetario', 'ticket_medio','Mes_Ultima_Compra']])
                    
                    st.write(f"Total de clientes no segmento: {len(clientes_segmento)}")
                else:
                    st.warning(f"Não há clientes no segmento {segmento_selecionado} para o período e/ou filtros selecionados.")
        else:
            st.info

if __name__ == "__main__":
    main()