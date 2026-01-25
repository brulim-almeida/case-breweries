import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path
from deltalake import DeltaTable

# Page config
st.set_page_config(
    page_title="Breweries Analytics",
    page_icon="🍺",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Cache data loading
@st.cache_data(ttl=3600)  # Cache por 1 hora
def load_gold_data():
    """Load data from Delta Lake using deltalake library (no Spark needed)"""
    base_path = "/opt/airflow/lakehouse/gold"
    
    try:
        data = {}
        
        # Mapear os nomes corretos das tabelas Gold
        table_mapping = {
            'by_country': 'breweries_by_country',
            'by_type': 'breweries_by_type',
            'by_state': 'breweries_by_state',
            'summary': 'brewery_summary_statistics',
            'breweries': 'breweries'  # Complete table for maps
        }
        
        for key, table_name in table_mapping.items():
            table_path = f"{base_path}/{table_name}"
            if Path(table_path).exists():
                try:
                    dt = DeltaTable(table_path)
                    data[key] = dt.to_pandas()
                except Exception as e:
                    st.warning(f"⚠️ Error reading {table_name}: {e}")
                    data[key] = pd.DataFrame()
            else:
                st.warning(f"⚠️ Table {table_name} not found. Run the pipeline first!")
                data[key] = pd.DataFrame()
        
        return data
    except Exception as e:
        st.error(f"Error loading Delta tables: {str(e)}")
        raise

# Main app
def main():
    # Header
    st.title("🍺 Breweries Data Lake - Gold Layer Analytics")
    st.markdown("**Real-time insights from brewery aggregations** | Data Lake Medallion Architecture")
    
    # Load data
    try:
        with st.spinner("Loading data from Delta Lake..."):
            data = load_gold_data()
        
        # Sidebar
        st.sidebar.header("⚙️ Configurações")
        
        # Refresh button
        if st.sidebar.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
        
        # Last update
        st.sidebar.info(f"📅 Última atualização: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check if data exists
        if data['by_country'].empty:
            st.error("## ⚠️ Nenhum Dado Encontrado!")
            st.markdown("""
            ### 📋 Para gerar os dados, siga os passos:
            
            1. **Acesse o Airflow**: http://localhost:8080
               - Login: `airflow`
               - Senha: `airflow`
            
            2. **Ative a DAG**: 
               - Vá em **DAGs**
               - Localize `breweries_pipeline_dag`
               - Clique no toggle para ativar ✅
            
            3. **Execute a DAG**:
               - Clique no botão **▶️ Trigger DAG**
               - Aguarde a conclusão (~15-20 minutos)
            
            4. **Refresh este dashboard**:
               - Clique em "🔄 Refresh Data" na sidebar
            
            ---
            
            ### 📊 O que será criado:
            - **Bronze Layer**: ~9,038 cervejarias (JSON)
            - **Silver Layer**: Dados limpos e normalizados (Delta Lake)
            - **Gold Layer**: 6 agregações de negócio
            
            """)
            st.stop()
        
        # Metrics row
        st.header("📊 Principais Métricas")
        
        # Get metrics from summary table
        summary = data['summary'].iloc[0] if not data['summary'].empty else {}
        
        total_breweries = summary.get('total_breweries', 0)
        total_countries = summary.get('distinct_countries', len(data['by_country']))
        total_types = summary.get('distinct_types', len(data['by_type']))
        coord_cov = (summary.get('with_coordinates', 0) / total_breweries * 100) if total_breweries > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("🏭 Total Breweries", f"{total_breweries:,}")
        col2.metric("🌍 Countries", total_countries)
        col3.metric("🏷️ Brewery Types", total_types)
        col4.metric("📍 Coordinate Coverage", f"{coord_cov:.1f}%")
        
        st.divider()
        
        # Tabs
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "🗺️ Maps", 
            "🌍 Geographic", 
            "🏷️ Types", 
            "📈 Quality", 
            "🏙️ Cities",
            "⚙️ Pipeline Metrics"
        ])
        
        with tab1:
            st.subheader("🗺️ Visualização Geográfica de Cervejarias")
            
            # Load breweries data with coordinates
            if 'breweries' in data and not data['breweries'].empty:
                breweries_df = data['breweries']
                
                # Filter only breweries with VALID coordinates
                # This removes points in the ocean and other suspicious coordinates
                if 'coordinates_valid' in breweries_df.columns:
                    breweries_with_coords = breweries_df[
                        (breweries_df['latitude'].notna()) & 
                        (breweries_df['longitude'].notna()) &
                        (breweries_df['coordinates_valid'] == True)
                    ].copy()
                    
                    invalid_coords = breweries_df[
                        (breweries_df['latitude'].notna()) & 
                        (breweries_df['longitude'].notna()) &
                        (breweries_df['coordinates_valid'] == False)
                    ]
                    
                    if len(invalid_coords) > 0:
                        st.warning(f"⚠️ {len(invalid_coords):,} cervejarias com coordenadas inválidas/suspeitas foram filtradas (ex: oceano, fora do país esperado)")
                else:
                    # Fallback if coordinates_valid column doesn't exist
                    breweries_with_coords = breweries_df[
                        (breweries_df['latitude'].notna()) & 
                        (breweries_df['longitude'].notna())
                    ].copy()
                
                if not breweries_with_coords.empty:
                    st.info(f"📍 Exibindo {len(breweries_with_coords):,} de {len(breweries_df):,} cervejarias com coordenadas válidas")
                    
                    # Sidebar filters
                    st.sidebar.header("🔍 Filtros do Mapa")
                    
                    # Country filter
                    countries = ['All'] + sorted(breweries_with_coords['country_normalized'].dropna().unique().tolist())
                    selected_country = st.sidebar.selectbox("País", countries)
                    
                    # Type filter
                    types = ['All'] + sorted(breweries_with_coords['brewery_type_normalized'].dropna().unique().tolist())
                    selected_type = st.sidebar.selectbox("Tipo de Cervejaria", types)
                    
                    # Apply filters
                    filtered_df = breweries_with_coords.copy()
                    if selected_country != 'All':
                        filtered_df = filtered_df[filtered_df['country_normalized'] == selected_country]
                    if selected_type != 'All':
                        filtered_df = filtered_df[filtered_df['brewery_type_normalized'] == selected_type]
                    
                    # Show metrics after filtering
                    col1, col2, col3 = st.columns(3)
                    col1.metric("🍺 Cervejarias Filtradas", f"{len(filtered_df):,}")
                    col2.metric("🌍 Países", filtered_df['country_normalized'].nunique())
                    col3.metric("🏷️ Tipos", filtered_df['brewery_type_normalized'].nunique())
                    
                    st.markdown("---")
                    
                    # Map type selector
                    map_type = st.radio(
                        "Selecione o tipo de visualização:",
                        ["🌍 Mapa de Dispersão", "🔥 Mapa de Densidade"],
                        horizontal=True
                    )
                    
                    if map_type == "🌍 Mapa de Dispersão":
                        # Scatter map using scatter_geo (works without mapbox token)
                        fig = px.scatter_geo(
                            filtered_df,
                            lat='latitude',
                            lon='longitude',
                            hover_name='name',
                            hover_data={
                                'brewery_type_normalized': True,
                                'city': True,
                                'state': True,
                                'country_normalized': True,
                                'latitude': ':.4f',
                                'longitude': ':.4f'
                            },
                            color='brewery_type_normalized',
                            title=f'Localização de Cervejarias {"- " + selected_country if selected_country != "All" else "(Global)"}',
                            height=700,
                            projection='natural earth'
                        )
                        fig.update_geos(
                            showcountries=True,
                            countrycolor="lightgray",
                            showcoastlines=True,
                            coastlinecolor="darkgray",
                            showland=True,
                            landcolor="white",
                            showlakes=True,
                            lakecolor="lightblue"
                        )
                        fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
                        st.plotly_chart(fig, use_container_width=True)
                        
                    else:  # Density Map
                        # Create density heatmap by aggregating nearby locations
                        st.info("💡 Densidade calculada por concentração geográfica de cervejarias")
                        
                        # Aggregate by rounded coordinates to create density
                        density_df = filtered_df.copy()
                        density_df['lat_rounded'] = density_df['latitude'].round(1)
                        density_df['lon_rounded'] = density_df['longitude'].round(1)
                        
                        # Aggregate and keep city/state information
                        density_agg = density_df.groupby(['lat_rounded', 'lon_rounded']).agg({
                            'id': 'count',
                            'city': 'first',
                            'state': 'first',
                            'country_normalized': 'first'
                        }).reset_index()
                        density_agg.columns = ['lat_rounded', 'lon_rounded', 'count', 'city', 'state', 'country_normalized']
                        
                        # Create scatter plot with size representing density
                        fig = px.scatter_geo(
                            density_agg,
                            lat='lat_rounded',
                            lon='lon_rounded',
                            size='count',
                            color='count',
                            color_continuous_scale='YlOrRd',
                            title=f'Densidade de Cervejarias {"- " + selected_country if selected_country != "All" else "(Global)"}',
                            labels={'count': 'Concentração'},
                            height=700,
                            projection='natural earth',
                            size_max=60,
                            hover_data={'city': True, 'state': True, 'country_normalized': True}
                        )
                        fig.update_geos(
                            showcountries=True,
                            countrycolor="lightgray",
                            showcoastlines=True,
                            coastlinecolor="darkgray",
                            showland=True,
                            landcolor="white",
                            showlakes=True,
                            lakecolor="lightblue"
                        )
                        fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Show density stats
                        st.markdown(f"**🔥 Regiões com maior densidade:**")
                        top_density = density_agg.nlargest(10, 'count')
                        cols = st.columns(5)
                        for idx, row in enumerate(top_density.iterrows()):
                            col_idx = idx % 5
                            with cols[col_idx]:
                                city_name = row[1]['city'] if pd.notna(row[1]['city']) else 'N/A'
                                state_name = row[1]['state'] if pd.notna(row[1]['state']) else ''
                                location = f"{city_name}, {state_name}" if state_name else city_name
                                st.metric(
                                    f"📍 {location}",
                                    f"{row[1]['count']} 🍺"
                                )
                    
                    # Additional insights
                    st.markdown("---")
                    st.subheader("📊 Insights Geográficos")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Top cities
                        top_cities = filtered_df.groupby(['city', 'state', 'country_normalized']).size().reset_index(name='count').nlargest(10, 'count')
                        st.markdown("**🏙️ Top 10 Cidades**")
                        for idx, row in top_cities.iterrows():
                            st.write(f"{idx+1}. {row['city']}, {row['state']} ({row['country_normalized']}) - {row['count']} cervejarias")
                    
                    with col2:
                        # Coverage by country
                        country_stats = filtered_df.groupby('country_normalized').agg({
                            'name': 'count',
                            'latitude': 'mean',
                            'longitude': 'mean'
                        }).reset_index()
                        country_stats.columns = ['País', 'Total', 'Lat Média', 'Lon Média']
                        st.markdown("**🌍 Estatísticas por País**")
                        st.dataframe(country_stats.sort_values('Total', ascending=False), hide_index=True, use_container_width=True)
                    
                    # Downloadable data
                    with st.expander("📥 Baixar Dados Filtrados"):
                        csv = filtered_df.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download CSV",
                            data=csv,
                            file_name=f"breweries_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                
                else:
                    st.warning("⚠️ Nenhuma cervejaria com coordenadas válidas encontrada.")
            else:
                st.info("📊 A tabela completa de cervejarias não está disponível. Execute a pipeline completa para gerar os dados.")
        
        with tab2:
            st.subheader("Distribuição Global de Cervejarias")
            
            # Two columns for better visualization
            col1, col2 = st.columns([1, 1])
            
            with col1:
                # All countries
                top_countries = data['by_country'].nlargest(20, 'brewery_count')
                
                fig = px.bar(
                    top_countries,
                    x='brewery_count',
                    y='country_normalized',
                    orientation='h',
                    title='Top 20 Países (Todos)',
                    labels={'brewery_count': 'Total de Cervejarias', 'country_normalized': 'País'},
                    color='brewery_count',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(height=600, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Excluding USA for better visibility of other countries
                other_countries = data['by_country'][
                    data['by_country']['country_normalized'] != 'United States'
                ].nlargest(19, 'brewery_count')
                
                fig = px.bar(
                    other_countries,
                    x='brewery_count',
                    y='country_normalized',
                    orientation='h',
                    title='Top 19 Países (Excluindo EUA)',
                    labels={'brewery_count': 'Total de Cervejarias', 'country_normalized': 'País'},
                    color='brewery_count',
                    color_continuous_scale='Viridis'
                )
                fig.update_layout(height=600, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            
            # Summary insight
            usa_count = data['by_country'][
                data['by_country']['country_normalized'] == 'United States'
            ]['brewery_count'].values[0] if len(data['by_country'][
                data['by_country']['country_normalized'] == 'United States'
            ]) > 0 else 0
            usa_pct = (usa_count / total_breweries * 100) if total_breweries > 0 else 0
            
            st.info(f"🇺🇸 **EUA domina o mercado:** {usa_count:,} cervejarias ({usa_pct:.1f}% do total global)")
            
            # Data table
            with st.expander("📋 Ver Tabela Completa"):
                st.dataframe(
                    data['by_country'].sort_values('brewery_count', ascending=False),
                    use_container_width=True
                )
        
        with tab3:
            st.subheader("Análise por Tipo de Cervejaria")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Pie chart - usar brewery_count
                fig = px.pie(
                    data['by_type'],
                    values='brewery_count',
                    names='brewery_type_normalized',
                    title='Distribuição por Tipo',
                    hole=0.4
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Bar chart
                fig = px.bar(
                    data['by_type'].sort_values('brewery_count', ascending=True),
                    x='brewery_count',
                    y='brewery_type_normalized',
                    orientation='h',
                    title='Total por Tipo',
                    labels={'brewery_count': 'Total', 'brewery_type_normalized': 'Tipo'},
                    color='brewery_count',
                    color_continuous_scale='Greens'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Insights
            top_type = data['by_type'].iloc[0]
            st.info(f"💡 **Insight:** O tipo mais comum é **{top_type['brewery_type_normalized']}** com {top_type['brewery_count']:,} cervejarias ({top_type['brewery_count']/total_breweries*100:.1f}% do total)")
        
        with tab4:
            st.subheader("Métricas de Qualidade dos Dados")
            
            # Gauges - usar summary
            col1, col2, col3 = st.columns(3)
            
            with col1:
                coord_coverage_pct = (summary.get('with_coordinates', 0) / summary.get('total_breweries', 1) * 100)
                fig = go.Figure(go.Indicator(
                    mode="gauge+number+delta",
                    value=coord_coverage_pct,
                    title={'text': "Coordinate Coverage"},
                    delta={'reference': 90},
                    domain={'x': [0, 1], 'y': [0, 1]},
                    number={'font': {'size': 50}},
                    gauge={
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "darkblue"},
                        'steps': [
                            {'range': [0, 50], 'color': "lightgray"},
                            {'range': [50, 80], 'color': "yellow"},
                            {'range': [80, 100], 'color': "lightgreen"}
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': 90
                        }
                    }
                ))
                fig.update_layout(
                    height=300,
                    margin=dict(l=30, r=30, t=60, b=40),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True, key="gauge1")
            
            with col2:
                contact_pct = (summary.get('with_contact', 0) / summary.get('total_breweries', 1) * 100)
                
                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=contact_pct,
                    title={'text': "Contact Info Coverage"},
                    domain={'x': [0, 1], 'y': [0, 1]},
                    number={'font': {'size': 50}},
                    gauge={
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "darkgreen"},
                        'steps': [
                            {'range': [0, 50], 'color': "lightgray"},
                            {'range': [50, 80], 'color': "yellow"},
                            {'range': [80, 100], 'color': "lightgreen"}
                        ]
                    }
                ))
                fig.update_layout(
                    height=300,
                    margin=dict(l=30, r=30, t=60, b=40),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True, key="gauge2")
            
            with col3:
                # Overall quality score (média das métricas)
                overall_quality = (coord_coverage_pct + contact_pct) / 2
                
                fig = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=overall_quality,
                    title={'text': "Overall Data Quality"},
                    domain={'x': [0, 1], 'y': [0, 1]},
                    number={'font': {'size': 50}},
                    gauge={
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "purple"},
                        'steps': [
                            {'range': [0, 50], 'color': "lightgray"},
                            {'range': [50, 80], 'color': "yellow"},
                            {'range': [80, 100], 'color': "lightgreen"}
                        ]
                    }
                ))
                fig.update_layout(
                    height=300,
                    margin=dict(l=30, r=30, t=60, b=40),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True, key="gauge3")
            
            # Detalhes
            st.markdown("---")
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric(
                    "🗺️ Breweries com Coordenadas",
                    f"{summary.get('with_coordinates', 0):,}",
                    f"{coord_coverage_pct:.1f}%"
                )
                st.metric(
                    "📞 Breweries com Contato",
                    f"{summary.get('with_contact', 0):,}",
                    f"{contact_pct:.1f}%"
                )
            
            with col2:
                st.metric(
                    "❌ Breweries sem Coordenadas",
                    f"{summary.get('total_breweries', 0) - summary.get('with_coordinates', 0):,}",
                    f"-{100-coord_coverage_pct:.1f}%",
                    delta_color="inverse"
                )
                st.metric(
                    "✅ Registros Completos",
                    f"{summary.get('complete_records', 0):,}",
                    f"{summary.get('complete_records', 0)/summary.get('total_breweries', 1)*100:.1f}%"
                )
        
        with tab5:
            st.subheader("Análise por Estado e Cidade")
            
            # State analysis (Top 20)
            if not data['by_state'].empty:
                st.subheader("Top 20 Estados com Mais Cervejarias")
                top_states = data['by_state'].nlargest(20, 'brewery_count')
                
                fig = px.bar(
                    top_states,
                    x='brewery_count',
                    y='state',
                    orientation='h',
                    title='Top 20 Estados',
                    labels={'brewery_count': 'Total de Cervejarias', 'state': 'Estado'},
                    color='brewery_count',
                    color_continuous_scale='Oranges',
                    hover_data=['country_normalized', 'distinct_cities']
                )
                fig.update_layout(height=600)
                st.plotly_chart(fig, use_container_width=True)
                
                # Treemap
                st.subheader("Distribuição Hierárquica: País → Estado")
                fig = px.treemap(
                    top_states,
                    path=['country_normalized', 'state'],
                    values='brewery_count',
                    title='Breweries por País e Estado',
                    color='brewery_count',
                    color_continuous_scale='RdYlGn'
                )
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
                
                # Data table
                with st.expander("📋 Ver Tabela Completa de Estados"):
                    st.dataframe(
                        data['by_state'].sort_values('brewery_count', ascending=False),
                        use_container_width=True
                    )
        
        # NEW TAB: Pipeline Metrics
        with tab6:
            st.subheader("⚙️ Pipeline Execution Metrics")
            
            try:
                import sys
                from pathlib import Path
                sys.path.insert(0, str(Path(__file__).parent.parent))
                from src.utils.metadata_manager import PipelineMetadataManager
                
                metadata_mgr = PipelineMetadataManager()
                latest_run = metadata_mgr.get_latest_run()
                all_runs = metadata_mgr.get_all_runs(limit=20)
                
                if latest_run:
                    # Latest Run Overview
                    st.markdown("### 🎯 Última Execução")
                    
                    exec_date = latest_run.get('execution_date', 'N/A')
                    status = latest_run.get('status', 'unknown')
                    dag_run_id = latest_run.get('dag_run_id', 'N/A')
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("📅 Data", exec_date)
                    col2.metric("✅ Status", status.upper(), 
                               delta="Success" if status == 'success' else "Failed",
                               delta_color="normal" if status == 'success' else "inverse")
                    col3.metric("🔄 Run ID", dag_run_id.split('__')[-1][:15] if '__' in dag_run_id else dag_run_id[:15])
                    
                    st.divider()
                    
                    # Execution Times
                    st.markdown("### ⏱️ Tempos de Execução")
                    
                    exec_times = latest_run.get('execution_times', {})
                    bronze_time = exec_times.get('bronze_ingestion_time', 0)
                    silver_time = exec_times.get('silver_transformation_time', 0)
                    gold_time = exec_times.get('gold_aggregation_time', 0)
                    total_time = exec_times.get('total_pipeline_time', 0)
                    
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("🥉 Bronze", f"{bronze_time:.1f}s", 
                               help="Tempo de ingestão da API")
                    col2.metric("🥈 Silver", f"{silver_time:.1f}s", 
                               help="Tempo de transformação e geocoding")
                    col3.metric("🥇 Gold", f"{gold_time:.1f}s", 
                               help="Tempo de agregação")
                    col4.metric("⏱️ Total", f"{total_time:.1f}s", 
                               help="Tempo total do pipeline")
                    
                    # Execution time breakdown chart
                    if bronze_time > 0 or silver_time > 0 or gold_time > 0:
                        fig = go.Figure(data=[
                            go.Bar(
                                x=['Bronze', 'Silver', 'Gold'],
                                y=[bronze_time, silver_time, gold_time],
                                marker=dict(
                                    color=['#CD7F32', '#C0C0C0', '#FFD700'],
                                    line=dict(color='rgb(8,48,107)', width=1.5)
                                ),
                                text=[f"{bronze_time:.1f}s", f"{silver_time:.1f}s", f"{gold_time:.1f}s"],
                                textposition='auto',
                            )
                        ])
                        fig.update_layout(
                            title="Tempo de Execução por Camada",
                            xaxis_title="Camada",
                            yaxis_title="Tempo (segundos)",
                            height=400
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    
                    st.divider()
                    
                    # Data Quality Metrics
                    st.markdown("### 📊 Métricas de Qualidade de Dados")
                    
                    data_quality = latest_run.get('data_quality', {})
                    records_ingested = data_quality.get('records_ingested', 0)
                    records_transformed = data_quality.get('records_transformed', 0)
                    aggregations = data_quality.get('aggregations_created', 0)
                    data_loss = data_quality.get('data_loss_rate', 0)
                    
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("📥 Ingeridos", f"{records_ingested:,}", 
                               help="Registros extraídos da API")
                    col2.metric("🔄 Transformados", f"{records_transformed:,}", 
                               help="Registros na Silver layer")
                    col3.metric("📦 Agregações", aggregations, 
                               help="Tabelas criadas na Gold layer")
                    col4.metric("📉 Data Loss", f"{data_loss:.2f}%", 
                               delta=f"{data_loss:.2f}%",
                               delta_color="inverse" if data_loss > 5 else "off",
                               help="Perda de dados Bronze → Silver")
                    
                    # Data flow sankey
                    fig = go.Figure(data=[go.Sankey(
                        node=dict(
                            pad=15,
                            thickness=20,
                            line=dict(color="black", width=0.5),
                            label=["Bronze Layer", "Silver Layer", "Gold Layer"],
                            color=["#CD7F32", "#C0C0C0", "#FFD700"]
                        ),
                        link=dict(
                            source=[0, 1],
                            target=[1, 2],
                            value=[records_transformed, aggregations * 100],  # Scale for visibility
                            label=[f"{records_transformed:,} records", f"{aggregations} tables"]
                        )
                    )])
                    fig.update_layout(
                        title="Fluxo de Dados no Pipeline",
                        height=300
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.divider()
                    
                    # Great Expectations Results
                    st.markdown("### 🔍 Resultados de Validação (Great Expectations)")
                    
                    validation_results = latest_run.get('validation_results', {})
                    
                    # Create validation summary
                    val_data = []
                    for layer in ['bronze', 'silver', 'gold']:
                        layer_val = validation_results.get(layer, {})
                        val_data.append({
                            'Layer': layer.capitalize(),
                            'Success': '✅' if layer_val.get('success', False) else '❌',
                            'Success Rate': f"{layer_val.get('success_rate', 0):.1f}%",
                            'Passed': layer_val.get('passed', 0),
                            'Failed': layer_val.get('failed', 0),
                            'Total': layer_val.get('total_expectations', layer_val.get('total_aggregations', 0))
                        })
                    
                    val_df = pd.DataFrame(val_data)
                    
                    # Display as styled dataframe
                    st.dataframe(
                        val_df.style.applymap(
                            lambda x: 'background-color: #90EE90' if x == '✅' else ('background-color: #FFB6C1' if x == '❌' else ''),
                            subset=['Success']
                        ),
                        use_container_width=True,
                        hide_index=True
                    )
                    
                    # Validation success rate chart
                    fig = go.Figure()
                    
                    for layer in ['bronze', 'silver', 'gold']:
                        layer_val = validation_results.get(layer, {})
                        passed = layer_val.get('passed', 0)
                        failed = layer_val.get('failed', 0)
                        
                        fig.add_trace(go.Bar(
                            name=layer.capitalize(),
                            x=['Passed', 'Failed'],
                            y=[passed, failed],
                            text=[passed, failed],
                            textposition='auto',
                        ))
                    
                    fig.update_layout(
                        title="Validações por Camada",
                        barmode='group',
                        xaxis_title="Status",
                        yaxis_title="Quantidade",
                        height=400
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Silver enrichment stats
                    silver_val = validation_results.get('silver', {})
                    enrichment_stats = silver_val.get('enrichment_stats', {})
                    
                    if enrichment_stats:
                        st.markdown("### 🌍 Estatísticas de Enrichment (Geocoding)")
                        
                        coord_cov = enrichment_stats.get('coordinate_coverage', 0) * 100
                        valid_coords = enrichment_stats.get('valid_coordinates_rate', 0) * 100
                        geocoded = enrichment_stats.get('geocoded_rate', 0) * 100
                        country_norm = enrichment_stats.get('country_normalized_rate', 0) * 100
                        
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("📍 Coordinate Coverage", f"{coord_cov:.1f}%",
                                   help="% de registros com coordenadas")
                        col2.metric("✅ Valid Coordinates", f"{valid_coords:.1f}%",
                                   help="% de coordenadas geograficamente válidas")
                        col3.metric("🗺️ Geocoded", f"{geocoded:.1f}%",
                                   help="% enriched via Nominatim API")
                        col4.metric("🌍 Country Normalized", f"{country_norm:.1f}%",
                                   help="% com país normalizado")
                        
                        # Enrichment gauge chart
                        fig = go.Figure()
                        
                        fig.add_trace(go.Indicator(
                            mode="gauge+number+delta",
                            value=coord_cov,
                            title={'text': "Coordinate Coverage"},
                            delta={'reference': 74, 'suffix': '%'},
                            gauge={
                                'axis': {'range': [None, 100]},
                                'bar': {'color': "darkblue"},
                                'steps': [
                                    {'range': [0, 50], 'color': "lightgray"},
                                    {'range': [50, 80], 'color': "gray"},
                                    {'range': [80, 100], 'color': "lightgreen"}
                                ],
                                'threshold': {
                                    'line': {'color': "red", 'width': 4},
                                    'thickness': 0.75,
                                    'value': 85
                                }
                            }
                        ))
                        
                        fig.update_layout(height=300)
                        st.plotly_chart(fig, use_container_width=True)
                    
                    st.divider()
                    
                    # Execution History
                    st.markdown("### 📈 Histórico de Execuções (Últimas 20)")
                    
                    if len(all_runs) > 1:
                        history_data = []
                        for run in all_runs:
                            exec_times = run.get('execution_times', {})
                            history_data.append({
                                'Execution Date': run.get('execution_date', 'N/A'),
                                'Status': run.get('status', 'unknown'),
                                'Records': run.get('data_quality', {}).get('records_ingested', 0),
                                'Total Time (s)': exec_times.get('total_pipeline_time', 0),
                                'Bronze (s)': exec_times.get('bronze_ingestion_time', 0),
                                'Silver (s)': exec_times.get('silver_transformation_time', 0),
                                'Gold (s)': exec_times.get('gold_aggregation_time', 0),
                            })
                        
                        history_df = pd.DataFrame(history_data)
                        
                        # Timeline chart
                        fig = go.Figure()
                        
                        fig.add_trace(go.Scatter(
                            x=history_df['Execution Date'],
                            y=history_df['Total Time (s)'],
                            mode='lines+markers',
                            name='Total Time',
                            line=dict(color='blue', width=2),
                            marker=dict(size=8)
                        ))
                        
                        fig.update_layout(
                            title="Tempo de Execução ao Longo do Tempo",
                            xaxis_title="Data de Execução",
                            yaxis_title="Tempo Total (segundos)",
                            height=400,
                            hovermode='x unified'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Volume chart
                        fig = go.Figure()
                        
                        fig.add_trace(go.Scatter(
                            x=history_df['Execution Date'],
                            y=history_df['Records'],
                            mode='lines+markers',
                            name='Records Ingested',
                            line=dict(color='green', width=2),
                            marker=dict(size=8),
                            fill='tozeroy'
                        ))
                        
                        fig.update_layout(
                            title="Volume de Dados ao Longo do Tempo",
                            xaxis_title="Data de Execução",
                            yaxis_title="Registros Ingeridos",
                            height=400,
                            hovermode='x unified'
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # History table
                        with st.expander("📋 Ver Tabela Completa de Histórico"):
                            st.dataframe(history_df, use_container_width=True, hide_index=True)
                    else:
                        st.info("Execute o pipeline mais vezes para ver o histórico de execuções.")
                    
                else:
                    st.warning("⚠️ Nenhum metadado de execução encontrado. Execute o pipeline primeiro!")
                    st.markdown("""
                    ### Como gerar metadados:
                    
                    1. Acesse o Airflow (http://localhost:8080)
                    2. Execute a DAG `breweries_pipeline_dag`
                    3. Aguarde a conclusão
                    4. Retorne aqui e recarregue a página
                    """)
                    
            except Exception as e:
                st.error(f"❌ Erro ao carregar metadados: {str(e)}")
                st.exception(e)
        
        # Footer
        st.divider()
        st.markdown("""
        **🏗️ Arquitetura:** Medallion (Bronze → Silver → Gold) | 
        **💾 Storage:** Delta Lake | 
        **⚙️ Processing:** PySpark 3.5.0 | 
        **🔄 Orchestration:** Apache Airflow 2.9.3
        """)
        
    except Exception as e:
        st.error(f"❌ Erro ao carregar dados: {str(e)}")
        st.exception(e)

if __name__ == "__main__":
    main()