import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
import json
from pathlib import Path
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../ETL/.env')  # if .env is in ETL folder

# Add the path to your database connection
sys.path.append('../ETL')
from ukeducationdbconnection import UKEducationDB

st.set_page_config(
    page_title="UK Education Analytics Dashboard",
    page_icon="🏫",
    layout="wide"
)

# Load choropleth configuration and data
@st.cache_data
def load_choropleth_data():
    """Load choropleth mapping data and configuration"""
    try:
        # Load configuration
        with open('choropleth_config.json', 'r') as f:
            config = json.load(f)
        
        # Load GeoJSON boundaries
        with open(config['boundaries_file'], 'r') as f:
            geojson_data = json.load(f)
        
        # Load LA mapping
        mapping_df = pd.read_csv(config['mapping_file'])
        
        return geojson_data, mapping_df, config
    
    except Exception as e:
        st.warning(f"Choropleth data not available: {e}")
        return None, None, None

# Initialize database connection
@st.cache_resource
def init_database():
    """Initialize database connection"""
    return UKEducationDB()

@st.cache_data
def aggregate_la_performance():
    """Aggregate school performance by local authority"""
    db = init_database()
    query = """
    SELECT 
        s.local_authority_code,
        la.la_name,
        la.region,
        la.population,
        la.sen_funding_per_pupil,
        la.total_sen_pupils,
        COUNT(*) as total_schools,
        -- Performance metrics
        AVG(sp.attainment_8_score) as avg_attainment_8,
        AVG(sp.progress_8_score) as avg_progress_8,
        AVG(sp.grade_4_english_maths_percentage) as avg_grade_4_eng_math,
        AVG(sp.grade_5_english_maths_percentage) as avg_grade_5_eng_math,
        AVG(sp.ebacc_average_point_score) as avg_ebacc_score,
        AVG(sp.staying_in_education_percentage) as avg_staying_in_education,
        -- Count non-null values
        COUNT(sp.attainment_8_score) as schools_with_attainment_8,
        COUNT(sp.progress_8_score) as schools_with_progress_8,
        COUNT(sp.grade_4_english_maths_percentage) as schools_with_grade_4,
        COUNT(sp.staying_in_education_percentage) as schools_with_staying_ed,
        -- Ofsted ratings
        MODE() WITHIN GROUP (ORDER BY oi.overall_effectiveness) as modal_ofsted_rating,
        COUNT(sp.urn) as schools_with_performance,
        COUNT(oi.urn) as schools_with_ofsted
    FROM schools s
    LEFT JOIN local_authorities la ON s.local_authority_code = la.la_code
    LEFT JOIN school_performance sp ON s.urn = sp.urn
    LEFT JOIN ofsted_inspections oi ON s.urn = oi.urn
    WHERE s.local_authority_code IS NOT NULL
    GROUP BY s.local_authority_code, la.la_name, la.region, la.population, la.sen_funding_per_pupil, la.total_sen_pupils
    HAVING COUNT(*) >= 3
    ORDER BY la.la_name
    """
    return db.read_sql(query)

@st.cache_data  
def load_regional_attainment():
    """Load regional education attainment data"""
    db = init_database()
    query = """
    SELECT 
        region,
        disability_status,
        qualification_level,
        COUNT(*) as records,
        AVG(sample_size) as avg_sample_size
    FROM graduate_outcomes
    WHERE region IS NOT NULL
    GROUP BY region, disability_status, qualification_level
    ORDER BY region, disability_status, qualification_level
    """
    return db.read_sql(query)

def create_choropleth_map(data, value_column, title, color_scale="Viridis", geojson_data=None, mapping_df=None, config=None):
    """Create a choropleth map for UK local authorities with proper column handling"""
    
    if geojson_data is None or mapping_df is None or config is None:
        st.warning("Choropleth mapping not available - showing bar chart instead")
        return create_bar_chart_alternative(data, value_column, title)
    
    try:
        # Prepare data for merging - only include necessary columns to avoid conflicts
        data_clean = data.dropna(subset=[value_column]).copy()
        
        # Select only the columns we need from the original data
        data_for_merge = data_clean[['local_authority_code', 'la_name', value_column]].copy()
        
        # Select only the columns we need from mapping 
        mapping_for_merge = mapping_df[['la_code', 'boundary_code']].copy()
        
        # Merge data with mapping
        map_data = pd.merge(
            data_for_merge,
            mapping_for_merge,
            left_on='local_authority_code',
            right_on='la_code',
            how='inner'
        )
        
        if len(map_data) == 0:
            st.warning("No mappable data found - showing bar chart instead")
            return create_bar_chart_alternative(data, value_column, title)
        
        # Debug info
        st.write(f"🔍 Debug: Merged {len(map_data)} records for mapping")
        
        # Create choropleth
        fig = go.Figure(go.Choropleth(
            geojson=geojson_data,
            locations=map_data['boundary_code'],
            z=map_data[value_column],
            featureidkey=f"properties.{config['code_field']}",
            colorscale=color_scale,
            showscale=True,
            hovertemplate='<b>%{text}</b><br>' +
                         f'{title}: %{{z:.1f}}<br>' +
                         '<extra></extra>',
            text=map_data['la_name']  # This should now work correctly
        ))
        
        fig.update_geos(
            fitbounds="locations",
            visible=False
        )
        
        fig.update_layout(
            title=title,
            height=600
        )
        
        # Show coverage info
        total_las = len(data_clean)
        mapped_las = len(map_data)
        st.info(f"🗺️ Choropleth shows {mapped_las} of {total_las} Local Authorities ({mapped_las/total_las*100:.0f}% coverage)")
        
        return fig
        
    except Exception as e:
        st.error(f"Error creating choropleth: {e}")
        # Debug information
        st.write("🔍 Debug information:")
        if 'map_data' in locals():
            st.write(f"Columns in merged data: {list(map_data.columns)}")
            st.write("Sample merged data:")
            st.dataframe(map_data.head())
        
        return create_bar_chart_alternative(data, value_column, title)

def create_bar_chart_alternative(data, value_column, title, top_n=20):
    """Create bar chart as alternative to choropleth"""
    
    clean_data = data.dropna(subset=[value_column])
    if len(clean_data) == 0:
        return None
    
    top_data = clean_data.nlargest(min(top_n, len(clean_data)), value_column)
    
    fig = px.bar(
        top_data,
        x=value_column,
        y='la_name',
        orientation='h',
        title=f"Top {len(top_data)} Local Authorities: {title}",
        labels={value_column: title, 'la_name': 'Local Authority'}
    )
    fig.update_layout(height=max(400, len(top_data) * 20))
    
    return fig

def main():
    st.title("🏫 UK Education Analytics Dashboard")
    st.markdown("**Exploring school performance and educational outcomes across local authorities**")
    
    # Load choropleth data
    geojson_data, mapping_df, choropleth_config = load_choropleth_data()
    
    if geojson_data is not None:
        st.sidebar.success("🗺️ Choropleth maps enabled!")
        st.sidebar.markdown(f"📊 {len(mapping_df)} LAs mappable")
    else:
        st.sidebar.warning("📊 Using bar charts (choropleth setup needed)")
    
    # Sidebar navigation
    st.sidebar.title("📊 Dashboard Sections")
    section = st.sidebar.selectbox(
        "Choose a section:",
        ["Overview", "Performance Maps", "SEN Analysis", "Ofsted Analysis", "Regional Attainment", "Data Explorer"]
    )
    
    # Load data
    with st.spinner("Loading data..."):
        try:
            la_performance = aggregate_la_performance()
            regional_data = load_regional_attainment()
        except Exception as e:
            st.error(f"Error loading data: {e}")
            st.stop()
    
    st.sidebar.markdown(f"**Data Summary:**")
    st.sidebar.markdown(f"- {len(la_performance)} Local Authorities")
    st.sidebar.markdown(f"- {la_performance['total_schools'].sum():,.0f} Schools")
    st.sidebar.markdown(f"- {len(regional_data)} Regional Records")
    
    # Debug info for troubleshooting
    if section == "Overview":
        with st.expander("🔍 Debug Information"):
            st.write("**LA Performance Columns:**")
            st.write(list(la_performance.columns))
            if mapping_df is not None:
                st.write("**Mapping DataFrame Columns:**")
                st.write(list(mapping_df.columns))
                st.write("**Sample Mapping Data:**")
                st.dataframe(mapping_df.head())
    
    # Main content
    if section == "Overview":
        show_overview(la_performance, geojson_data, mapping_df, choropleth_config)
    elif section == "Performance Maps":
        show_performance_maps(la_performance, geojson_data, mapping_df, choropleth_config)
    elif section == "SEN Analysis":
        show_sen_analysis(la_performance, geojson_data, mapping_df, choropleth_config)
    elif section == "Ofsted Analysis":
        show_ofsted_analysis(la_performance, geojson_data, mapping_df, choropleth_config)
    elif section == "Regional Attainment":
        show_regional_analysis(regional_data)
    elif section == "Data Explorer":
        show_data_explorer(la_performance, regional_data)

def show_overview(la_performance, geojson_data, mapping_df, choropleth_config):
    st.header("📈 Overview Dashboard")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Local Authorities", f"{len(la_performance):,}")
    with col2:
        st.metric("Total Schools", f"{la_performance['total_schools'].sum():,.0f}")
    with col3:
        avg_att8 = la_performance['avg_attainment_8'].mean()
        if pd.notna(avg_att8):
            st.metric("Mean Attainment 8", f"{avg_att8:.1f}")
        else:
            st.metric("Mean Attainment 8", "No data")
    with col4:
        if geojson_data is not None and mapping_df is not None:
            st.metric("Mappable LAs", f"{len(mapping_df)}")
        else:
            st.metric("Choropleth Maps", "Not available")
    
    # Test choropleth with simple data
    st.subheader("🗺️ Test Geographic Visualization")
    
    if geojson_data is not None and mapping_df is not None:
        # Try with SEN funding data
        sen_funding_data = la_performance.dropna(subset=['sen_funding_per_pupil'])
        
        if len(sen_funding_data) > 0:
            st.write(f"📊 Testing with {len(sen_funding_data)} LAs that have SEN funding data")
            
            fig = create_choropleth_map(
                sen_funding_data,
                'sen_funding_per_pupil',
                'SEN Funding per Pupil (£)',
                'Viridis',
                geojson_data,
                mapping_df,
                choropleth_config
            )
            if fig:
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No SEN funding data available for mapping")
    else:
        st.info("Choropleth maps not available - run choropleth setup script first")

def show_performance_maps(la_performance, geojson_data, mapping_df, choropleth_config):
    st.header("🗺️ School Performance Geographic Analysis")
    
    # Test with Attainment 8 first
    st.subheader("📊 Attainment 8 Scores by Local Authority")
    
    att8_data = la_performance.dropna(subset=['avg_attainment_8'])
    
    if len(att8_data) > 0:
        st.write(f"📈 Data available for {len(att8_data)} Local Authorities")
        
        fig = create_choropleth_map(
            att8_data,
            'avg_attainment_8',
            'Average Attainment 8 Score',
            'Viridis',
            geojson_data,
            mapping_df,
            choropleth_config
        )
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        
        # Show top performers
        st.markdown("**Top 5 Performers:**")
        top_5 = att8_data.nlargest(5, 'avg_attainment_8')[['la_name', 'avg_attainment_8']]
        st.dataframe(top_5.round(1))
    else:
        st.warning("No Attainment 8 data available")

def show_sen_analysis(la_performance, geojson_data, mapping_df, choropleth_config):
    st.header("♿ SEN Analysis by Local Authority")
    
    st.subheader("💰 SEN Funding Geographic Distribution")
    
    funding_data = la_performance.dropna(subset=['sen_funding_per_pupil'])
    
    if len(funding_data) > 0:
        fig = create_choropleth_map(
            funding_data,
            'sen_funding_per_pupil',
            'SEN Funding per Pupil (£)',
            'Blues',
            geojson_data,
            mapping_df,
            choropleth_config
        )
        if fig:
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No SEN funding data available")

def show_ofsted_analysis(la_performance, geojson_data, mapping_df, choropleth_config):
    st.header("🎯 Ofsted Ratings Analysis")
    
    ofsted_data = la_performance.dropna(subset=['modal_ofsted_rating'])
    
    if len(ofsted_data) > 0:
        fig = create_choropleth_map(
            ofsted_data,
            'modal_ofsted_rating',
            'Modal Ofsted Rating (1=Outstanding, 4=Inadequate)',
            'RdYlGn_r',
            geojson_data,
            mapping_df,
            choropleth_config
        )
        if fig:
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No Ofsted data available")

def show_regional_analysis(regional_data):
    st.header("🌍 Regional Education Attainment Analysis")
    
    if len(regional_data) == 0:
        st.error("No regional attainment data available")
        return
    
    # Simple regional analysis without choropleth
    st.subheader("📊 Data by Region")
    
    regional_summary = regional_data.groupby('region').agg({
        'records': 'sum'
    }).sort_values('records', ascending=False)
    
    st.dataframe(regional_summary)

def show_data_explorer(la_performance, regional_data):
    st.header("🔍 Data Explorer")
    
    st.subheader("🏫 Local Authority Performance Data")
    
    # Show sample data
    display_cols = [
        'la_name', 'local_authority_code', 'region', 'total_schools', 
        'avg_attainment_8', 'avg_progress_8', 'sen_funding_per_pupil'
    ]
    
    st.dataframe(la_performance[display_cols].head(20).round(2))

if __name__ == "__main__":
    main()