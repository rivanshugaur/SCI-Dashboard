import streamlit as st
import pandas as pd
import plotly.express as px
from cleaner_func import load_cleaned_data
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# ==============================================
# CONSTANTS AND CONFIGURATION
# ==============================================

# Database configuration
DB_CONFIG = {
    "user": "root",
    "password": "LSrqVWjJJeexcYUtPijebYrdYTKfFtHy", 
    "host": "hopper.proxy.rlwy.net",
    "port": "42510",
    "name": "railway"
}

# KPI columns
KPI_COLS = ["Total_Income", "DOE", "IOE", "PBT", "GOP"]
KPI_COLORS = {
    "Total_Income": "#1f77b4",
    "DOE": "#ff7f0e",
    "IOE": "#2ca02c",
    "PBT": "#d62728",
    "GOP": "#9467bd"
}

# Month names and ordering
MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]
MONTH_ORDER = {month: idx for idx, month in enumerate(MONTHS, start=1)}

# Quarter mapping
QUARTER_MAP = {
    "January": "Q1", "February": "Q1", "March": "Q1",
    "April": "Q2", "May": "Q2", "June": "Q2",
    "July": "Q3", "August": "Q3", "September": "Q3",
    "October": "Q4", "November": "Q4", "December": "Q4"
}

# ==============================================
# CUSTOM CSS STYLING
# ==============================================

def apply_custom_css():
    st.markdown("""
    <style>
        /* Main page styling */
        .main {
            background-color: #0e1117;
        }
        
        /* Header styling */
        .header {
            padding: 1rem 0;
            background: linear-gradient(90deg, #1a2a6c, #2a52be);
            border-radius: 10px;
            margin-bottom: 1.5rem;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        
        .header h1 {
            color: white;
            text-align: center;
            font-size: 2.5rem;
            margin-bottom: 0.5rem;
        }
        
        .header h2 {
            color: #f0f0f0;
            text-align: center;
            font-size: 1.2rem;
            font-weight: 300;
        }
        
        /* Sidebar styling */
        .sidebar .sidebar-content {
            background-color: #1e2130;
            padding: 1rem;
            border-radius: 10px;
        }
        
        /* Card styling */
        .card {
            background-color: #1e2130;
            border-radius: 10px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            border-left: 4px solid #2a52be;
        }
        
        .card h3 {
            color: #ffffff;
            border-bottom: 1px solid #2a52be;
            padding-bottom: 0.5rem;
            margin-top: 0;
        }
        
        /* KPI summary styling */
        .kpi-card {
            background-color: #1e2130;
            border-radius: 10px;
            padding: 1rem;
            margin: 0.5rem 0;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }
        
        .kpi-title {
            font-size: 1rem;
            color: #a0a0a0;
            margin-bottom: 0.5rem;
        }
        
        .kpi-value {
            font-size: 1.4rem;
            font-weight: bold;
            color: #ffffff;
        }
        
        /* Button styling */
        .stButton>button {
            background: linear-gradient(90deg, #1a2a6c, #2a52be);
            color: white;
            border: none;
            border-radius: 5px;
            padding: 0.5rem 1rem;
            font-weight: bold;
            transition: all 0.3s ease;
        }
        
        .stButton>button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
        }
        
        /* Tab styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 10px;
        }
        
        .stTabs [data-baseweb="tab"] {
            background-color: #1e2130 !important;
            border-radius: 5px 5px 0 0 !important;
            padding: 0.5rem 1rem !important;
            transition: all 0.3s ease;
        }
        
        .stTabs [aria-selected="true"] {
            background-color: #2a52be !important;
            color: white !important;
        }
        
        /* Metric styling */
        .metric-container {
            display: flex;
            flex-wrap: wrap;
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        
        .metric-box {
            flex: 1;
            min-width: 200px;
            background: linear-gradient(135deg, #1e2130, #2a3a6c);
            border-radius: 10px;
            padding: 1.2rem;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        
        .metric-label {
            font-size: 1rem;
            color: #a0a0a0;
            margin-bottom: 0.5rem;
        }
        
        .metric-value {
            font-size: 1.8rem;
            font-weight: bold;
            color: #ffffff;
        }
        
        /* Filter styling */
        .filter-box {
            background-color: #1e2130;
            border-radius: 10px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        
        .filter-box h3 {
            color: #ffffff;
            margin-top: 0;
            border-bottom: 1px solid #2a52be;
            padding-bottom: 0.5rem;
        }
        
        /* Table styling */
        .dataframe {
            border-radius: 10px;
            overflow: hidden;
        }
        
        .dataframe th {
            background-color: #2a52be !important;
            color: white !important;
        }
        
        .dataframe tr:nth-child(even) {
            background-color: #1e2130;
        }
        
        .dataframe tr:nth-child(odd) {
            background-color: #252a40;
        }
    </style>
    """, unsafe_allow_html=True)

# ==============================================
# HELPER FUNCTIONS
# ==============================================

def create_db_engine():
    """Create and return a database engine"""
    escaped_password = quote_plus(DB_CONFIG['password'])
    return create_engine(
        f"mysql+pymysql://{DB_CONFIG['user']}:{escaped_password}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['name']}"
    )

def load_and_clean_data(engine):
    """Load and clean data from database"""
    df = pd.read_sql("SELECT * FROM kpi_data", con=engine)
    
    # Data cleaning
    df['year'] = df['year'].astype(int)
    df['sector'] = df['sector'].astype(str).str.strip()
    df['vessel'] = df['vessel'].astype(str).str.strip()
    df = df.dropna(subset=['year', 'sector', 'vessel'])
    
    # Add month index and month-year columns
    df['month_index'] = df['month'].map(MONTH_ORDER)
    df["month_year"] = (
        df["year"].astype(str) + "-" + df["month_index"].astype(str).str.zfill(2)
    )
    df["month_year"] = pd.to_datetime(
        df["month_year"], format="%Y-%m"
    ).dt.strftime('%b %Y')
    
    return df

def display_kpi_summary(df, kpi_cols):
    """Create and display KPI summary"""
    totals = {col: df[col].sum() for col in kpi_cols}
    
    # Create a container for metrics
    st.markdown("### 📊 KPI Summary")
    
    # Create columns for metrics
    cols = st.columns(len(kpi_cols))
    for i, kpi in enumerate(kpi_cols):
        with cols[i]:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">{kpi}</div>
                <div class="kpi-value">₹{abs(totals[kpi]):,.2f} Lacs ({'Debit' if totals[kpi] >= 0 else 'Credit'})</div>
            </div>
            """, unsafe_allow_html=True)

def create_bar_chart(df, x, y, color=None, facet_col=None, 
                     barmode="group", text_auto=True, width=1000, facet_col_wrap=None):
    # Format bar text: show Paid or Credit, only add if not present
    if "formatted_text" not in df.columns:
        df["formatted_text"] = df[y].apply(
            lambda val: f"₹{abs(val):,.2f} ({'Debit' if val >= 0 else 'Credit'})"
        )
    """Standardized bar chart"""
    fig = px.bar(
        df,
        x=x,
        y=y,
        color=color,
        facet_col=facet_col,
        facet_col_wrap=facet_col_wrap,
        barmode=barmode,
        text="formatted_text",
        width=width,
        color_discrete_map=KPI_COLORS
    )
    fig.update_layout(
        bargap=0.15, 
        bargroupgap=0.05,
        xaxis_title=x.replace("_", " ").title(),
        yaxis_title="₹ (Lacs)",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor='#2a3a6c')
    return fig

def create_line_chart(df, x, y, color=None, title=None):
    """Create a standardized line chart"""
    fig = px.line(
        df,
        x=x,
        y=y,
        color=color,
        markers=True,
        title=title,
        color_discrete_map=KPI_COLORS
    )
    fig.update_layout(
        xaxis_title=x.replace("_", " ").title(),
        yaxis_title="₹ (Lacs)",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor='#2a3a6c')
    return fig

def create_month_year_filter(df):
    """Create month-year filters"""
    with st.sidebar.expander("📆 Date Range", expanded=True):
        years = sorted(df['year'].unique().tolist())
        from_year = st.selectbox("From Year", years)
        to_year = st.selectbox("To Year", [y for y in years if y >= from_year])
        from_month = st.selectbox("From Month", MONTHS)
        to_month = st.selectbox("To Month", 
                                [m for m in MONTHS if MONTH_ORDER[m] >= MONTH_ORDER[from_month]])
    
    return from_year, to_year, from_month, to_month

def create_data_preview(df, title):
    """Create a styled data preview"""
    with st.expander(f"📋 {title}", expanded=False):
        st.dataframe(df.style.format({col: "₹{:,.2f}" for col in KPI_COLS}))

# ==============================================
# ANALYSIS FUNCTIONS
# ==============================================

def yearly_analysis(df):
    """Yearly analysis page"""
    with st.sidebar.expander("🔍 Analysis Filters", expanded=True):
        years = sorted(df['year'].unique().tolist())
        sectors = sorted(df['sector'].unique().tolist())
        vessels = sorted(df['vessel'].unique().tolist())
        
        # Filters
        from_year = st.selectbox("From Year", years)
        to_year = st.selectbox("To Year", [y for y in years if y >= from_year])
        selected_sector = st.selectbox("Select Sector", sectors)
        selected_vessel = st.selectbox("Select Vessel", vessels)

    # Data processing
    grouped_df = df.groupby(['year', 'sector', 'vessel'])[KPI_COLS].sum().reset_index()
    
    # Filter data
    filtered_df = grouped_df[
        (grouped_df['year'] >= from_year) &
        (grouped_df['year'] <= to_year) &
        (grouped_df['sector'] == selected_sector) &
        (grouped_df['vessel'] == selected_vessel)
    ]
    
    # Display results
    st.markdown("### 📋 Filtered Yearly KPI Data")
    if filtered_df.empty:
        st.warning("⚠️ No data matches your filter criteria.")
        return
    
    create_data_preview(filtered_df, "Show Filtered Data")
    
    # KPI Summary
    display_kpi_summary(filtered_df, KPI_COLS)
    
    # Yearly KPI Trends
    st.markdown("### 📈 Yearly KPI Trends")
    viz_type = st.radio("Chart Type", ["Multi-KPI Bar Chart", "Table"], horizontal=True, index=0)
    
    if viz_type == "Multi-KPI Bar Chart":
        long_df = pd.melt(filtered_df, id_vars=["year"], value_vars=KPI_COLS, 
                          var_name="KPI", value_name="Value")
        fig = create_bar_chart(long_df, x="year", y="Value", color="KPI")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(filtered_df.style.format({col: "₹{:,.2f}" for col in KPI_COLS}))
    
    # Individual KPI charts
    st.markdown("### 📊 Individual KPI Analysis")
    for kpi in KPI_COLS:
        st.markdown(f"#### {kpi} over Years")
        
        # Create tabs for each KPI
        tab1, tab2 = st.tabs(["📊 Chart", "📋 Data"])
        
        kpi_df = filtered_df[["year", kpi]].groupby("year").sum().reset_index()
        
        with tab1:
            if "formatted_text" not in kpi_df.columns:
                kpi_df["formatted_text"] = kpi_df[kpi].apply(
                    lambda val: f"₹{abs(val):,.2f} ({'Debit' if val >= 0 else 'Credit'})"
                )
            fig = px.bar(
                kpi_df, 
                x="year", 
                y=kpi,
                text="formatted_text",
                color_discrete_sequence=[KPI_COLORS[kpi]]
            )
            fig.update_layout(
                title=f"{kpi} Trend",
                xaxis_title="Year",
                yaxis_title="₹ (Lacs)",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white')
            )
            st.plotly_chart(fig, use_container_width=True)
            
        with tab2:
            st.dataframe(kpi_df.style.format({kpi: "₹{:,.2f}"}))

def monthly_analysis(df):
    """Monthly analysis page"""
    from_year, to_year, from_month, to_month = create_month_year_filter(df)
    
    with st.sidebar.expander("🔍 Additional Filters", expanded=True):
        sectors = sorted(df['sector'].unique().tolist())
        vessels = sorted(df['vessel'].unique().tolist())
        selected_sector = st.selectbox("Select Sector", sectors)
        selected_vessel = st.selectbox("Select Vessel", vessels)

    # Data processing
    grouped_df = df.groupby(['year', 'month', 'sector', 'vessel'])[KPI_COLS].sum().reset_index()
    grouped_df["month_index"] = grouped_df["month"].map(MONTH_ORDER)
    grouped_df["month_year"] = (
        grouped_df["year"].astype(str) + "-" +
        grouped_df["month_index"].astype(str).str.zfill(2)
    )
    grouped_df["month_year"] = pd.to_datetime(
        grouped_df["month_year"], format="%Y-%m"
    ).dt.strftime('%b %Y')
    
    # Create calendar month index for proper sorting
    calendar_month_map = {m: i for i, m in enumerate(MONTHS, 1)}
    grouped_df["calendar_month_index"] = grouped_df["month"].map(calendar_month_map)
    grouped_df = grouped_df.sort_values(by=["year", "calendar_month_index"])

    # Filter data
    start_date = pd.to_datetime(f"{from_year}-{MONTH_ORDER[from_month]:02}")
    end_date = pd.to_datetime(f"{to_year}-{MONTH_ORDER[to_month]:02}")

    filtered_df = grouped_df[
        (pd.to_datetime(grouped_df["year"].astype(str) + "-" + 
         grouped_df["calendar_month_index"].astype(str).str.zfill(2), format="%Y-%m") >= start_date) &
        (pd.to_datetime(grouped_df["year"].astype(str) + "-" + 
         grouped_df["calendar_month_index"].astype(str).str.zfill(2), format="%Y-%m") <= end_date) &
        (grouped_df['sector'] == selected_sector) &
        (grouped_df['vessel'] == selected_vessel)
    ]

    filtered_df = filtered_df.sort_values(by=["year", "calendar_month_index"])
    display_df = filtered_df.drop(columns=["calendar_month_index"])

    # Display results
    st.markdown("### 📋 Filtered Monthly KPI Data")
    if display_df.empty:
        st.warning("⚠️ No data matches your filter criteria.")
        return
    
    create_data_preview(display_df, "Show Filtered Monthly Data")
    
    # KPI Summary
    display_kpi_summary(filtered_df, KPI_COLS)
    
    # Monthly KPI Trends
    st.markdown("### 📈 Monthly KPI Trends")
    viz_type = st.radio("Chart Type", ["Multi-KPI Bar Chart", "Table"], horizontal=True, index=0)

    if viz_type == "Multi-KPI Bar Chart":
        long_df = pd.melt(
            filtered_df,
            id_vars=["year", "month", "month_year"],
            value_vars=KPI_COLS,
            var_name="KPI",
            value_name="Value"
        )
        fig = create_bar_chart(long_df, x="month_year", y="Value", color="KPI")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(filtered_df.style.format({col: "₹{:,.2f}" for col in KPI_COLS}))

    # Individual KPI charts
    st.markdown("### 📊 Individual KPI Analysis")
    for kpi in KPI_COLS:
        st.markdown(f"#### {kpi} over Months")
        
        # Create tabs for each KPI
        tab1, tab2 = st.tabs(["📊 Chart", "📋 Data"])
        
        kpi_df = filtered_df[["month", "year", "month_year", kpi]].copy()
        kpi_df["month_index"] = kpi_df["month"].map(MONTH_ORDER)
        kpi_df = kpi_df.sort_values(by=["year", "month_index"])

        with tab1:
            if "formatted_text" not in kpi_df.columns:
                kpi_df["formatted_text"] = kpi_df[kpi].apply(
                    lambda val: f"₹{abs(val):,.2f} ({'Debit' if val >= 0 else 'Credit'})"
                )
            fig = px.bar(
                kpi_df,
                x="month_year",
                y=kpi,
                text="formatted_text",
                color_discrete_sequence=[KPI_COLORS[kpi]]
            )
            fig.update_layout(
                title=f"{kpi} Monthly Trend",
                xaxis_title="Month-Year",
                yaxis_title="₹ (Lacs)",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white')
            )
            st.plotly_chart(fig, use_container_width=True)
            
        with tab2:
            st.dataframe(kpi_df.style.format({kpi: "₹{:,.2f}"}))

def quarterly_analysis(df):
    """Quarterly analysis page"""
    with st.sidebar.expander("🔍 Analysis Filters", expanded=True):
        years = sorted(df['year'].unique().tolist())
        sectors = sorted(df['sector'].unique().tolist())
        vessels = sorted(df['vessel'].unique().tolist())
        
        # Add quarter column
        df["quarter"] = df["month"].map(QUARTER_MAP)
        
        # Filters
        from_year = st.selectbox("From Year", years, key="q_from_year")
        to_year = st.selectbox("To Year", [y for y in years if y >= from_year], key="q_to_year")
        selected_quarters = st.multiselect("Select Quarters", 
                                          ["Q1", "Q2", "Q3", "Q4"], 
                                          default=["Q1", "Q2", "Q3", "Q4"])
        selected_sector = st.selectbox("Select Sector", sectors, key="q_sector")
        selected_vessel = st.selectbox("Select Vessel", vessels, key="q_vessel")

    # Data processing
    grouped_df = df.groupby(["year", "quarter", "sector", "vessel"])[KPI_COLS].sum().reset_index()
    grouped_df["quarter_year"] = grouped_df["quarter"] + " " + grouped_df["year"].astype(str)
    
    # Filter data
    filtered_df = grouped_df[
        (grouped_df["year"] >= from_year) &
        (grouped_df["year"] <= to_year) &
        (grouped_df["quarter"].isin(selected_quarters)) &
        (grouped_df["sector"] == selected_sector) &
        (grouped_df["vessel"] == selected_vessel)
    ]

    # Display results
    st.markdown("### 📋 Filtered Quarterly KPI Data")
    if filtered_df.empty:
        st.warning("⚠️ No data matches your filter criteria.")
        return
    
    create_data_preview(filtered_df, "Show Filtered Quarterly Data")
    
    # KPI Summary
    display_kpi_summary(filtered_df, KPI_COLS)
    
    # Quarterly KPI Trends
    st.markdown("### 📈 Quarterly KPI Trends")
    viz_type = st.radio("Chart Type", ["Multi-KPI Bar Chart", "Table"], horizontal=True, index=0)

    if viz_type == "Multi-KPI Bar Chart":
        long_df = pd.melt(
            filtered_df,
            id_vars=["quarter_year"],
            value_vars=KPI_COLS,
            var_name="KPI",
            value_name="Value"
        )
        fig = create_bar_chart(long_df, x="quarter_year", y="Value", color="KPI")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(filtered_df.style.format({col: "₹{:,.2f}" for col in KPI_COLS}))

    # Individual KPI charts
    st.markdown("### 📊 Individual KPI Analysis")
    for kpi in KPI_COLS:
        st.markdown(f"#### {kpi} over Quarters")
        
        # Create tabs for each KPI
        tab1, tab2 = st.tabs(["📊 Chart", "📋 Data"])
        
        kpi_df = filtered_df[["year", "quarter", kpi]].copy()
        kpi_df["quarter_year"] = kpi_df["quarter"] + " " + kpi_df["year"].astype(str)

        with tab1:
            if "formatted_text" not in kpi_df.columns:
                kpi_df["formatted_text"] = kpi_df[kpi].apply(
                    lambda val: f"₹{abs(val):,.2f} ({'Debit' if val >= 0 else 'Credit'})"
                )
            fig = px.bar(
                kpi_df, 
                x="quarter_year", 
                y=kpi,
                text="formatted_text",
                color_discrete_sequence=[KPI_COLORS[kpi]]
            )
            fig.update_layout(
                title=f"{kpi} Quarterly Trend",
                xaxis_title="Quarter",
                yaxis_title="₹ (Lacs)",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='white')
            )
            st.plotly_chart(fig, use_container_width=True)
            
        with tab2:
            st.dataframe(kpi_df.style.format({kpi: "₹{:,.2f}"}))

def sector_wise_analysis(df):
    """Sector-wise analysis page"""
    from_year, to_year, from_month, to_month = create_month_year_filter(df)
    
    with st.sidebar.expander("🔍 Additional Filters", expanded=True):
        sectors = sorted(df['sector'].unique().tolist())
        selected_sector = st.multiselect("Select Sector(s)", sectors, default=sectors[:3])

    # Data processing
    grouped_df = df.groupby(['year', 'sector', 'month'])[KPI_COLS].sum().reset_index()
    grouped_df["month_index"] = grouped_df["month"].map(MONTH_ORDER)
    grouped_df = grouped_df.sort_values(by=["year", "month_index"])
    
    # Filter data
    from_index = MONTH_ORDER[from_month]
    to_index = MONTH_ORDER[to_month]

    filtered_df = grouped_df[
        (grouped_df["year"] >= from_year) &
        (grouped_df["year"] <= to_year) &
        (grouped_df["month_index"] >= from_index) &
        (grouped_df["month_index"] <= to_index) &
        (grouped_df["sector"].isin(selected_sector))
    ]

    display_df = filtered_df.drop(columns="month_index")

    # Display results
    st.markdown("### 📋 Filtered Sector-wise KPI Data")
    if display_df.empty:
        st.warning("⚠️ No data matches your filter criteria.")
        return
    
    create_data_preview(display_df, "Show Filtered Sector-wise Data")
    
    # KPI Summary
    display_kpi_summary(filtered_df, KPI_COLS)
    
    # Sector-wise KPI Trends
    st.markdown("### 📈 Sector-wise KPI Trends")
    viz_type = st.radio("Chart Type", ["Multi-KPI Bar Chart", "Table"], horizontal=True, index=0)

    if viz_type == "Multi-KPI Bar Chart":
        display_df["month_index"] = display_df["month"].map(MONTH_ORDER)
        display_df["month_year"] = (
            display_df["year"].astype(str) + "-" + 
            display_df["month_index"].astype(str).str.zfill(2)
        )
        display_df["month_year"] = pd.to_datetime(
            display_df["month_year"], format="%Y-%m"
        ).dt.strftime('%b %Y')

        long_df = pd.melt(
            display_df, 
            id_vars=["month_year", "sector"], 
            value_vars=KPI_COLS, 
            var_name="KPI", 
            value_name="Value"
        )

        fig = create_bar_chart(
            long_df,
            x="month_year",
            y="Value",
            color="KPI",
            facet_col="sector",
            barmode="stack"
        )
        fig.update_traces(textposition="inside")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(display_df.style.format({col: "₹{:,.2f}" for col in KPI_COLS}))

    # Individual KPI charts
    st.markdown("### 📊 Individual KPI Analysis")
    for kpi in KPI_COLS:
        st.markdown(f"#### {kpi} by Sector")
        
        # Create tabs for each KPI
        tab1, tab2 = st.tabs(["📊 Chart", "📋 Data"])
        
        kpi_df = display_df[["month", "year", "sector", kpi]].copy()
        kpi_df["month_index"] = kpi_df["month"].map(MONTH_ORDER)
        kpi_df = kpi_df.sort_values(by=["year", "month_index", "sector"])
        kpi_df["month_year"] = (
            kpi_df["year"].astype(str) + "-" + 
            kpi_df["month_index"].astype(str).str.zfill(2)
        )
        kpi_df["month_year"] = pd.to_datetime(
            kpi_df["month_year"], format="%Y-%m"
        ).dt.strftime('%b %Y')

        with tab1:
            fig = create_bar_chart(
                kpi_df, 
                x="month_year", 
                y=kpi,
                color="sector"
            )
            st.plotly_chart(fig, use_container_width=True)
            
        with tab2:
            st.dataframe(kpi_df.style.format({kpi: "₹{:,.2f}"}))

def vessel_wise_analysis(df):
    """Vessel-wise analysis page"""
    from_year, to_year, from_month, to_month = create_month_year_filter(df)
    
    with st.sidebar.expander("🔍 Additional Filters", expanded=True):
        vessels = sorted(df['vessel'].unique().tolist())
        selected_vessel = st.multiselect("Select Vessel(s)", vessels, default=vessels[:3])

    # Data processing
    grouped_df = df.groupby(['year', 'vessel', 'month'])[KPI_COLS].sum().reset_index()
    grouped_df["month_index"] = grouped_df["month"].map(MONTH_ORDER)
    grouped_df = grouped_df.sort_values(by=["year", "month_index"])
    
    # Filter data
    from_index = MONTH_ORDER[from_month]
    to_index = MONTH_ORDER[to_month]

    filtered_df = grouped_df[
        (grouped_df["year"] >= from_year) &
        (grouped_df["year"] <= to_year) &
        (grouped_df["month_index"] >= from_index) &
        (grouped_df["month_index"] <= to_index) &
        (grouped_df["vessel"].isin(selected_vessel))
    ]

    display_df = filtered_df.drop(columns="month_index")

    # Display results
    st.markdown("### 📋 Filtered Vessel-wise KPI Data")
    if display_df.empty:
        st.warning("⚠️ No data matches your filter criteria.")
        return
    
    create_data_preview(display_df, "Show Filtered Vessel-wise Data")
    
    # KPI Summary
    display_kpi_summary(filtered_df, KPI_COLS)
    
    # Vessel-wise KPI Trends
    st.markdown("### 📈 Vessel-wise KPI Trends")
    viz_type = st.radio("Chart Type", ["Multi-KPI Bar Chart", "Table"], horizontal=True, index=0)

    if viz_type == "Multi-KPI Bar Chart":
        long_df = pd.melt(
            display_df, 
            id_vars=["year", "month", "vessel"], 
            value_vars=KPI_COLS, 
            var_name="KPI", 
            value_name="Value"
        )
        long_df["month_year"] = long_df["year"].astype(str) + "-" + long_df["month"]
        
        fig = create_bar_chart(
            long_df,
            x="month_year",
            y="Value",
            color="KPI",
            facet_col="vessel",
            facet_col_wrap=4,
            barmode="stack"
        )
        fig.update_layout(margin=dict(t=40), height=600)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(display_df.style.format({col: "₹{:,.2f}" for col in KPI_COLS}))

    # Individual KPI charts
    st.markdown("### 📊 Individual KPI Analysis")
    for kpi in KPI_COLS:
        st.markdown(f"#### {kpi} by Vessel")
        
        # Create tabs for each KPI
        tab1, tab2 = st.tabs(["📊 Chart", "📋 Data"])
        
        kpi_df = display_df[["month", "year", "vessel", kpi]].copy()
        kpi_df["month_index"] = kpi_df["month"].map(MONTH_ORDER)
        kpi_df = kpi_df.sort_values(by=["year", "month_index", "vessel"])
        kpi_df["month_year"] = (
            kpi_df["year"].astype(str) + "-" + 
            kpi_df["month_index"].astype(str).str.zfill(2)
        )
        kpi_df["month_year"] = pd.to_datetime(
            kpi_df["month_year"], format="%Y-%m"
        ).dt.strftime('%b %Y')

        with tab1:
            fig = create_bar_chart(
                kpi_df, 
                x="month_year", 
                y=kpi,
                color="vessel"
            )
            st.plotly_chart(fig, use_container_width=True)
            
        with tab2:
            st.dataframe(kpi_df.style.format({kpi: "₹{:,.2f}"}))

# ==============================================
# MAIN APPLICATION
# ==============================================

def main():
    # Apply custom CSS
    apply_custom_css()
    
    # Initialize database engine
    engine = create_db_engine()
    
    # Page configuration
    st.set_page_config(
        page_title="SCI KPI Dashboard", 
        layout="wide",
        page_icon="⛴️"
    )
    
    # Main header
    st.markdown("""
    <div class="header">
        <h1>Shipping Corporation of India</h1>
        <h2>KPI Dashboard: Analyze Total Income, DOE, GOP, IOE, and PBT</h2>
    </div>
    """, unsafe_allow_html=True)
    
    # Navigation
    with st.sidebar:
        st.markdown("""
        <div style="text-align:center; margin-bottom:20px;">
            <h2>⛴️ Navigation</h2>
        </div>
        """, unsafe_allow_html=True)
        
        page = st.radio("Choose Action", ["📤 Upload CSV", "📊 KPI Dashboard"])
    
    if page == "📤 Upload CSV":
        st.markdown("## 📤 Upload Weekly KPI CSV File")
        uploaded_file = st.file_uploader("Upload your KPI CSV", type=["csv"])
    
        if uploaded_file is not None:
            try:
                df_uploaded = load_cleaned_data(uploaded_file)
                df_uploaded.to_sql("kpi_data", con=engine, if_exists='append', index=False)
                st.success(f"✅ Uploaded and saved {len(df_uploaded)} rows to the database.")
                
                # Display uploaded data with styling
                with st.expander("View Uploaded Data", expanded=True):
                    st.dataframe(df_uploaded.style.format({col: "₹{:,.2f}" for col in KPI_COLS}))
            except Exception as e:
                st.error(f"❌ Failed to process: {e}")
        return
    
    # Main dashboard
    # Load data
    df_cleaned = load_and_clean_data(engine)
    
    # Report type selection
    st.markdown("### 📁 Select Report Type to Continue")
    report_type = st.selectbox("Choose analysis type:", [
        "Select...", 
        "📅 Yearly Analysis", 
        "📆 Monthly Analysis", 
        "🔄 Quarterly Analysis",
        "🌐 Sector-wise Analysis", 
        "🚢 Vessel-wise Analysis"
    ])
    
    # Display stats summary
    st.markdown("### 📈 Data Overview")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Records", len(df_cleaned))
    col2.metric("Unique Vessels", df_cleaned['vessel'].nunique())
    col3.metric("Data Coverage", 
                f"{df_cleaned['year'].min()} - {df_cleaned['year'].max()}")
    
    # Route to analysis
    if report_type == "📅 Yearly Analysis":
        yearly_analysis(df_cleaned)
    elif report_type == "📆 Monthly Analysis":
        monthly_analysis(df_cleaned)
    elif report_type == "🔄 Quarterly Analysis":
        quarterly_analysis(df_cleaned)
    elif report_type == "🌐 Sector-wise Analysis":
        sector_wise_analysis(df_cleaned)
    elif report_type == "🚢 Vessel-wise Analysis":
        vessel_wise_analysis(df_cleaned)

if __name__ == "__main__":
    main()