import streamlit as st
import pandas as pd
import duckdb
import os

md_token = os.environ.get("MOTHERDUCK_TOKEN")
if md_token is None:
    try:
        md_token = st.secrets["MOTHERDUCK_TOKEN"]
    except:
        st.error("MotherDuck token not found. Please configure your secrets.")
        st.stop()

@st.cache_resource
def get_connection():
    return duckdb.connect(f'md:?motherduck_token={md_token}')

con = get_connection()

st.title("StackVitals: Dependency Health Analytics")
st.write("Upload or search for a package to evaluate its size, security, and maintenance health.")

package_input = st.text_input("Enter NPM Package Name (e.g., lodash, react, moment):", "lodash")

if st.button("Analyze Package Health"):
    query = f"""
    SELECT 
        p.Package_Name, 
        p.Version,
        p.Dependencies_Count,
        f.Health_Score, 
        f.Gzipped_Size_Bytes, 
        f.Days_Since_Last_Commit,
        m.Open_Issues
    FROM fact_package_metrics f
    JOIN dim_package p ON f.Package_ID = p.Package_ID
    JOIN dim_maintenance m ON f.Maintenance_ID = m.Maintenance_ID
    WHERE p.Package_Name = '{package_input}'
    """
    
    try:
        df = con.execute(query).df()
        
        if not df.empty:
            st.subheader(f"Results for {package_input}")
            st.dataframe(df)
            
            health = df['Health_Score'].iloc[0]
            size_kb = df['Gzipped_Size_Bytes'].iloc[0] / 1024
            days_since_commit = df['Days_Since_Last_Commit'].iloc[0]
            
            st.metric("Aggregate Health Score", f"{health:.2f} / 1.00")
            
            st.divider()
            st.subheader("Risk Alerts")
            
            if days_since_commit > 540:
                st.error(f"🧟‍♂️ Zombie Library Alert: This package has received no updates in {days_since_commit} days (over 18 months). Highly vulnerable to unpatched exploits.")
            else:
                st.success("✅ Maintenance: Package is actively maintained.")
                
            if size_kb > 150: 
                st.warning(f"📦 Bloat Detector: Package size is {size_kb:.2f} KB. Monitor for potential application bloat.")
            else:
                st.success(f"✅ Size: Optimal package footprint ({size_kb:.2f} KB).")
                
        else:
            st.warning("Package not found in the warehouse. Check if it was processed in the daily pipeline.")
            
    except Exception as e:
        st.error(f"Database error: {e}")