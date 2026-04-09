import pandas as pd
import duckdb
import os
from datetime import datetime

def transform_data():
    df = pd.read_csv("stackvitals_raw_data.csv")

    df.fillna({
        "Minified_Size_Bytes": 0,
        "Gzipped_Size_Bytes": 0,
        "Download_Time_3G_ms": 0,
        "Open_Issues": 0,
        "Stars": 0,
        "Forks": 0,
        "Last_Commit_Date": datetime.now().isoformat()
    }, inplace=True)

    min_size = df['Gzipped_Size_Bytes'].min()
    max_size = df['Gzipped_Size_Bytes'].max()

    if max_size - min_size == 0:
        df['Normalized_Size'] = 1.0
    else:
        df['Normalized_Size'] = (df['Gzipped_Size_Bytes'] - min_size) / (max_size - min_size)

    df['Size_Score'] = 1.0 - df['Normalized_Size']

    current_date = datetime.now()
    df['Last_Commit_Date'] = pd.to_datetime(df['Last_Commit_Date']).dt.tz_localize(None)
    df['Days_Since_Last_Commit'] = (current_date - df['Last_Commit_Date']).dt.days

    max_days = df['Days_Since_Last_Commit'].max()
    if max_days == 0:
        df['Commit_Recency_Score'] = 1.0
    else:
        df['Commit_Recency_Score'] = 1.0 - (df['Days_Since_Last_Commit'] / max_days)

    df['Issue_Resolution_Rate'] = 0.8
    df['Health_Score'] = (0.4 * df['Size_Score']) + (0.35 * df['Commit_Recency_Score']) + (0.25 * df['Issue_Resolution_Rate'])

    df['Package_ID'] = range(1, len(df) + 1)
    df['Time_ID'] = df['Package_ID']
    df['Maintenance_ID'] = df['Package_ID']

    dim_package = df[['Package_ID', 'Package_Name', 'License', 'Version', 'Dependencies_Count']]
    
    dim_time = pd.DataFrame({
        'Time_ID': df['Time_ID'],
        'Extraction_Date': current_date.strftime('%Y-%m-%d'),
        'Year': current_date.year,
        'Quarter': (current_date.month - 1) // 3 + 1
    })
    
    dim_maintenance = df[['Maintenance_ID', 'Open_Issues', 'Stars', 'Forks', 'Last_Commit_Date']]

    fact_package_metrics = df[['Package_ID', 'Time_ID', 'Maintenance_ID', 'Gzipped_Size_Bytes', 'Download_Time_3G_ms', 'Days_Since_Last_Commit', 'Health_Score']]

    return dim_package, dim_time, dim_maintenance, fact_package_metrics

def load_to_motherduck(dim_package, dim_time, dim_maintenance, fact_package_metrics):
    md_token = os.environ.get("MOTHERDUCK_TOKEN")
    con = duckdb.connect(f'md:?motherduck_token={md_token}')

    con.execute("CREATE OR REPLACE TABLE dim_package AS SELECT * FROM dim_package")
    con.execute("CREATE OR REPLACE TABLE dim_time AS SELECT * FROM dim_time")
    con.execute("CREATE OR REPLACE TABLE dim_maintenance AS SELECT * FROM dim_maintenance")
    con.execute("CREATE OR REPLACE TABLE fact_package_metrics AS SELECT * FROM fact_package_metrics")

    print("Star Schema successfully pushed to MotherDuck.")

if __name__ == "__main__":
    dp, dt, dm, fpm = transform_data()
    load_to_motherduck(dp, dt, dm, fpm)