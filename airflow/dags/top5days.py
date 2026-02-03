from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import pandas as pd
from pathlib import Path

DATA_DIR = Path("/opt/airflow/data")
OUTPUT_DIR = Path("/opt/airflow/output")
INPUT_FILE = DATA_DIR / "IOT-temp.csv"

with DAG(
    'top5days',
    start_date=datetime(2026, 2, 3),
    schedule_interval=None,
    catchup=False,
) as dag:

    @task
    def process_temperature():
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        
        df = pd.read_csv(INPUT_FILE)
        df = df[df['out/in'].str.strip().str.lower() == 'in']
        
        df['noted_date'] = pd.to_datetime(df['noted_date'], dayfirst=True).dt.normalize()
        df['year'] = df['noted_date'].dt.year
        
        p5, p95 = df['temp'].quantile([0.05, 0.95])
        df = df[(df['temp'] >= p5) & (df['temp'] <= p95)]
        
        days = (df.groupby(['year', 'noted_date'])
                  .agg(
                      avg_temp=('temp', 'mean'),
                      min_temp=('temp', 'min'),
                      max_temp=('temp', 'max'),
                      measurements=('temp', 'count')
                  )
                  .round(2)
                  .reset_index())
        
        top5hottest = (days.sort_values(['year', 'avg_temp'], ascending=[True, False])
                          .groupby('year')
                          .head(5)
                          .reset_index(drop=True))
        
        top5coldest = (days.sort_values(['year', 'avg_temp'], ascending=[True, True])
                          .groupby('year')
                          .head(5)
                          .reset_index(drop=True))
        
        days.to_csv(OUTPUT_DIR / "days.csv", index=False)
        top5hottest.to_csv(OUTPUT_DIR / "top5hottest.csv", index=False)
        top5coldest.to_csv(OUTPUT_DIR / "top5coldest.csv", index=False)

    process_temperature()