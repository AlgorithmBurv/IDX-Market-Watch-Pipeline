import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import time

DB_URI = 'postgresql://admin:rahasia123@127.0.0.1:5435/finance_db'

def get_engine():
    return create_engine(DB_URI)

def extract_stock_data(tickers):
    """
    EXTRACT: Ambil data untuk list saham tertentu (7 hari terakhir)
    """
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d') 
    
    print(f"Download data {start_date} to {end_date}")
    data = yf.download(tickers, start=start_date, end=end_date, group_by='ticker')
    return data

def transform_data(raw_data, tickers):
    """
    TRANSFORM: Ubah struktur data dari Wide Format ke Long Format
    agar mudah masuk database SQL.
    """
    all_data = []

    for ticker in tickers:
        if len(tickers) > 1:
            df = raw_data[ticker].copy()
        else:
            df = raw_data.copy()

        df = df.reset_index()
        
        df.columns = [c.lower() for c in df.columns] 
        df['ticker'] = ticker

        if 'date' in df.columns:
            df = df.rename(columns={'date': 'tanggal'})
        
        cols = ['tanggal', 'ticker', 'open', 'high', 'low', 'close', 'volume']
        df_final = df[[c for c in cols if c in df.columns]]
        
        all_data.append(df_final)

    combined_df = pd.concat(all_data)
    print(f"Finish, total rows: {len(combined_df)}")
    return combined_df

def load_to_db(df, engine):
    """
    LOAD: Masukkan ke PostgreSQL.
    """
    try:
        df.to_sql('stock_prices', engine, if_exists='replace', index=False)
        print("Succes save to 'stock_prices'!")
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    engine = get_engine()
    MY_PORTFOLIO = ['BBCA.JK', 'TLKM.JK', 'ASII.JK', 'GOTO.JK', 'BMRI.JK']
   
    while True:
        now = datetime.now()
        current_hour = now.hour

        if 9 <= current_hour <= 16:
            print(f"\nTime to update! Current time: {now}")
            
            raw_df = extract_stock_data(MY_PORTFOLIO)
            
            if not raw_df.empty:
                clean_df = transform_data(raw_df, MY_PORTFOLIO)
                load_to_db(clean_df, engine)
            
            print("Finished. Sleeping for 1 hour")
            time.sleep(3600) 
            
        else:
            print(f"Market is closed (Hour {current_hour}). Checking again later")
            time.sleep(3600)