import pandas as pd
from sqlalchemy import create_engine

def extract_transform_load(db_config):
    
    engine = create_engine(f'postgresql://{db_config["username"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["dbname"]}')

    query = 'SELECT * FROM public."Transactions";'
    df = pd.read_sql(query, engine)
    
    df['date'] = pd.to_datetime(df['date'])
        
    return df