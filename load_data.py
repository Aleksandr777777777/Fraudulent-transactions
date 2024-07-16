import pandas as pd
from sqlalchemy import create_engine

def load_data(csv_file, db_config):
    
    engine = create_engine(f'postgresql://{db_config["username"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["dbname"]}')

    df = pd.read_csv(csv_file)

    df.to_sql('Transactions', engine, if_exists='replace', index=False)