from load_data import load_data
from ETL import extract_transform_load
from Patterns import identify_patterns
from Dashboard import create_dashboard

db_config = {
    'username': 'postgres',
    'password': '89136157439aA',
    'host': 'localhost',
    'port': '5432',
    'dbname': 'PracticeDatabase'
}

csv_file = 'archive.csv'
load_data(csv_file, db_config)

df = extract_transform_load(db_config)

suspicious_transactions = identify_patterns(df)

app = create_dashboard(suspicious_transactions, db_config)
app.run_server(debug=True)
