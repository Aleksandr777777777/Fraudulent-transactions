from datetime import datetime

def identify_patterns(df):
    def is_night_activity(row):
        transaction_time = row['date'].time()
        return transaction_time >= datetime.strptime('03:00:00', '%H:%M:%S').time() and transaction_time <= datetime.strptime('05:00:00', '%H:%M:%S').time()

    def is_high_frequency(df, address_column, threshold=7):
        address_counts = df[address_column].value_counts()
        return df[address_column].isin(address_counts[address_counts > threshold].index)

    def is_patterned_behavior(df):
        df['time_diff'] = df['date'].diff().dt.total_seconds().fillna(0)
        df['prev_time_diff'] = df['time_diff'].shift()
        df['patterned_behavior'] = (df['time_diff'] == df['prev_time_diff']) & (df['time_diff'] != 0)
        return df['patterned_behavior']

    def is_large_amount(row, client_second_largest):
        return row['amount'] > (1.5 * client_second_largest)

    def is_high_frequency_clicks(df, time_threshold=10):
        df = df.sort_values(by=['client', 'date'])
        df['time_diff'] = df.groupby('client')['date'].diff().dt.total_seconds().fillna(float('inf'))
        df['high_frequency_clicks'] = df.groupby('client')['time_diff'].transform(lambda x: (x < time_threshold).any())
        return df['high_frequency_clicks']
    
    def check_consistent_client_info(df):
        inconsistent_clients = []

        grouped = df.groupby('client')

        for client_id, group in grouped:
            birth_dates = group['date_of_birth'].unique()
            passport_expirations = group['passport_valid_to'].unique()

            if len(birth_dates) > 1 or len(passport_expirations) > 1:
                inconsistent_clients.append(client_id)

        return inconsistent_clients
    

    df['night_activity'] = df.apply(is_night_activity, axis=1)
    df['high_frequency'] = is_high_frequency(df, 'address')
    df['patterned_behavior'] = is_patterned_behavior(df)

    client_max = df.groupby('client')['amount'].apply(lambda x: sorted(x)[-2] if len(x) > 1 else float('inf')).reset_index()
    df = df.merge(client_max.rename(columns={'amount': 'client_second_largest'}), on='client')

    df['large_amount'] = df.apply(lambda row: is_large_amount(row, row['client_second_largest']), axis=1)
    df['high_frequency_clicks'] = is_high_frequency_clicks(df)
    
    inconsistent_clients = check_consistent_client_info(df)
    df['inconsistent_client_info'] = df['client'].isin(inconsistent_clients)

    df['pattern_matches'] = df[['night_activity', 'high_frequency', 'patterned_behavior', 'large_amount', 'high_frequency_clicks', 'inconsistent_client_info']].sum(axis=1)
    suspicious_transactions = df[df['pattern_matches'] >= 1]

    return suspicious_transactions