import base64
import io
from dash import Dash, dcc, html, dash_table, Input, Output, State, no_update
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from load_data import load_data
from ETL import extract_transform_load
from Patterns import identify_patterns

def create_dashboard(initial_data, db_config):
    app = Dash(__name__)

    def create_figures(data):
        fig_night_activity = px.histogram(data[data['night_activity']], x='date', y='amount', title='Night Activity Transactions', 
                                          marginal="box", nbins=24)
        fig_high_frequency = px.histogram(data[data['high_frequency']], x='date', y='amount', title='High Frequency Transactions', 
                                          marginal="box", nbins=24)
        fig_patterned_behavior = px.box(data[data['patterned_behavior']], x='date', y='amount', title='Patterned Behavior Transactions')
        fig_large_amount = px.box(data[data['large_amount']], x='date', y='amount', title='Large Amount Transactions')
        fig_high_frequency_clicks = px.histogram(data[data['high_frequency_clicks']], x='date', y='amount', title='High Frequency Clicks Transactions', 
                                                 marginal="box", nbins=24)
        fig_matched_patterns = px.scatter(data[data['pattern_matches'] >= 3], x='date', y='amount', color='pattern_matches', 
                                          title='Transactions with 3 or More Matching Patterns')
        fig_inconsistent_details = px.histogram(data[data['inconsistent_client_info']], x='date', y='amount', title='Inconsistent Client Details Transactions', 
                                                marginal="box", nbins=24)
        return fig_night_activity, fig_high_frequency, fig_patterned_behavior, fig_large_amount, fig_high_frequency_clicks, fig_matched_patterns, fig_inconsistent_details

    fig_night_activity, fig_high_frequency, fig_patterned_behavior, fig_large_amount, fig_high_frequency_clicks, fig_matched_patterns, fig_inconsistent_details = create_figures(initial_data)

    app.layout = html.Div(children=[
        html.H1(children='Suspicious Transactions Dashboard'),

        dcc.Upload(
            id='upload-data',
            children=html.Div([
                'Drag and Drop or ',
                html.A('Select Files')
            ]),
            style={
                'width': '100%',
                'height': '60px',
                'lineHeight': '60px',
                'borderWidth': '1px',
                'borderStyle': 'dashed',
                'borderRadius': '5px',
                'textAlign': 'center',
                'margin': '10px'
            },
            multiple=False
        ),
        html.Div(id='output-data-upload'),

        html.H2(children='Night Activity Transactions'),
        dcc.Graph(id='night-activity-transactions-graph', figure=fig_night_activity),
        dash_table.DataTable(id='night-activity-transactions-table',
                             columns=[{'name': i, 'id': i} for i in initial_data.columns],
                             data=initial_data[initial_data['night_activity']].to_dict('records'),
                             page_size=10,
                             style_data_conditional=[]),

        html.H2(children='High Frequency Transactions'),
        dcc.Graph(id='high-frequency-transactions-graph', figure=fig_high_frequency),
        dash_table.DataTable(id='high-frequency-transactions-table',
                             columns=[{'name': i, 'id': i} for i in initial_data.columns],
                             data=initial_data[initial_data['high_frequency']].to_dict('records'),
                             page_size=10,
                             style_data_conditional=[]),

        html.H2(children='Patterned Behavior Transactions'),
        dcc.Graph(id='patterned-behavior-transactions-graph', figure=fig_patterned_behavior),
        dash_table.DataTable(id='patterned-behavior-transactions-table',
                             columns=[{'name': i, 'id': i} for i in initial_data.columns],
                             data=initial_data[initial_data['patterned_behavior']].to_dict('records'),
                             page_size=10,
                             style_data_conditional=[]),

        html.H2(children='Large Amount Transactions'),
        dcc.Graph(id='large-amount-transactions-graph', figure=fig_large_amount),
        dash_table.DataTable(id='large-amount-transactions-table',
                             columns=[{'name': i, 'id': i} for i in initial_data.columns],
                             data=initial_data[initial_data['large_amount']].to_dict('records'),
                             page_size=10,
                             style_data_conditional=[]),

        html.H2(children='High Frequency Clicks Transactions'),
        dcc.Graph(id='high-frequency-clicks-transactions-graph', figure=fig_high_frequency_clicks),
        dash_table.DataTable(id='high-frequency-clicks-transactions-table',
                             columns=[{'name': i, 'id': i} for i in initial_data.columns],
                             data=initial_data[initial_data['high_frequency_clicks']].to_dict('records'),
                             page_size=10,
                             style_data_conditional=[]),
        
        html.H2(children='Inconsistent Client Details Transactions'),
        dcc.Graph(id='inconsistent-details-transactions-graph', figure=fig_inconsistent_details),
        dash_table.DataTable(id='inconsistent-details-transactions-table',
                             columns=[{'name': i, 'id': i} for i in initial_data.columns],
                             data=initial_data[initial_data['inconsistent_client_info']].to_dict('records'),
                             page_size=10,
                             style_data_conditional=[]),
        
        html.H2(children='Transactions with 3 or More Matching Patterns'),
        dcc.Graph(id='matched-patterns-transactions-graph', figure=fig_matched_patterns),
        dash_table.DataTable(id='matched-patterns-transactions-table',
                             columns=[{'name': i, 'id': i} for i in initial_data.columns],
                             data=initial_data[initial_data['pattern_matches'] >= 3].to_dict('records'),
                             page_size=10,
                             style_data_conditional=[])

        
    ])

    @app.callback(
    [Output('night-activity-transactions-graph', 'figure'),
     Output('high-frequency-transactions-graph', 'figure'),
     Output('patterned-behavior-transactions-graph', 'figure'),
     Output('large-amount-transactions-graph', 'figure'),
     Output('high-frequency-clicks-transactions-graph', 'figure'),
     Output('matched-patterns-transactions-graph', 'figure'),
     Output('inconsistent-details-transactions-graph', 'figure'),
     Output('night-activity-transactions-table', 'data'),
     Output('high-frequency-transactions-table', 'data'),
     Output('patterned-behavior-transactions-table', 'data'),
     Output('large-amount-transactions-table', 'data'),
     Output('high-frequency-clicks-transactions-table', 'data'),
     Output('matched-patterns-transactions-table', 'data'),
     Output('inconsistent-details-transactions-table', 'data'),
     Output('night-activity-transactions-table', 'style_data_conditional'),
     Output('high-frequency-transactions-table', 'style_data_conditional'),
     Output('patterned-behavior-transactions-table', 'style_data_conditional'),
     Output('large-amount-transactions-table', 'style_data_conditional'),
     Output('high-frequency-clicks-transactions-table', 'style_data_conditional'),
     Output('matched-patterns-transactions-table', 'style_data_conditional'),
     Output('inconsistent-details-transactions-table', 'style_data_conditional')],
    [Input('upload-data', 'contents')],
    [State('upload-data', 'filename'), State('upload-data', 'last_modified')]
    )
    def update_output(content, filename, last_modified):
        if content is not None:
            content_type, content_string = content.split(',')
            decoded = io.BytesIO(base64.b64decode(content_string))

            try:
                new_data = pd.read_csv(decoded, delimiter=';', on_bad_lines='skip')
            except pd.errors.ParserError as e:
                print(f"Ошибка парсинга CSV: {e}")
                return no_update

            print(new_data.head())

            engine = create_engine(f'postgresql://{db_config["username"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["dbname"]}')
            new_data.to_sql('Transactions', engine, if_exists='append', index=False)

            new_data['date'] = pd.to_datetime(new_data['date'])

            new_suspicious_transactions = identify_patterns(new_data)

            combined_suspicious_transactions = pd.concat([initial_data, new_suspicious_transactions])

            fig_night_activity, fig_high_frequency, fig_patterned_behavior, fig_large_amount, fig_high_frequency_clicks, fig_matched_patterns, fig_inconsistent_details = create_figures(combined_suspicious_transactions)

            new_data_ids = new_data['id_transaction'].tolist()

            style_data_conditional = [{
                'if': {'filter_query': f'{{id_transaction}} = {transaction_id}', 'column_id': col},
                'backgroundColor': 'yellow'
            } for transaction_id in new_data_ids for col in combined_suspicious_transactions.columns]

            return (fig_night_activity, fig_high_frequency, fig_patterned_behavior, fig_large_amount, fig_high_frequency_clicks, fig_matched_patterns, fig_inconsistent_details,
                    combined_suspicious_transactions[combined_suspicious_transactions['night_activity']].to_dict('records'),
                    combined_suspicious_transactions[combined_suspicious_transactions['high_frequency']].to_dict('records'),
                    combined_suspicious_transactions[combined_suspicious_transactions['patterned_behavior']].to_dict('records'),
                    combined_suspicious_transactions[combined_suspicious_transactions['large_amount']].to_dict('records'),
                    combined_suspicious_transactions[combined_suspicious_transactions['high_frequency_clicks']].to_dict('records'),
                    combined_suspicious_transactions[combined_suspicious_transactions['pattern_matches'] >= 3].to_dict('records'),
                    combined_suspicious_transactions[combined_suspicious_transactions['inconsistent_client_info']].to_dict('records'),
                    style_data_conditional, style_data_conditional, style_data_conditional, style_data_conditional, style_data_conditional, style_data_conditional, style_data_conditional)

        return no_update

    return app