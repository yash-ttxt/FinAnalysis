import dash
from dash import dcc, html
from dash.dependencies import Output, Input
import dotenv
import pandas as pd
import plotly.express as px
import os

dotenv.load_dotenv()
app = dash.Dash(__name__)

data = pd.read_csv(os.getenv('RAW_BANKING_DATASET_PATH'))
data['Transaction Date'] = pd.to_datetime(data['Transaction Date'])

def get_transaction_volume_by_branch_plot(data):
    branch_transaction_volume = data.groupby('Branch ID').agg({'Transaction Amount': 'sum'}).rename(columns={'Transaction Amount': 'Transaction Volume'}).reset_index()
    plot = px.bar(branch_transaction_volume, x='Branch ID', y='Transaction Volume')
    plot.update_layout(title='Transaction Volume by Branch')
    plot.update_xaxes(title='Branch ID')
    plot.update_yaxes(title='Transaction Volume')
    plot.update_xaxes(range=[-1, len(data['Branch ID'].unique())+1], categoryorder='category descending')
    plot.update_yaxes(range=[0, branch_transaction_volume['Transaction Volume'].max() * 1.1])
    return plot

def get_10_customers_per_branch(data, branch_id):
    top_10_customers_per_branch = data[data["Branch ID"] == branch_id].groupby(['Email']).agg({'Transaction Amount': 'sum'}).rename(columns={'Transaction Amount': 'Transaction Volume'}).reset_index()
    top_10_customers_per_branch = top_10_customers_per_branch.nlargest(10, 'Transaction Volume').reset_index(drop=True)
    top_10_customers_per_branch['Rank'] = top_10_customers_per_branch['Transaction Volume'].rank(method='min', ascending=False).astype(int)
    return top_10_customers_per_branch

def init_threshold():
    return data['Account Balance'].mean().round(2)

def get_high_account_balance_customers(data, threshold):
    high_account_balance_customers = data[data['Account Balance'] > threshold].groupby(['Email']).agg({'Account Balance': 'mean'}).rename(columns={'Account Balance': 'Average Account Balance'}).reset_index()
    high_account_balance_customers = high_account_balance_customers.nlargest(10, 'Average Account Balance').reset_index(drop=True)
    high_account_balance_customers['Rank'] = high_account_balance_customers['Average Account Balance'].rank(method='min', ascending=False).astype(int)
    high_account_balance_customers['Average Account Balance'] = high_account_balance_customers['Average Account Balance'].round(2)
    return high_account_balance_customers

def generate_customer_table(branch_id):
    top_10_customers_per_branch = get_10_customers_per_branch(data, branch_id)
    return html.Table([
        html.Thead([
            html.Tr([html.Th("Rank"), html.Th("Email"), html.Th("Transaction Volume")])
        ]),
        html.Tbody([
            html.Tr([
                html.Td(customer['Rank']),
                html.Td(customer['Email']),
                html.Td(customer['Transaction Volume'])
            ]) for _, customer in top_10_customers_per_branch.iterrows()
        ])
    ], style={'margin': '0 auto', 'text-align': 'center', 'border': '1px solid #999'})


def generate_high_account_balance_customer_table(threshold=init_threshold()):
    high_account_balance_customers = get_high_account_balance_customers(data, threshold)
    return html.Table([
        html.Thead([
            html.Tr([html.Th("Rank"), html.Th("Email"), html.Th("Average Account Balance")])
        ]),
        html.Tbody([
            html.Tr([
                html.Td(customer['Rank']),
                html.Td(customer['Email']),
                html.Td(customer['Average Account Balance'])
            ]) for _, customer in high_account_balance_customers.iterrows()
        ])
    ], style={'margin': '0 auto', 'text-align': 'center', 'border': '1px solid #999'})

app.layout = html.Div([
    html.H1("FinAnalysis"),
    html.Div([
        html.Div([
            html.H3("Total Customer IDs"),
            html.P(f"{data['Customer ID'].nunique()}")
        ], style={'display': 'inline-block', 'width': '24%', 'padding': '10px', 'border': '1px solid #999'}),
        html.Div([
            html.H3("Total Emails"),
            html.P(f"{data['Email'].nunique()}")
        ], style={'display': 'inline-block', 'width': '24%', 'padding': '10px', 'border': '1px solid #999'}),
        html.Div([
            html.H3("Total Branches"),
            html.P(f"{data['Branch ID'].nunique()}")
        ], style={'display': 'inline-block', 'width': '24%', 'padding': '10px', 'border': '1px solid #999'}),
        html.Div([
            html.H3("Total Transactions"),
            html.P(f"{data.shape[0]}")
        ], style={'display': 'inline-block', 'width': '24%', 'padding': '10px', 'border': '1px solid #999'})
    ], style={'display': 'flex', 'justify-content': 'space-around', 'margin-bottom': '20px'}),
    html.Div([
        dcc.DatePickerRange(
            id='date-picker-range',
            start_date=data['Transaction Date'].min(),
            end_date=data['Transaction Date'].max(),
            display_format='YYYY-MM-DD',
            style={'border': '1px solid #fff', 'padding': '10px'}
        ),
        html.Button('Reset', id='reset-button', n_clicks=0, style={'margin-left': '10px', 'height': '48px', 'padding': '10px'})
    ], style={'display': 'flex', 'align-items': 'center', 'justify-content': 'center', 'margin-bottom': '20px'}),
    html.Div([
        html.H2("Transaction Volume by Branch", style={'margin-bottom': '20px', 'align-items': 'center'}),
        html.Div(id='transaction-vol-by-branch', children=[
            dcc.Graph(figure=get_transaction_volume_by_branch_plot(data))
        ])
    ], style={'margin-bottom': '10px', 'padding': '10px', 'border': '1px solid #999'}),
    html.Div([
        html.H3("Top 10 Customers Per Branch By Transaction Volume", style={'text-align': 'center'}),
        html.Div([
            html.H4("Branch ID"),
            dcc.Dropdown(
                id='branch-dropdown',
                options=[{'label': branch, 'value': branch} for branch in sorted(data['Branch ID'].unique())],
                value=data['Branch ID'].unique()[0],
                clearable=False
            ),
        ], style={'margin': 'auto', 'width': '10%', 'text-align': 'center', 'padding': '10px'}),
        html.Div(id='top-10-customers-per-branch', children=[
            generate_customer_table(data['Branch ID'][0])
        ], style={'margin-bottom': '20px', 'align-items': 'center'}),
    ], style={'margin-bottom': '20px', 'padding': '10px', 'border': '1px solid #999', 'align-items': 'center'}),
    html.Div([
        html.H3("Customers with High Account Balance", style={'text-align': 'center'}),
        html.Div([
            html.H4("Account Balance Threshold"),
            html.Div([
                dcc.Input(
                    id='balance-threshold-input',
                    type='number',
                    value=init_threshold(),
                    style={'margin': 'auto', 'width': '20%', 'text-align': 'center', 'padding': '10px'}
                ),
            ], style={'margin': 'auto', 'width': '30%', 'text-align': 'center', 'padding': '10px'}),
            html.Div(id='high-balance-customers', children=[
                generate_high_account_balance_customer_table()
        ], style={'margin-bottom': '20px', 'align-items': 'center'}),
        ])
    ], style={'margin-bottom': '20px', 'padding': '10px', 'border': '1px solid #999', 'align-items': 'center'}),

], style={'text-align': 'center'})


@app.callback(
    Output('transaction-vol-by-branch', 'children'),
    [Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date')]
)
def update_transaction_volume_by_branch_plot(start_date, end_date):
    updated_data = data[(data['Transaction Date'] >= start_date) & (data['Transaction Date'] <= end_date)]
    plot = get_transaction_volume_by_branch_plot(updated_data)
    return dcc.Graph(figure=plot)

@app.callback(
    Output('date-picker-range', 'start_date'),
    Output('date-picker-range', 'end_date'),
    [Input('reset-button', 'n_clicks')]
)
def reset_date_range(n_clicks):
    return data['Transaction Date'].min(), data['Transaction Date'].max()

@app.callback(
    Output('top-10-customers-per-branch', 'children'),
    [Input('branch-dropdown', 'value')]
)
def update_top_10_customers_per_branch(branch_id=data['Branch ID'][0]):
    print("Method Called with Branch ID: ", branch_id)
    return generate_customer_table(branch_id)

@app.callback(
    Output('high-balance-customers', 'children'),
    [Input('balance-threshold-input', 'value')]
)
def update_high_account_balance_customer_table(threshold):
    return generate_high_account_balance_customer_table(threshold)

if __name__ == '__main__':
    dotenv.load_dotenv()
    app.run_server(debug=True)