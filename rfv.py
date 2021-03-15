import mysql.connector
conn= mysql.connector.connect(host='94.237.77.73',database='REPORTING-insytio',user='ruser',password='Loyalytics@1234%')
import os
import pandas as pd
import datetime as dt
from datetime import timedelta
import numpy as np
from dateutil.relativedelta import *
from datetime import datetime
import dash_auth

USERNAME_PASSWORD_PAIRS = [['Batman','Begins']]

print('reading from my sql database')
df1 = pd.read_sql("""select CDID,
sum(billAmt)/count(DISTINCT transactionId) atv,
datediff(now(),max(transactionDate)) recency,
count(DISTINCT transactionId) trxns,
cast(max(transactionDate) as date) last_shopped
from transaction_tables tt
where client ='alghurair'
and cdid is not null
and datediff(now(),transactionDate)<=365
and billAmt >0
group by CDID""",conn)
print('data loaded')
df1['last_shopped'] = pd.to_datetime(df1['last_shopped'])
# print(df1.shape)
# print(df1.CDID.nunique())

# splurgers, frequentist, lapsing, moderates

df1.loc[df1['trxns']>=4,'cluster']='Frequentist'
df1.loc[df1['atv']>=420,'cluster']='Splurgers'
df1.loc[df1['recency']>=180,'cluster']='Lapsing'
df1.cluster=df1.cluster.fillna('Moderates')


def avg_kpi(df2,kpi1,kpi2,kpi3):
    df3 = df2.groupby('cluster')[kpi1].mean().reset_index()
    df4 = df2.groupby('cluster')[kpi2].mean().reset_index()
    return df3.merge(df4).merge(df2.groupby('cluster')[kpi3].mean().reset_index())

df2 = avg_kpi(df1,'atv','recency','trxns')
# print(df2)


### DASH AND PLOTLY
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go

def fig1(df2):
    fig_data = {
        'data': [
            go.Scatter(
                x = df2['atv'],
                y = df2['recency'],
                mode = 'markers',
                text = df2['cluster'],
                marker = dict(size = 15*df2['trxns']),
                opacity=0.7
            )
        ],
        'layout': go.Layout(
            title = 'RFV Segments Bubble chart',
            xaxis = {'title': 'Average of ATV'},
            yaxis = dict(title= 'Average of Recency'),
            hovermode='closest'
        )
    }
    return fig_data

app = dash.Dash()
auth = dash_auth.BasicAuth(app,USERNAME_PASSWORD_PAIRS)

server = app.server ## understand dash that we are deploying this 

app.layout = html.Div([
    html.Div([
        html.H3('Select Last Shopped Date Range:'),
        dcc.DatePickerRange(
            id='my_date_picker',
            min_date_allowed=df1['last_shopped'].min(),
            max_date_allowed=df1['last_shopped'].max(),
            start_date=datetime(2021, 1, 2),
            end_date=datetime.today()
        ),
        html.Button(
            id='submit-button',
            n_clicks=0,
            children='Submit',
            style={'fontSize':24, 'marginLeft':'30px'}
        )
    ], style={'display':'inline-block'})
    ,
    html.Div([
    dcc.Graph(
        id = 'rfv-graph',
        figure = fig1(df2)
    )
    ])
])

@app.callback(
    Output('rfv-graph', 'figure'),
    [Input('submit-button', 'n_clicks')],
    [State('my_date_picker', 'start_date'),
    State('my_date_picker', 'end_date')])
def update_graph(n_clicks, start_date, end_date):
    if n_clicks==0:
        fig_data = fig1(df2)
        return fig_data
    else:
        start = datetime.strptime(start_date[:10], '%Y-%m-%d')
        end = datetime.strptime(end_date[:10], '%Y-%m-%d')
        df2 = df1[(df1['last_shopped']>=start) & (df1['last_shopped']<=end)]
        df2 = avg_kpi(df2,'atv','recency','trxns')

        fig = fig1(df2)
        return fig
print('calling server')
# Add the server clause:
if __name__ == '__main__':
    app.run_server()
