from dash.dependencies import Input, Output
from plotly.graph_objects import Figure
from pymongo.database import Database
from dash import Dash, dcc, html
from pymongo import MongoClient
from typing import Tuple
import pandas as pd
import datetime
import plots

app = Dash(__name__)


def app_layout(interval: int) -> html.Div:
    layout = html.Div(
        children=[
            html.H1(children="Crypto Currencies and Exchanges Analysis "
                             "using CoinCap", ),
            html.P(children="Exchanges Market Share", ),
            dcc.Graph(id='fig-exchanges-pie'),
            html.P(children="Volume of top Crypto Exchanges", ),
            dcc.Graph(id='fig-exchanges-volume-bar'),
            html.P(children="Trading pairs offered by top Exchanges", ),
            dcc.Graph(id='fig-exchanges-trading-pairs-bar'),
            html.P(children="Price of Top 10 Currencies (USD)", ),
            dcc.Graph(id='fig-assets-price-line'),
            dcc.Interval(
                id='interval-component',
                interval=interval * 1000,  # in milliseconds
                n_intervals=0
            )
        ]
    )
    return layout


def query_exchanges(db: Database) -> pd.DataFrame:
    pipeline = [
        {"$sort": {"_id": -1}},
        {"$limit": 200},
        {"$group": {
            "_id": "$exchangeId",
            "name": {"$last": "$name"},
            "percentTotalVolume": {"$last": "$percentTotalVolume"},
            "rank": {"$last": "$rank"},
            "tradingPairs": {"$last": "$tradingPairs"},
            "volumeUsd": {"$last": "$volumeUsd"},
        }},
        {"$sort": {"rank": 1}},
    ]
    cursor = db.exchanges.aggregate(pipeline)
    documents = [doc for doc in cursor]
    df = pd.DataFrame(documents)
    return df


def query_assets(db: Database):
    pipeline = [
        {"$sort": {"_id": 1}},
        {"$limit": 300},
        {"$group": {
            "_id": "$id",
            "rank": {"$last": "$rank"},
            "symbol": {"$last": "$symbol"},
            "name": {"$last": "$name"},
            "supply": {"$last": "$supply"},
            "marketCapUsd": {"$last": "$marketCapUsd"},
            "volumeUsd24Hr": {"$last": "$volumeUsd24Hr"},
            "priceUsd": {"$last": "$priceUsd"},
            "changePercent24Hr": {"$last": "$changePercent24Hr"},
            "vwap24Hr": {"$last": "$vwap24Hr"},
        }},
        {"$sort": {"rank": 1}},
    ]
    cursor = db.assets.aggregate(pipeline)
    documents = [doc for doc in cursor]
    df = pd.DataFrame(documents)
    return df


def get_database(mongodb_uri: str) -> Database:
    client = MongoClient(mongodb_uri)
    try:
        print(client.server_info())
        print(client.list_database_names())
    except Exception as e:
        print(e)
    db: Database = client['coincap']
    return db


def update_df_list(df_as: pd.DataFrame, num_rank: int) -> pd.DataFrame:
    df = df_as[df_as['rank'] <= num_rank]
    values = [datetime.datetime.now()] * len(df)
    df.insert(len(df_as.columns), 'timestamp', values)
    if len(df_list) >= num_rank:
        df_list.pop(0)
        df_list.append(df)
    else:
        df_list.append(df)
    return pd.concat(df_list).reset_index(drop=True)


@app.callback(Output('fig-exchanges-pie', 'figure'),
              Output('fig-exchanges-volume-bar', 'figure'),
              Output('fig-exchanges-trading-pairs-bar', 'figure'),
              Output('fig-assets-price-line', 'figure'),
              Input('interval-component', 'n_intervals'))
def draw_panel(n: int) -> Tuple[Figure, Figure, Figure, Figure]:
    # exchanges analysis
    df_ex = query_exchanges(mongodb)
    df_ex.fillna(0, inplace=True)
    # plots for exchanges
    fig_exchanges_pie = plots.exchanges_pie_plot(df_ex)
    fig_exchanges_volume_bar = plots.exchanges_volume_bar(df_ex)
    fig_exchanges_trading_pairs_bar = plots.exchanges_trading_pairs_bar(df_ex)
    # analysis of assets
    num_rank = 10
    df_as = update_df_list(query_assets(mongodb), num_rank)
    fig_assets_price_line = plots.assets_price_line(df_as)
    figures = (fig_exchanges_pie, fig_exchanges_volume_bar,
               fig_exchanges_trading_pairs_bar, fig_assets_price_line)
    return figures


if __name__ == '__main__':
    app.layout = app_layout(10)
    MONGODB_URI = 'mongodb://localhost:27017/'
    mongodb = get_database(MONGODB_URI)
    df_list = []
    app.run_server(debug=True)
