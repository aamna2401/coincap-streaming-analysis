from dash.dependencies import Input, Output
from plotly.graph_objects import Figure
from pymongo.database import Database
from dash import Dash, dcc, html
from pymongo import MongoClient
from typing import Dict, Tuple
from typing import Any, List
import pandas as pd
import plots


app = Dash(__name__)


def app_layout() -> html.Div:
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
            dcc.Interval(
                id='interval-component',
                interval=2 * 1000,  # in milliseconds
                n_intervals=0
            )
        ]
    )
    return layout


def aggregate_query(db: Database, pipeline: List[Dict[str, Any]]
                    ) -> pd.DataFrame:
    cursor = db.exchanges.aggregate(pipeline)
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


@app.callback(Output('fig-exchanges-pie', 'figure'),
              Output('fig-exchanges-volume-bar', 'figure'),
              Output('fig-exchanges-trading-pairs-bar', 'figure'),
              Input('interval-component', 'n_intervals'))
def draw_panel(n: int) -> Tuple[Figure, Figure, Figure]:
    # aggregation pipeline
    pipeline = [
        {"$sort": {"_id": -1}},
        {"$limit": 200},
        {"$group": {
            "_id": "$exchangeId",
            "name": {"$first": "$name"},
            "percentTotalVolume": {"$first": "$percentTotalVolume"},
            "rank": {"$first": "$rank"},
            "tradingPairs": {"$first": "$tradingPairs"},
            "volumeUsd": {"$first": "$volumeUsd"},
        }},
        {"$sort": {"rank": 1}},
    ]
    df = aggregate_query(mongodb, pipeline)
    df.fillna(0, inplace=True)
    # pie chart of exchanges percentage volume
    fig_exchanges_pie = plots.exchanges_pie_plot(df)
    fig_exchanges_volume_bar = plots.exchanges_volume_bar(df)
    fig_exchanges_trading_pairs_bar = plots.exchanges_trading_pairs_bar(df)
    figures = (fig_exchanges_pie, fig_exchanges_volume_bar,
               fig_exchanges_trading_pairs_bar)
    return figures


if __name__ == '__main__':
    app.layout = app_layout()
    MONGODB_URI = 'mongodb://localhost:27017/'
    mongodb = get_database(MONGODB_URI)
    app.run_server(debug=True)
