from typing import Any, Dict, List
import pandas as pd
import pymongo
from pymongo import MongoClient
from pymongo.database import Database
from typing import Dict, Tuple
from dash.dependencies import Input, Output
from plotly.graph_objects import Figure
from dash import Dash, dcc, html
import plots


app = Dash(__name__)


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


def main():
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
    df = aggregate_query(db, pipeline)
    df.fillna(0, inplace=True)


if __name__ == '__main__':
    app.layout = html.Div(
        children=[
            html.H1(children="Crypto Currencies and Exchanges Analysis "
                             "using CoinCap", ),
            html.P(children="Exchanges Market Share", ),
            dcc.Graph(id='fig-exchanges-pie'),
            # html.P(children="Gender Information", ),
            # dcc.Graph(id='fig-gender'),
            dcc.Interval(
                id='interval-component',
                interval=2 * 1000,  # in milliseconds
                n_intervals=0
            )
        ]
    )
    MONGODB_URI = 'mongodb://localhost:27017/'
    main()
