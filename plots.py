from plotly.graph_objects import Figure
import plotly.express as px
import pandas as pd


def exchanges_pie_plot(df: pd.DataFrame) -> Figure:
    df = df[df.percentTotalVolume >= 1]
    fig = px.pie(df, values='percentTotalVolume', names='name', color='name')
    fig.update_layout(title='Percentage Volume of Exchanges', width=600)
    return fig


def exchanges_volume_bar(df: pd.DataFrame) -> Figure:
    df = df[df['rank'] <= 20]
    fig = px.bar(df, x='name', y='volumeUsd', color='name')
    fig.update_layout(title='Volume of Exchanges',
                      xaxis_title='Exchange Name',
                      yaxis_title='Volume in USD',
                      height=600, width=1500)
    return fig


def exchanges_trading_pairs_bar(df: pd.DataFrame) -> Figure:
    df = df[df['rank'] <= 20]
    fig = px.bar(df, x='name', y='tradingPairs', color='name')
    fig.update_layout(title='Trading Pairs offered by Exchanges',
                      xaxis_title='Exchange Name',
                      yaxis_title='Number of Trading Pairs',
                      height=600, width=1500)
    return fig
