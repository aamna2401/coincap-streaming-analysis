from plotly.graph_objects import Figure
import plotly.express as px


def exchanges_pie_plot(df):
    df = df[df.percentTotalVolume > 0]
    fig = px.pie(df, values='percentTotalVolume', names='name', color='name')
    fig.update_layout(title='Percentage Volume of Exchanges', width=600)
    return fig
