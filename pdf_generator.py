import os
from utils import create_filter_query, parse_args_for_pdf
import psycopg2
from statistics import mean
from tqdm import tqdm
import plotly.express as px
import locale
import plotly.graph_objects as go
from generate_final_image import generate_final_image
import cv2

locale.setlocale(locale.LC_ALL, 'en_US')

start_date = '18112022'
# info = "The Plant was not operational from 00:40 - 15:20, ~15 Hrs."
# info = "System was in shutdown between 07:40 to 13:40"
# note = "(Coinciding with Plant shutdown b/w 06:00 to 15:20)"
info = ''
note = 'The system was not operational between 18:00 - 20:00'
con = psycopg2.connect(
    database="prt-db",
    user="postgres",
    password="biology12",
    host="prt-prod.cmuk1jsd4tyu.ap-south-1.rds.amazonaws.com",
    port='5432'
)
cursor = con.cursor()

db_table_name = 'aggregate_new'
timestamp_format = "%d%m%Y-%H%M"

start_timestamp, end_timestamp = parse_args_for_pdf(start_date)
filter_query = create_filter_query(start_timestamp, end_timestamp,
                                   ['bottle_processed', 'no_bottle_processed',
                                    'cans_processed', 'non_food_processed', 'food_processed',
                                    'opaque_processed', 'bottle_per_sec'],
                                   db_table_name)

cursor.execute(filter_query)
results = cursor.fetchall()

breakpoints = [2, 15, 30]
histogram_dist = {}

breakpoints_cum = [(x + 1) * 5 for x in range(12)]
cum_dist = {}
print(bool(results))
if bool(results):
    bottle_processed, no_bottle_processed, cans_processed, non_food_processed, food_processed, opaque_processed, \
    bottle_per_sec = list(map(list, zip(*results)))

    bottle_per_sec = [round(x, 2) for x in bottle_per_sec]

    print(len(bottle_processed))
    total_bottle_processed = int(sum(bottle_processed))
    average_bottle_per_sec = round(mean(bottle_per_sec), 2)
    max_bottle_per_sec = round(max(bottle_per_sec), 2)
    average_opaque_percentage = round(sum(opaque_processed) / sum(bottle_processed) * 100, 2)
    average_non_food_percentage = round(sum(non_food_processed) / (sum(non_food_processed) + sum(food_processed)) * 100,
                                        2)
    print(total_bottle_processed, average_bottle_per_sec, max_bottle_per_sec, average_non_food_percentage,
          average_opaque_percentage)

    for i, ele in enumerate(breakpoints):
        if i == 0:
            end = ele
            filtered = list(filter(lambda x: x < end, bottle_per_sec))
            histogram_dist['less than {}'.format(ele)] = len(filtered)

            start = ele
            end = breakpoints[i + 1]
            filtered = list(filter(lambda x: start <= x < end, bottle_per_sec))
            histogram_dist['between {} and {}'.format(start, end)] = len(filtered)

        elif i == len(breakpoints) - 1:
            start = ele
            filtered = list(filter(lambda x: x >= start, bottle_per_sec))
            histogram_dist['more than {}'.format(ele)] = len(filtered)


        else:
            start = ele
            end = breakpoints[i + 1]
            filtered = list(filter(lambda x: start <= x < end, bottle_per_sec))
            histogram_dist['between {} and {}'.format(start, end)] = len(filtered)

    print(histogram_dist, sum(list(histogram_dist.values())))

    for i, ele in enumerate(breakpoints_cum):
        filtered = list(filter(lambda x: x <= ele, bottle_per_sec))
        cum_dist[ele] = round(len(filtered) / len(bottle_processed) * 100, 2)

    print(cum_dist)

    values = [int(sum(bottle_processed)), int(sum(no_bottle_processed)), int(sum(cans_processed))]
    labels = ['Bottle : {}'.format(locale.format_string("%d", values[0], grouping=True)),
              'No_bottle : {} '.format(locale.format_string("%d", values[1], grouping=True)),
              'Cans : {}'.format(locale.format_string("%d", values[2], grouping=True))]

    print(values)

    # PIE CHART
    if True:
        fig = go.Figure(data=[
            go.Pie(labels=labels, values=values, hole=.4,
                   marker={'colors': ['#004d90', '#e923a7', '#1e88e5', '#7e35b1']})])
        fig.update_layout(
            font=dict(
                family="Calibri",
                size=23,  # Set the font size here
                color="Black"
            ),
            title={'text': '<b><i>Pie chart showing material distribution in Plant</b></i>',
                   'xanchor': 'center',
                   'x': 0.5, },
            title_font_size=25
        )
        fig.update_layout(legend=dict(y=0.5, x=1))
        fig.write_image("images\\{}_pie.jpeg".format(start_date))
        # fig.show()

    # BAR CHART
    if True:
        print('Plotting histogram...')
        fig2 = px.bar(x=list(histogram_dist.keys()),
                      y=[round(x / sum(list(histogram_dist.values())) * 100, 2) for x in list(histogram_dist.values())],
                      text_auto='.3s', width=800, height=600)
        fig2.update_traces(textfont_size=20, textangle=0, textposition="outside", cliponaxis=False,
                           marker_color='#6e1ca7')
        fig2.update_layout(
            font=dict(
                family="Calibri",
                size=18,  # Set the font size here
                color="black",

            ),
            bargap=0.4,
            barmode='relative',
            xaxis=dict(
                tickformat='%.format.%3f',
                fixedrange=True,
                hoverformat='.3f',
                showgrid=False),
            plot_bgcolor='rgb(255,255,255)',
            title={'text': '<b><i>Distribution of Plant throughput across time</b></i>',
                   'xanchor': 'center',
                   'x': 0.5}
        )
        fig2.update_xaxes(
            title_text="<b>Bottles/sec range</b>",
            title_font={"size": 25},
            title_standoff=30)
        fig2.update_yaxes(
            title_text="<b>Percent of Total Time</b>",
            title_font={"size": 25},
            title_standoff=20,
            gridcolor='#d9e4ee')
        fig2.write_image("images\\{}_hist.jpeg".format(start_date))

    # Line chart
    fig3 = go.Figure(data=go.Scatter(x=list(cum_dist.keys()), y=list(cum_dist.values()), text=[str(round(x, 1)) + '%'
                                                                                               for x in
                                                                                               list(cum_dist.values())],
                                     mode='lines+markers+text'))
    fig3.update_layout(
        font=dict(
            family="Calibri",
            size=18,  # Set the font size here
            color="black",

        ),
        xaxis=dict(
            tickformat='%.format.%3f',
            fixedrange=True,
            hoverformat='.3f',
            showgrid=False),
        plot_bgcolor='rgb(255,255,255)',
        title={'text': '<b><i>Cumulative Distribution of Plant throughput across time</b></i>',
               'xanchor': 'center',
               'x': 0.5}
    )
    fig3.update_xaxes(
        title_text="<b>Bottles/sec range</b>",
        title_font={"size": 25},
        title_standoff=30)
    fig3.update_yaxes(
        title_text="<b>Percent of Total Time</b>",
        title_font={"size": 25},
        title_standoff=20,
        gridcolor='#d9e4ee')
    fig3.update_layout(
        autosize=False,
        width=1200,
        height=600,
        margin=dict(
            l=50,
            r=50,
            b=100,
            t=100,
            pad=4
        ))
    fig3.update_traces(marker=dict(size=12,
                                   line=dict(width=2,
                                             color='DarkSlateGrey')),
                       textposition='top center')
    fig3.write_image("images\\{}_cumhist.jpeg".format(start_date))

    hist, pi, cumhist = cv2.imread("images\\{}_hist.jpeg".format(start_date)), cv2.imread(
        "images\\{}_pie.jpeg".format(start_date)), cv2.imread(
        "images\\{}_cumhist.jpeg".format(start_date))
    generate_final_image(start_date, hist, pi, cumhist, total_bottle_processed, average_bottle_per_sec,
                         max_bottle_per_sec, average_non_food_percentage, average_opaque_percentage, bottom_note=note,
                         info=info)

    os.remove("images\\{}_hist.jpeg".format(start_date))
    os.remove("images\\{}_pie.jpeg".format(start_date))
    os.remove("images\\{}_cumhist.jpeg".format(start_date))
