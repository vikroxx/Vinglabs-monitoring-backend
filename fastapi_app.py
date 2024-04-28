from fastapi import FastAPI, Query
import psycopg2
from statistics import mean
import math
from datetime import datetime, timedelta, timezone
from utils import get_random_color_distribution, get_random_color_trends, get_random_food_distribution, get_timestamps, \
    get_random_bottle_trends, parse_args, \
    get_random_bottle_distribution, get_random_food_trends, create_filter_query
from typing import Union
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

con = psycopg2.connect(
    database="prt-db",
    user="postgres",
    password="XXXXXXXX",
    host="prt-prod.cgmaehois3kz.ap-south-1.rds.amazonaws.com",
    port='5432'
)
# cursor = con.cursor()

entries_per_minute = 3
least_count = 60 // entries_per_minute
db_table_name = 'aggregate_new'
timestamp_format = "%d%m%Y-%H%M"
timestamp_format_trends = "%d%m%Y-%H%M%S"

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get('/kpis', tags=['KPIs'])
def get_kpi(type_: str = Query(..., alias="type"), start_date: Union[str, None] = None,
            end_date: Union[str, None] = None,
            start_time: Union[str, None] = None, end_time: Union[str, None] = None):
    cursor = con.cursor()
    kpi_request_args = {'start_date': start_date, 'type': type_, 'end_date': end_date, 'start_time': start_time,
                        'end_time': end_time}

    request_type, start_timestamp, end_timestamp = parse_args(kpi_request_args)

    bottle_processed = []
    food_grade_percentage = []
    non_food_clear_percentage = []
    opaque_percentage = []
    bot_per_sec = []

    filter_query = create_filter_query(start_timestamp, end_timestamp,
                                       ['datetime', 'bottle_processed', 'clear_light_blue_processed',
                                        'sleeve_clear_processed', 'food_processed', 'non_food_processed',
                                        'opaque_processed', 'bottle_per_sec'],
                                       db_table_name)
    print(filter_query)

    try:
        cursor.execute(filter_query)
    except Exception as err:
        print(err)
    results = cursor.fetchall()
    # print("kpi results ", results)
    if bool(results):
        timestamps, bottle_processed, clear_light_blue_processed, sleeve_clear_processed, food_processed, \
        non_food_processed, opaque_processed, bot_per_sec = list(map(list, zip(*results)))

        bottle_processed = sum(bottle_processed)
        clear_light_blue_processed = sum(clear_light_blue_processed)
        sleeve_clear_processed = sum(sleeve_clear_processed)
        food_processed = sum(food_processed)
        non_food_processed = sum(non_food_processed)
        opaque_processed = sum(opaque_processed)

        # print(bot_per_sec[0], timestamps[0])
        # print(type(timestamps[-1]))
        # print(bot_per_sec[-1], timestamps[-1].strftime(timestamp_format_trends))
        current_datetime = datetime.now(timezone.utc) + timedelta(hours=2)
        # print(current_datetime)
        delta = current_datetime.replace(tzinfo=None) - timestamps[-1].replace(tzinfo=None)
        # print(delta < timedelta(seconds=30))
        # print(delta)

        if request_type == 'filter':
            bot_per_sec = round(mean(bot_per_sec), 2)
        elif request_type == 'live':
            if delta < timedelta(minutes =2):
                bot_per_sec = round(bot_per_sec[-1], 2)
            else:
                bot_per_sec = []

        if bottle_processed != 0:
            food_grade_percentage = round(food_processed / bottle_processed * 100, 2)
            opaque_percentage = round(opaque_processed / bottle_processed * 100, 2)

        clear_light_blue_processed += sleeve_clear_processed
        if clear_light_blue_processed != 0:
            non_food_clear_percentage = round(non_food_processed / clear_light_blue_processed * 100, 2)

    return {
        "bottle_processed": bottle_processed,
        "food_grade_percentage": food_grade_percentage,
        "non_food_clear": non_food_clear_percentage,
        "opaque_percentage": opaque_percentage,
        "bottles_per_second": bot_per_sec,
        "start_timestamp": start_timestamp.strftime(timestamp_format),
        "end_timestamp": end_timestamp.strftime(timestamp_format)
    }


@app.get('/dbstart_time', tags=['Database start time'])
def get_dbstart_time():
    return {
        'start_timestamp': '29092022-2140',
    }


@app.get('/bottle_trends', tags=['Bottle, No Bottle, Cans Trend'])
def bottle_trend(type_: str = Query(..., alias="type"), start_date: Union[str, None] = None,
                 end_date: Union[str, None] = None,
                 start_time: Union[str, None] = None, end_time: Union[str, None] = None):
    cursor = con.cursor()

    kpi_request_args = {'start_date': start_date, 'type': type_, 'end_date': end_date, 'start_time': start_time,
                        'end_time': end_time}

    request_type, start_timestamp, end_timestamp = parse_args(kpi_request_args)

    can_values = []
    bottle_percent = []
    non_bottle_percent = []
    cans_percent = []
    bot_per_sec = []
    nobot_per_sec = []
    timestamps = []

    filter_query = create_filter_query(start_timestamp, end_timestamp,
                                       ['datetime', 'cans_processed', 'bottle_percentage', 'no_bottle_percentage',
                                        'cans_percentage',
                                        'bottle_per_sec', 'no_bottle_per_sec'],
                                       db_table_name)

    print(filter_query)

    cursor.execute(filter_query)

    results = cursor.fetchall()

    if bool(results):
        timestamps, can_values, bottle_percent, non_bottle_percent, cans_percent, \
        bot_per_sec, nobot_per_sec = list(map(list, zip(*results)))

    timestamps = [timestamp.strftime(timestamp_format_trends) for timestamp in timestamps]

    return {
        'bottle_trend': {
            'timestamp': timestamps,
            'value': bot_per_sec,
            'percent': bottle_percent
        },
        'nonbottle_trend': {
            'timestamp': timestamps,
            'value': nobot_per_sec,
            'percent': non_bottle_percent
        },
        'cans_trend': {
            'timestamp': timestamps,
            'value': can_values,
            'percent': cans_percent
        }

    }


@app.get('/bottle_distribution', tags=['Bottle Distribution for PI charts'])
def bottle_distribution(type_: str = Query(..., alias="type"), start_date: Union[str, None] = None,
                        end_date: Union[str, None] = None,
                        start_time: Union[str, None] = None, end_time: Union[str, None] = None):
    cursor = con.cursor()
    kpi_request_args = {'start_date': start_date, 'type': type_, 'end_date': end_date, 'start_time': start_time,
                        'end_time': end_time}

    request_type, start_timestamp, end_timestamp = parse_args(kpi_request_args)

    bottle_values = []
    non_bottle_values = []
    can_values = []
    bottle_percent = []
    non_bottle_percent = []
    cans_percent = []

    filter_query = create_filter_query(start_timestamp, end_timestamp,
                                       ['bottle_processed', 'no_bottle_processed', 'cans_processed'], db_table_name)

    print(filter_query)

    cursor.execute(filter_query)

    results = cursor.fetchall()

    if bool(results):
        bottle_values, non_bottle_values, can_values = list(map(list, zip(*results)))

        bottle_values = sum(bottle_values)
        non_bottle_values = sum(non_bottle_values)
        can_values = sum(can_values)

        # can_values = int(sum(list(filter('NA'.__ne__, can_values))))
        total_number = bottle_values + non_bottle_values + can_values

        if total_number != 0:
            bottle_percent = round(bottle_values / total_number * 100, 2)
            non_bottle_percent = round(non_bottle_values / total_number * 100, 2)
            cans_percent = round(can_values / total_number * 100, 2)

    return {
        'bottle_percent': bottle_percent,
        'nonbottle_percent': non_bottle_percent,
        'cans_percent': cans_percent,
        'bottle_number': bottle_values,
        'nonbottle_number': non_bottle_values,
        'cans_number': can_values
    }


@app.get("/food_trends", tags=['Food,  Non Food Trend'])
def food_trend(type_: str = Query(..., alias="type"), start_date: Union[str, None] = None,
               end_date: Union[str, None] = None,
               start_time: Union[str, None] = None, end_time: Union[str, None] = None):
    cursor = con.cursor()
    kpi_request_args = {'start_date': start_date, 'type': type_, 'end_date': end_date, 'start_time': start_time,
                        'end_time': end_time}

    request_type, start_timestamp, end_timestamp = parse_args(kpi_request_args)

    food_values = []
    food_percentage = []
    nonfood_values = []
    nonfood_percentage = []
    timestamps = []

    filter_query = create_filter_query(start_timestamp, end_timestamp,
                                       ['datetime', 'food_per_sec', 'food_percentage', 'non_food_per_sec',
                                        'non_food_percentage'],
                                       db_table_name)

    print(filter_query)

    cursor.execute(filter_query)

    results = cursor.fetchall()

    if bool(results):
        timestamps, food_values, food_percentage, nonfood_values, nonfood_percentage = list(
            map(list, zip(*results)))

        timestamps = [timestamp.strftime(timestamp_format_trends) for timestamp in timestamps]

    return {
        'food_trend': {
            'timestamp': timestamps,
            'value': food_values,
            'percent': food_percentage
        },
        'nonfood_trend': {
            'timestamp': timestamps,
            'value': nonfood_values,
            'percent': nonfood_percentage
        }

    }


@app.get('/food_distribution', tags=['Food, Non Food Distribution for PI charts'])
def food_distribution(type_: str = Query(..., alias="type"), start_date: Union[str, None] = None,
                      end_date: Union[str, None] = None,
                      start_time: Union[str, None] = None, end_time: Union[str, None] = None):
    cursor = con.cursor()
    kpi_request_args = {'start_date': start_date, 'type': type_, 'end_date': end_date, 'start_time': start_time,
                        'end_time': end_time}

    request_type, start_timestamp, end_timestamp = parse_args(kpi_request_args)

    food_distribution = []
    nonfood_distribution = []
    food_values = []
    non_food_values = []

    filter_query = create_filter_query(start_timestamp, end_timestamp,
                                       ['food_processed', 'non_food_processed'],
                                       db_table_name)

    print(filter_query)

    cursor.execute(filter_query)

    results = cursor.fetchall()

    if bool(results):
        food_processed, non_food_processed = list(map(list, zip(*results)))

        food_values = sum(food_processed)
        non_food_values = sum(non_food_processed)
        total_number = food_values + non_food_values

        if total_number != 0:
            food_distribution = round(food_values / total_number * 100, 2)
            nonfood_distribution = round(non_food_values / total_number * 100, 2)

    return {
        'food_percent': food_distribution,
        'nonfood_percent': nonfood_distribution,
        'food_number': food_values,
        'nonfood_number': non_food_values,
    }


@app.get('/color_trends', tags=['Color Trend'])
def color_trend(type_: str = Query(..., alias="type"), start_date: Union[str, None] = None,
                end_date: Union[str, None] = None,
                start_time: Union[str, None] = None, end_time: Union[str, None] = None):
    cursor = con.cursor()
    kpi_request_args = {'start_date': start_date, 'type': type_, 'end_date': end_date, 'start_time': start_time,
                        'end_time': end_time}

    request_type, start_timestamp, end_timestamp = parse_args(kpi_request_args)

    clear_light_blue_values = []
    blue_and_darks_values = []
    green_values = []
    opaque_values = []
    other_values = []
    sleeve_clear_values = []

    clear_light_blue_percentage = []
    blue_and_darks_percentage = []
    green_percentage = []
    opaque_percentage = []
    other_percentage = []
    sleeve_clear_percentage = []

    timestamps = []

    filter_query = create_filter_query(start_timestamp, end_timestamp,
                                       ['datetime', 'clear_light_blue_processed', 'darks_and_blue_processed',
                                        'opaque_processed', 'other_processed', 'sleeve_clear_processed',
                                        'green_processed', 'clear_light_blue_percentage',
                                        'darks_and_blue_percentage',
                                        'opaque_percentage', 'other_percentage', 'sleeve_clear_percentage',
                                        'green_percentage'],
                                       db_table_name)

    print(filter_query)

    cursor.execute(filter_query)

    results = cursor.fetchall()

    if bool(results):
        timestamps, clear_light_blue_values, blue_and_darks_values, opaque_values, other_values, \
        sleeve_clear_values, green_values, clear_light_blue_percentage, blue_and_darks_percentage, \
        opaque_percentage, other_percentage, sleeve_clear_percentage, green_percentage = list(
            map(list, zip(*results)))

        clear_light_blue_values = [x + y for x, y in zip(clear_light_blue_values, sleeve_clear_values)]
        clear_light_blue_percentage = [
            x + y if (x is not None and y is not None) else (x if y is None else (y if x is None else None)) for x, y in
            zip(clear_light_blue_percentage, sleeve_clear_percentage)]

    timestamps = [timestamp.strftime(timestamp_format_trends) for timestamp in timestamps]

    return {
        'clear_light_blue_trend': {
            'timestamp': timestamps,
            'value': clear_light_blue_values,
            'percent': clear_light_blue_percentage
        },
        'blue_and_darks_trend': {
            'timestamp': timestamps,
            'value': blue_and_darks_values,
            'percent': blue_and_darks_percentage
        },
        'green_trend': {
            'timestamp': timestamps,
            'value': green_values,
            'percent': green_percentage
        },
        'opaque_trend': {
            'timestamp': timestamps,
            'value': opaque_values,
            'percent': opaque_percentage
        },
        'other_trend': {
            'timestamp': timestamps,
            'value': other_values,
            'percent': other_percentage
        }

    }


@app.get('/color_distribution', tags=['Color  Distribution for PI charts'])
def color_distribution(type_: str = Query(..., alias="type"), start_date: Union[str, None] = None,
                       end_date: Union[str, None] = None,
                       start_time: Union[str, None] = None, end_time: Union[str, None] = None):
    cursor = con.cursor()
    kpi_request_args = {'start_date': start_date, 'type': type_, 'end_date': end_date, 'start_time': start_time,
                        'end_time': end_time}

    request_type, start_timestamp, end_timestamp = parse_args(kpi_request_args)

    clear_light_blue_values = []
    blue_and_darks_values = []
    green_values = []
    opaque_values = []
    other_values = []
    sleeve_clear_values = []

    filter_query = create_filter_query(start_timestamp, end_timestamp,
                                       ['clear_light_blue_processed', 'darks_and_blue_processed',
                                        'opaque_processed', 'other_processed', 'sleeve_clear_processed',
                                        'green_processed'],
                                       db_table_name)

    print(filter_query)

    cursor.execute(filter_query)

    results = cursor.fetchall()

    if bool(results):
        clear_light_blue_values, blue_and_darks_values, opaque_values, other_values, \
        sleeve_clear_values, green_values = list(map(list, zip(*results)))

        clear_light_blue_values = sum(clear_light_blue_values)
        sleeve_clear_values = sum(sleeve_clear_values)
        green_values = sum(green_values)
        opaque_values = sum(opaque_values)
        other_values = sum(other_values)
        blue_and_darks_values = sum(blue_and_darks_values)

        clear_light_blue_values += sleeve_clear_values
        total_number = clear_light_blue_values + blue_and_darks_values + green_values + opaque_values + \
                       other_values
        if total_number != 0:
            clear_light_blue_values = round(clear_light_blue_values / total_number * 100, 2)
            blue_and_darks_values = round(blue_and_darks_values / total_number * 100, 2)
            green_values = round(green_values / total_number * 100, 2)
            opaque_values = round(opaque_values / total_number * 100, 2)
            other_values = round(other_values / total_number * 100, 2)

    return {
        'clear_light_blue_percent': clear_light_blue_values,
        'blue_and_darks_percent': blue_and_darks_values,
        'green_percent': green_values,
        'opaque_percent': opaque_values,
        'other_percent': other_values
    }
