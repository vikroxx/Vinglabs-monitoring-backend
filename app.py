from wsgiref import validate
from flask import Flask
from flask_restx import Resource, Api, fields, reqparse
from datetime import datetime, timedelta
from utils import get_random_color_distribution, get_random_color_trends, get_random_food_distribution, get_timestamps, \
    get_random_bottle_trends, parse_args, \
    get_random_bottle_distribution, get_random_food_trends, create_filter_query
import random
from flask_cors import CORS
import psycopg2
from statistics import mean
import math

app = Flask(__name__)
api = Api(app)
CORS(app, resources={r'/*': {'origins': '*'}})

con = psycopg2.connect(
    database="prt-db",
    user="postgres",
    password="biology12",
    host="prt-prod.cmuk1jsd4tyu.ap-south-1.rds.amazonaws.com",
    port='5432'
)
cursor = con.cursor()

entries_per_minute = 3
least_count = 60 // entries_per_minute
db_table_name = 'aggregate'
timestamp_format = "%d%m%Y-%H%M"

kpi_response = api.model('kpi_response', {
    "bottle_processed": fields.String,
    "food_grade_percentage": fields.String,
    "non_food_clear": fields.String,
    "opaque_percentage": fields.String,
    "bottles_per_second": fields.String,
    "start_date": fields.String,
    "start_time": fields.String,
    "end_date": fields.String,
    "end_time": fields.String
})

bottle_trend_response = api.model('bottle_trend_response', {
    'bottle_trend': fields.Nested(api.model('model1', {
        'timestamp': fields.List(fields.String),
        'value': fields.List(fields.String),
        'percent': fields.List(fields.String)
    })),
    'nonbottle_trend': fields.Nested(api.model('model2', {
        'timestamp': fields.List(fields.String),
        'value': fields.List(fields.String),
        'percent': fields.List(fields.String)
    })),
    'cans_trend': fields.Nested(api.model('model3', {
        'timestamp': fields.List(fields.String),
        'value': fields.List(fields.String),
        'percent': fields.List(fields.String)

    }))

})

food_trend_response = api.model('food_trend_response', {
    'food_trend': fields.Nested(api.model('model4', {
        'timestamp': fields.List(fields.String),
        'value': fields.List(fields.String),
        'percent': fields.List(fields.String)
    })),
    'nonfood_trend': fields.Nested(api.model('model5', {
        'timestamp': fields.List(fields.String),
        'value': fields.List(fields.String),
        'percent': fields.List(fields.String)
    }))

})

color_trend_response = api.model('color_trend_response', {
    'clear_light_blue_trend': fields.Nested(api.model('model6', {
        'timestamp': fields.List(fields.String),
        'value': fields.List(fields.String),
        'percent': fields.List(fields.String)
    })),
    'blue_and_darks_trend': fields.Nested(api.model('model7', {
        'timestamp': fields.List(fields.String),
        'value': fields.List(fields.String),
        'percent': fields.List(fields.String)
    })),
    'green_trend': fields.Nested(api.model('model8', {
        'timestamp': fields.List(fields.String),
        'value': fields.List(fields.String),
        'percent': fields.List(fields.String)
    })),
    'opaque_trend': fields.Nested(api.model('model9', {
        'timestamp': fields.List(fields.String),
        'value': fields.List(fields.String),
        'percent': fields.List(fields.String)
    })),
    'other_trend': fields.Nested(api.model('model10', {
        'timestamp': fields.List(fields.String),
        'value': fields.List(fields.String),
        'percent': fields.List(fields.String)
    }))
})

bottle_distribution_response = api.model('bottle_distribution_response', {
    'bottle_percent': fields.String,
    'nonbottle_percent': fields.String,
    'cans_percent': fields.String,
    'bottle_number': fields.String,
    'nonbottle_number': fields.String,
    'cans_number': fields.String
})

food_distribution_response = api.model('food_distribution_response', {
    'food_percent': fields.String,
    'nonfood_percent': fields.String,
    'food_number': fields.String,
    'non_food_number': fields.String,

})

color_distribution_response = api.model('color_distribution_response', {
    'clear_light_blue_percent': fields.String,
    'blue_and_darks_percent': fields.String,
    'green_percent': fields.String,
    'opaque_percent': fields.String,
    'other_percent': fields.String,

    'clear_light_blue_number': fields.String,
    'blue_and_darks_number': fields.String,
    'green_number': fields.String,
    'opaque_number': fields.String,
    'other_number': fields.String
})

database_start_time_response = api.model('database_start_time_response', {
    'start_timestamp': fields.String
})

request_parser = reqparse.RequestParser()
request_parser.add_argument('type', type=str, help='live/filter')
request_parser.add_argument('start_date', type=str, help='""/ddmmyyyy')
request_parser.add_argument('end_date', type=str, help='""/ddmmyyyy')
request_parser.add_argument('start_time', type=str, help='""/hhmm')
request_parser.add_argument('end_time', type=str, help='""/hhmm')


@api.route('/dbstart_time')
class DBStartTime(Resource):
    @api.marshal_with(database_start_time_response, envelope='resource')
    @api.expect(request_parser)
    def get(self, **kwargs):
        request_args = request_parser.parse_args()
        request_type, start_timestamp, end_timestamp = parse_args(request_args)

        return {
            'start_timestamp': '30092022-2200',
        }


@api.route('/kpis')
class KPI(Resource):
    @api.marshal_with(kpi_response, envelope='resource')
    @api.expect(request_parser)
    def get(self, **kwargs):
        kpi_request_args = request_parser.parse_args()
        print(kpi_request_args)
        request_type, start_timestamp, end_timestamp = parse_args(kpi_request_args)

        bottle_processed = []
        food_grade_percentage = []
        non_food_clear_percentage = []
        opaque_percentage = []
        bot_per_sec = []

        filter_query = create_filter_query(start_timestamp, end_timestamp,
                                           ['bottle_processed', 'clear_light_blue_processed',
                                            'sleeve_clear_processed', 'food_processed', 'non_food_processed',
                                            'opaque_processed', 'bottle_per_sec'],
                                           db_table_name)
        print(filter_query)
        cursor.execute(filter_query)
        results = cursor.fetchall()

        if bool(results):
            bottle_processed, clear_light_blue_processed, sleeve_clear_processed, food_processed, \
            non_food_processed, opaque_processed, bot_per_sec = list(map(list, zip(*results)))

            bottle_processed = sum(bottle_processed)
            clear_light_blue_processed = sum(clear_light_blue_processed)
            sleeve_clear_processed = sum(sleeve_clear_processed)
            food_processed = sum(food_processed)
            non_food_processed = sum(non_food_processed)
            opaque_processed = sum(opaque_processed)
            bot_per_sec = math.floor(mean(bot_per_sec))

            if bottle_processed != 0:
                food_grade_percentage = round(food_processed / bottle_processed * 100, 2) + '%'
                opaque_percentage = round(opaque_processed / bottle_processed * 100, 2) + '%'

            clear_light_blue_processed += sleeve_clear_processed
            if clear_light_blue_processed != 0:
                non_food_clear_percentage = round(non_food_processed / clear_light_blue_processed * 100, 2) + '%'

        return {
            "bottle_processed": bottle_processed,
            "food_grade_percentage": food_grade_percentage,
            "non_food_clear": non_food_clear_percentage,
            "opaque_percentage": opaque_percentage,
            "bottles_per_second": math.floor(bot_per_sec),
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp
        }


@api.route('/bottle_trends')
class BottleTrends(Resource):
    @api.marshal_with(bottle_trend_response, envelope='resource')
    @api.expect(request_parser)
    def get(self, **kwargs):
        request_args = request_parser.parse_args()
        # request_type,start_date,start_time,end_date,end_time = parse_args(request_args)
        request_type, start_timestamp, end_timestamp = parse_args(request_args)

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

        timestamps = [timestamp.strftime(timestamp_format) for timestamp in timestamps]

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


@api.route('/bottle_distribution')
class BottleDistribution(Resource):
    @api.marshal_with(bottle_distribution_response, envelope='resource')
    @api.expect(request_parser)
    def get(self, **kwargs):
        request_args = request_parser.parse_args()
        # request_type, start_date, start_time, end_date, end_time = parse_args(request_args)
        # request_args = request_parser.parse_args()
        # request_type,start_date,start_time,end_date,end_time = parse_args(request_args)
        request_type, start_timestamp, end_timestamp = parse_args(request_args)

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

            # bottle_percent = list(filter('NA'.__ne__, bottle_percent))
            # if len(bottle_percent) != 0:
            #     bottle_percent = round(mean(bottle_percent), 2)
            # else:
            #     bottle_percent = 'NA'
            #
            # non_bottle_percent = list(filter('NA'.__ne__, non_bottle_percent))
            # if len(non_bottle_percent) != 0:
            #     non_bottle_percent = round(mean(non_bottle_percent), 2)
            # else:
            #     non_bottle_percent = 'NA'
            #
            # cans_percent = list(filter('NA'.__ne__, cans_percent))
            # if len(cans_percent) != 0:
            #     cans_percent = round(mean(cans_percent), 2)
            # else:
            #     cans_percent = 'NA'

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


@api.route("/food_trends")
class FoodTrends(Resource):
    @api.marshal_with(food_trend_response, envelope='resource')
    @api.expect(request_parser)
    def get(self, **kwargs):
        request_args = request_parser.parse_args()

        food_values = []
        food_percentage = []
        nonfood_values = []
        nonfood_percentage = []
        timestamps = []
        request_type, start_timestamp, end_timestamp = parse_args(request_args)

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

            timestamps = [timestamp.strftime(timestamp_format) for timestamp in timestamps]

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


@api.route('/food_distribution')
class FoodDistribution(Resource):
    @api.marshal_with(food_distribution_response, envelope='resource')
    @api.expect(request_parser)
    def get(self, **kwargs):
        request_args = request_parser.parse_args()
        request_type, start_date, start_time, end_date, end_time = parse_args(request_args)

        food_distribution = []
        nonfood_distribution = []
        food_values = []
        non_food_values = []
        request_type, start_timestamp, end_timestamp = parse_args(request_args)

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


@api.route('/color_trends')
class ColorTrends(Resource):
    @api.marshal_with(color_trend_response, envelope='resource')
    @api.expect(request_parser)
    def get(self, **kwargs):
        request_args = request_parser.parse_args()
        # request_type, start_date, start_time, end_date, end_time = parse_args(request_args)

        # clear_light_blue_values, blue_and_darks_values, green_values, opaque_values, other_values,
        # clear_light_blue_percentage, blue_and_darks_percentage, green_percentage, opaque_percentage,

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

        request_type, start_timestamp, end_timestamp = parse_args(request_args)

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
            timestamps, clear_light_blue_values, blue_and_darks_values, green_values, opaque_values, other_values, \
            sleeve_clear_values, clear_light_blue_percentage, blue_and_darks_percentage, green_percentage, \
            opaque_percentage, other_percentage, sleeve_clear_percentage = list(
                map(list, zip(*results)))

            clear_light_blue_values = [x + y for x, y in zip(clear_light_blue_values, sleeve_clear_values)]
            clear_light_blue_percentage = [x + y if x != 'NA' else 'NA' for x, y in
                                           zip(clear_light_blue_percentage, sleeve_clear_percentage)]

        timestamps = [timestamp.strftime(timestamp_format) for timestamp in timestamps]

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


@api.route('/color_distribution')
class ColorDistribution(Resource):
    @api.marshal_with(color_distribution_response, envelope='resource')
    @api.expect(request_parser)
    def get(self, **kwargs):
        request_args = request_parser.parse_args()
        request_type, start_date, start_time, end_date, end_time = parse_args(request_args)

        clear_light_blue_values = []
        blue_and_darks_values = []
        green_values = []
        opaque_values = []
        other_values = []
        sleeve_clear_values = []

        request_type, start_timestamp, end_timestamp = parse_args(request_args)

        filter_query = create_filter_query(start_timestamp, end_timestamp,
                                           ['clear_light_blue_processed', 'darks_and_blue_processed',
                                            'opaque_processed', 'other_processed', 'sleeve_clear_processed',
                                            'green_processed'],
                                           db_table_name)

        print(filter_query)

        cursor.execute(filter_query)

        results = cursor.fetchall()

        if bool(results):
            clear_light_blue_values, blue_and_darks_values, green_values, opaque_values, other_values, \
            sleeve_clear_values = list(map(list, zip(*results)))

            clear_light_blue_values += sleeve_clear_values
            total_number = clear_light_blue_values + blue_and_darks_values + green_values + opaque_values + \
                           other_values + sleeve_clear_values
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


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")
