from datetime import datetime, timedelta, timezone
import random


def get_timestamps(start_date, start_time, end_date, end_time, least_count):
    timestamps = []
    start_timestamp = datetime.strptime(start_date + "-" + start_time, "%d%m%Y-%H%M")
    end_timestamp = datetime.strptime(end_date + "-" + end_time, "%d%m%Y-%H%M")
    current_timestamp = start_timestamp
    while current_timestamp < end_timestamp:
        current_timestamp = current_timestamp + timedelta(seconds=least_count)
        timestamps.append(current_timestamp.strftime("%d%m%Y-%H%M%S"))
    return timestamps


def get_random_bottle_trends(timestamps):
    bottle_values = []
    bottle_percent = []
    non_bottle_values = []
    non_bottle_percent = []
    can_values = []
    for timestamp in timestamps:
        if random.random() > 0.7:
            bottle_values.append(random.randint(50, 70))
            non_bottle_values.append(random.randint(15, 30))
            bottle_percent.append(bottle_values[-1] * 100 // (bottle_values[-1] + non_bottle_values[-1]))
            non_bottle_percent.append(100 - bottle_percent[-1])
        else:
            bottle_value = random.randint(0, 50)
            bottle_values.append(bottle_value)
            non_bottle_values.append(random.randint(0, int(0.3 * bottle_value)))
            if bottle_values[-1] + non_bottle_values[-1] != 0:
                bottle_percent.append(bottle_values[-1] * 100 // (bottle_values[-1] + non_bottle_values[-1]))
                non_bottle_percent.append(100 - bottle_percent[-1])
            else:
                bottle_percent.append(0)
                non_bottle_percent.append(0)

        can_values.append(random.randint(0, 5))

    return bottle_values, non_bottle_values, can_values, bottle_percent, non_bottle_percent


def get_random_food_trends(timestamps):
    food_values = []
    nonfood_values = []
    food_percentage = []
    nonfood_percentage = []
    for timestamp in timestamps:
        if random.random() > 0.7:
            food_values.append(random.randint(50, 70))
            nonfood_values.append(random.randint(3, 7))
            food_percentage.append(food_values[-1] * 100 // (food_values[-1] + nonfood_values[-1]))
            nonfood_percentage.append(100 - food_percentage[-1])
        else:
            food_value = random.randint(0, 50)
            food_values.append(food_value)
            nonfood_values.append(random.randint(0, int(0.05 * food_value)))
            if food_values[-1] + nonfood_values[-1] != 0:
                food_percentage.append(food_values[-1] * 100 // (food_values[-1] + nonfood_values[-1]))
                nonfood_percentage.append(100 - food_percentage[-1])
            else:
                food_percentage.append(0)
                nonfood_percentage.append(0)
    return food_values, nonfood_values, food_percentage, nonfood_percentage


def get_random_color_trends(timestamps):
    clear_light_blue_values = []
    blue_and_darks_values = []
    green_values = []
    opaque_values = []
    other_values = []
    clear_light_blue_percentage = []
    blue_and_darks_percentage = []
    green_percentage = []
    opaque_percentage = []
    other_percentage = []
    for timestamp in timestamps:
        clear_light_blue_values.append(random.randint(3240, 3456))
        blue_and_darks_values.append(random.randint(0, 18))
        other_values.append(random.randint(0, 18))
        opaque_values.append(random.randint(0, 54))
        green_values.append(
            3600 - (clear_light_blue_values[-1] + blue_and_darks_values[-1] + other_values[-1] + opaque_values[-1]))
        clear_light_blue_percentage.append(round(clear_light_blue_values[-1] * 100 / (
                clear_light_blue_values[-1] + green_values[-1] + other_values[-1] + opaque_values[-1] +
                blue_and_darks_values[-1]), 2))
        blue_and_darks_percentage.append(round(blue_and_darks_values[-1] * 100 / (
                clear_light_blue_values[-1] + green_values[-1] + other_values[-1] + opaque_values[-1] +
                blue_and_darks_values[-1]), 2))
        opaque_percentage.append(round(opaque_values[-1] * 100 / (
                clear_light_blue_values[-1] + green_values[-1] + other_values[-1] + opaque_values[-1] +
                blue_and_darks_values[-1]), 2))
        green_percentage.append(round(green_values[-1] * 100 / (
                clear_light_blue_values[-1] + green_values[-1] + other_values[-1] + opaque_values[-1] +
                blue_and_darks_values[-1]), 2))
        other_percentage.append(round(other_values[-1] * 100 / (
                clear_light_blue_values[-1] + green_values[-1] + other_values[-1] + opaque_values[-1] +
                blue_and_darks_values[-1]), 2))

    return clear_light_blue_values, blue_and_darks_values, green_values, opaque_values, other_values, clear_light_blue_percentage, blue_and_darks_percentage, green_percentage, opaque_percentage, other_percentage


def parse_args(args):
    request_type = args['type']
    start_time = args['start_time']
    end_time = args['end_time']
    start_date = args['start_date']
    end_date = args['end_date']

    if request_type == 'live':
        # start_date = (current_datetime - timedelta(hours=2)).strftime("%d%m%Y")
        # start_time = (current_datetime - timedelta(hours=2)).strftime("%H%M")
        # end_date = current_datetime.strftime("%d%m%Y")
        # end_time = current_datetime.strftime("%H%M")
        current_datetime = datetime.now(timezone.utc) + timedelta(hours=2)
        start_timestamp = current_datetime - timedelta(minutes=90)
        end_timestamp = current_datetime

        current_datetime = datetime(2022, 10, 25, 9, 0, tzinfo=timezone.utc)
        start_timestamp = current_datetime - timedelta(minutes=90)
        end_timestamp = current_datetime
    else:
        start_timestamp = datetime.strptime(start_date + "-" + start_time, "%d%m%Y-%H%M")
        end_timestamp = datetime.strptime(end_date + "-" + end_time, "%d%m%Y-%H%M")

    # return request_type,start_date,start_time,end_date,end_time
    return request_type, start_timestamp, end_timestamp


def parse_args_for_pdf(start_date):
    start_time = '000000'
    end_time = '235959'
    end_date = start_date
    start_timestamp = datetime.strptime(start_date + "-" + start_time, "%d%m%Y-%H%M%S")
    end_timestamp = datetime.strptime(end_date + "-" + end_time, "%d%m%Y-%H%M%S")

    # return request_type,start_date,start_time,end_date,end_time
    return start_timestamp, end_timestamp


def get_random_bottle_distribution():
    cans_distribution = round(random.uniform(0.1, 0.5), 2)
    bottle_distribution = random.randint(65, 99)
    nonbottle_distribution = round(100 - cans_distribution - bottle_distribution, 2)

    return cans_distribution, bottle_distribution, nonbottle_distribution


def get_random_food_distribution():
    food_distribution = round(random.uniform(90, 98), 2)
    nonfood_distribution = round(100 - food_distribution, 2)

    return food_distribution, nonfood_distribution


def get_random_color_distribution():
    clear_light_blue_distribution = round(random.uniform(90, 96), 2)
    blue_and_darks_distribution = round(random.uniform(0, 0.5), 2)
    opaque_distribution = round(random.uniform(0, 1.5), 2)
    other_distribution = round(random.uniform(0, 0.5), 2)
    green_distribution = round(
        100 - clear_light_blue_distribution - blue_and_darks_distribution - opaque_distribution - other_distribution, 2)

    return clear_light_blue_distribution, blue_and_darks_distribution, opaque_distribution, other_distribution, green_distribution


def create_filter_query(start_timestamp, end_timestamp, columns_list, table_name):
    columns = ",".join(columns_list)
    start_timestamp = datetime.strftime(start_timestamp, "%Y-%m-%d %H:%M:%S")
    end_timestamp = datetime.strftime(end_timestamp, "%Y-%m-%d %H:%M:%S")

    query = "SELECT " + columns + " FROM " + table_name + " where datetime BETWEEN '" + start_timestamp + "' AND '" + end_timestamp + "'" + " ORDER BY datetime"

    return query
