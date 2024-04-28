import requests
import time
import psycopg2
from datetime import datetime, timedelta, timezone

db_table_name = 'aggregate_new'
timestamp_format = "%d%m%Y-%H%M"
timestamp_format_trends = "%d%m%Y-%H%M%S"


def alert_message():
    api_key = "b0b0e31cdaf3fbbe40c7279c618aa4fe-b0ed5083-38ad4f33"
    domain_name = "sandbox7294f39fc64f4e88ba8d1f0bdb4a0d5f.mailgun.org"
    # print('drawn participant :\n', text_string)
    return requests.post(
        "https://api.mailgun.net/v3/{}/messages".format(domain_name),
        auth=("api", api_key),
        data={"from": "Mailgun <mailgun@vinglabs.com>",
              "to": ["vinglabs", "contact@vinglabs.com"],
              "subject": "PRT System - Stopped working!",
              "text": "Kuch fat gaya bhai!!\nCheck kar lo!"})


def main():
    last_fail_time = False
    while True:
        try:
            cursor = psycopg2.connect(
                database="prt-db",
                user="postgres",
                password="XXXXXXXX",
                host="prt-prod.cmuk1jsd4tyu.ap-south-1.rds.amazonaws.com",
                port='5432').cursor()

            query = "SELECT " + 'datetime' + " FROM " + db_table_name + " order by datetime desc limit 10"
            # print(query)
            try:
                cursor.execute(query)
            except Exception as err:
                print(err)
            results = cursor.fetchall()
            if bool(results):
                timestamps = list(map(list, zip(*results)))
                # print(timestamps[0][0])
                current_datetime = datetime.now(timezone.utc) + timedelta(hours=2)
                delta = current_datetime.replace(tzinfo=None) - timestamps[0][0].replace(tzinfo=None)
                # print(delta)
                if delta > timedelta(minutes=2):
                    fail_time = timestamps[0][0]
                    if not last_fail_time:
                        last_fail_time = fail_time
                        alert_message()
                        print('{} : kaand ho gaya.., Current time :{} '.format(timestamps[0][0], current_datetime))
                    else:
                        if last_fail_time == fail_time:
                            print('Already intimated! , Current time :{}'.format(timestamps[0][0], current_datetime))
                        else:
                            alert_message()
                            print('{} : kaand ho gaya.., Current time :{} '.format(timestamps[0][0], current_datetime))
                            last_fail_time = fail_time
                else:
                    print('{} : Sab changa si..'.format(timestamps[0][0]))

            time.sleep(10)
        except Exception as e:
            print('Failed due to error {}'.format(e))
            time.sleep(5)


if __name__ == '__main__':
    main()
