from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import pytz

#define the access to the database
auth_db = psycopg2.connect(
    dbname="auth",
    user="postgres",
    password="@Nedap1929",
    host="194.164.175.63",
    port="30100"
)
data_db = psycopg2.connect(
    dbname="data",
    user="postgres",
    password="@Nedap1929",
    host="194.164.175.63",
    port="30100"
)

def start_of_week(date):
    # Assuming the week starts on Monday
    start = date - timedelta(days=date.weekday())
    return start

def cron_location():
    date = datetime.now(pytz.utc)
    print('Start receiving data at', date)
    auth_cursor = auth_db.cursor()

    auth_cursor.execute('SELECT * FROM scopes')
    organizations = auth_cursor.fetchall()

    for org in organizations:
        org_name, org_id, org_token = org
        print(org_name, org_id)

        data_cursor = data_db.cursor()

        data_cursor.execute(f'SELECT last_entry FROM "{org_id}_cron" WHERE cron_name = %s ORDER BY last_entry DESC LIMIT 1', ('location',))
        last_entry = data_cursor.fetchone()

        if not last_entry:
            print('no register')
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'location', '1', date))
        else:
            print('register found')
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'location', '1', date))

        try:
            response = requests.get(
                'https://api.nedapretail.com/organization/v2/list_stores',
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {org_token}'
                },
                params={
                    'fields[]': ['location', 'store_type', 'name', 'address']
                }
            )

            if response.status_code != 200:
                print(response.json())
            else:
                receive_data = response.json()
                print('receiveData:', len(receive_data))

                for location in receive_data:
                    query = f"""
                    INSERT INTO "{org_id}_location" (location_id, type, country, active)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (location_id) DO UPDATE SET
                    type = EXCLUDED.type, country = EXCLUDED.country, active = EXCLUDED.active;
                    """
                    data_cursor.execute(query, (location['location'], location['store_type'], location['address']['country_code'], '1'))

                update_query = f"""
                UPDATE "{org_id}_cron" SET last_entry = %s, cron_active = '0'
                WHERE cron_name = 'location' AND cron_time = %s
                """
                data_cursor.execute(update_query, (date, date))

        except requests.RequestException as e:
            print('Error receiving data:', e)

        data_db.commit()
        data_cursor.close()
        data_db.close()

    auth_cursor.close()
    auth_db.close()

def cron_receive():
    date = datetime.now(pytz.utc)
    print('Start receiving data at', date)
    auth_cursor = auth_db.cursor()

    auth_cursor.execute('SELECT * FROM scopes')
    organizations = auth_cursor.fetchall()

    for org in organizations:
        org_name, org_id, org_token = org
        print(org_name, org_id)

        data_cursor = data_db.cursor()

        data_cursor.execute(f'SELECT last_entry FROM "{org_id}_cron" WHERE cron_name = %s ORDER BY last_entry DESC LIMIT 1', ('receive',))
        last_entry = data_cursor.fetchone()
        last_entry_date = datetime(2023, 1, 1, tzinfo=pytz.utc)

        if not last_entry:
            print('no register')
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'receive', '1', last_entry_date))
        else:
            print('register found')
            last_entry_date = last_entry[0]
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'receive', '1', last_entry_date))

        has_more = True
        next_cursor = None
        last_data = None

        try:
            while has_more:
                body = {
                    'use_cursor': True,
                    'parameters': [
                        {'name': 'GE_eventTime', 'value': last_entry_date.isoformat()},
                        {'name': 'EQ_bizStep', 'value': 'urn:epcglobal:cbv:bizstep:receiving'}
                    ],
                    'from_cursor': next_cursor
                }
                response = requests.post(
                    'https://api.nedapretail.com/epcis/v3/query',
                    headers={
                        'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {org_token}'
                    },
                    json=body
                )

                if response.status_code != 200:
                    print(response.json())
                else:
                    receive_data = response.json()
                    print('receiveData:', len(receive_data['events']))

                    for event in receive_data['events']:
                        event_time = datetime.fromisoformat(event['event_time']).replace(tzinfo=pytz.utc)
                        first_day_of_week = start_of_week(event_time).isoformat()
                        avg_items = len(event['child_epcs'])
                        last_data = event['event_time']

                        query = f"""
                        INSERT INTO "{org_id}_receive" (wks_start_date, boxes, items)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (wks_start_date) DO UPDATE SET
                        boxes = "{org_id}_receive".boxes + 1,
                        items = ((("{org_id}_receive".boxes * "{org_id}_receive".items) + %s) / ("{org_id}_receive".boxes + 1))
                        """
                        data_cursor.execute(query, (first_day_of_week, 1, avg_items, avg_items))

                    has_more = receive_data['has_more']
                    next_cursor = receive_data['next_cursor']

            if not has_more and last_data:
                last_entry_date = last_data
                update_query = f"""
                UPDATE "{org_id}_cron" SET last_entry = %s, cron_active = '0'
                WHERE cron_name = 'receive' AND cron_time = %s
                """
                data_cursor.execute(update_query, (last_entry_date, date))

        except requests.RequestException as e:
            print('Error receiving data:', e)

        data_db.commit()  
        data_cursor.close()
        data_db.close()

    auth_cursor.close()
    auth_db.close()

def cron_checkout():
    date = datetime.now(pytz.utc)
    print('Start receiving data at', date)

    auth_cursor = auth_db.cursor()

    auth_cursor.execute('SELECT * FROM scopes')
    organizations = auth_cursor.fetchall()

    for org in organizations:
        org_name, org_id, org_token = org
        print(org_name, org_id)

        data_cursor = data_db.cursor()

        data_cursor.execute(f'SELECT last_entry FROM "{org_id}_cron" WHERE cron_name = %s ORDER BY last_entry DESC LIMIT 1', ('checkout',))
        last_entry = data_cursor.fetchone()
        last_entry_date = datetime(2023, 6, 6, tzinfo=pytz.utc)

        if not last_entry:
            print('no register')
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'checkout', '1', last_entry_date))
        else:
            print('register found')
            last_entry_date = last_entry[0]
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'checkout', '1', last_entry_date))

        has_more = True
        next_cursor = None
        last_data = None

        try:
            while has_more:
                body = {
                    'use_cursor': True,
                    'parameters': [
                        {'name': 'GE_eventTime', 'value': last_entry_date.isoformat()},
                        {'name': 'EQ_bizStep', 'value': 'urn:epcglobal:cbv:bizstep:retail_selling'}
                    ],
                    'from_cursor': next_cursor
                }
                response = requests.post(
                    'https://api.nedapretail.com/epcis/v3/query',
                    headers={
                        'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {org_token}'
                    },
                    json=body
                )

                if response.status_code != 200:
                    print(response.json())
                else:
                    receive_data = response.json()
                    print('receiveData:', len(receive_data['events']))

                    for event in receive_data['events']:
                        event_time = datetime.fromisoformat(event['event_time']).replace(tzinfo=pytz.utc)
                        first_day_of_week = start_of_week(event_time).isoformat()
                        avg_items = len(event['epc_list'])
                        last_data = event['event_time']
                        store = event['biz_location']

                        if event['disposition'] == 'urn:epcglobal:cbv:disp:retail_sold':
                            query = f"""
                            INSERT INTO "{org_id}_checkout" (wks_start_date_store, wks_start_date, sold, basket, store)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (wks_start_date_store) DO UPDATE SET
                            sold = "{org_id}_checkout".sold + 1,
                            basket = ((("{org_id}_checkout".sold * "{org_id}_checkout".basket) + %s) / ("{org_id}_checkout".sold + 1))
                            """
                            data_cursor.execute(query, (f'{first_day_of_week}L{store}', first_day_of_week, 1, avg_items, store, avg_items))

                    has_more = receive_data['has_more']
                    next_cursor = receive_data['next_cursor']

            if not has_more and last_data:
                last_entry_date = last_data
                update_query = f"""
                UPDATE "{org_id}_cron" SET last_entry = %s, cron_active = '0'
                WHERE cron_name = 'checkout' AND cron_time = %s
                """
                data_cursor.execute(update_query, (last_entry_date, date))

        except requests.RequestException as e:
            print('Error receiving data:', e)

        data_db.commit()  
        data_cursor.close()
        data_db.close()

    auth_cursor.close()
    auth_db.close()

def cron_count():
    date = datetime.now(pytz.utc)
    print('Start receiving data at', date)

    auth_cursor = auth_db.cursor()

    auth_cursor.execute('SELECT * FROM scopes')
    organizations = auth_cursor.fetchall()

    for org in organizations:
        org_name, org_id, org_token = org
        print(org_name, org_id)

        data_cursor = data_db.cursor()

        data_cursor.execute(f'SELECT last_entry FROM "{org_id}_cron" WHERE cron_name = %s ORDER BY last_entry DESC LIMIT 1', ('count',))
        last_entry = data_cursor.fetchone()
        last_entry_date = datetime(2000, 1, 1, tzinfo=pytz.utc)

        if not last_entry:
            print('no register')
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'count', '1', last_entry_date))
        else:
            print('register found')
            last_entry_date = last_entry[0]
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'count', '1', last_entry_date))

        data_cursor.execute(f'SELECT * FROM "{org_id}_location"')
        locations = data_cursor.fetchall()

        for location in locations:
            location_id = location[0]
            try:
                response = requests.get(
                    f'https://api.nedapretail.com/rfid_count/v1/list?location={location_id}&from_event_time={last_entry_date.isoformat()}&include_partial_counts=false',
                    headers={
                        'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {org_token}'
                    }
                )

                if response.status_code != 200:
                    error = response.json()
                    if 'requires at least one active subscription' in error.get('reason', ''):
                        data_cursor.execute(f'UPDATE "{org_id}_location" SET active = %s WHERE location_id = %s', ('0', location_id))
                    print(error)
                else:
                    receive_data = response.json()
                    print('receiveData:', len(receive_data))

                    for count in receive_data:
                        query = f"""
                        INSERT INTO "{org_id}_count" (id, location, event_time, quantity)
                        VALUES (gen_random_uuid(), %s, %s, %s)
                        """
                        data_cursor.execute(query, (count['location'], count['event_time'], count['quantity']))

            except requests.RequestException as e:
                print('Error receiving data:', e)

        update_query = f"""
        UPDATE "{org_id}_cron" SET last_entry = %s, cron_active = '0'
        WHERE cron_name = 'count' AND cron_time = %s
        """
        data_cursor.execute(update_query, (date, date))

        data_db.commit()
        data_cursor.close()
        data_db.close()

    auth_cursor.close()
    auth_db.close()

def cron_osa():
    date = datetime.now(pytz.utc)
    print('Start receiving data at', date)

    auth_cursor = auth_db.cursor()

    auth_cursor.execute('SELECT * FROM scopes')
    organizations = auth_cursor.fetchall()

    for org in organizations:
        org_name, org_id, org_token = org
        print(org_name, org_id)

        data_cursor = data_db.cursor()

        data_cursor.execute(f'SELECT last_entry FROM "{org_id}_cron" WHERE cron_name = %s ORDER BY last_entry DESC LIMIT 1', ('osa',))
        last_entry = data_cursor.fetchone()
        last_entry_date = datetime(2023, 6, 6, tzinfo=pytz.utc)

        if not last_entry:
            print('no register')
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'osa', '1', last_entry_date))
        else:
            print('register found')
            last_entry_date = last_entry[0]
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'osa', '1', last_entry_date))

        data_cursor.execute(f'SELECT location_id FROM "{org_id}_location" WHERE active = %s AND type = %s', ('1', 'fashion'))
        locations = data_cursor.fetchall()

        for location in locations:
            location_id = location[0]
            from_week = last_entry_date.strftime('%Y-%U')
            to_week = date.strftime('%Y-%U')

            try:
                response = requests.get(
                    f'https://api.nedapretail.com/kpi/v2/on_shelf_availability?location={location_id}&from_week={from_week}&to_week={to_week}',
                    headers={
                        'Accept': 'application/json',
                        'Content-Type': 'application/json',
                        'Authorization': f'Bearer {org_token}'
                    }
                )

                if response.status_code != 200:
                    print(response.text)
                else:
                    receive_data = response.json()

                    for data in receive_data['data']:
                        if not data['on_shelf_availability_kpi_value']['on_shelf_availability']:
                            continue

                        week = data['week_info']['week']
                        year = data['week_info']['year']
                        osa = data['on_shelf_availability_kpi_value']['on_shelf_availability']
                        first_day_of_week = datetime.strptime(f'{year}-{week}-1', "%Y-%W-%w").isoformat()

                        query = f"""
                        INSERT INTO "{org_id}_osa" (store_date, wks_start_date, week_number, year, store, osa)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (store_date) DO UPDATE SET osa = EXCLUDED.osa
                        """
                        data_cursor.execute(query, (f'{first_day_of_week}L{location_id}', first_day_of_week, week, year, location_id, osa))

            except requests.RequestException as e:
                print('Error receiving data:', e)

        update_query = f"""
        UPDATE "{org_id}_cron" SET last_entry = %s, cron_active = '0'
        WHERE cron_name = 'osa' AND cron_time = %s
        """
        data_cursor.execute(update_query, (last_entry_date, date))

        data_db.commit()
        data_cursor.close()
        data_db.close()

    auth_cursor.close()
    auth_db.close()

def cron_summary():
    date = datetime.now(pytz.utc)
    print('Start receiving data at', date)

    auth_cursor = auth_db.cursor()

    auth_cursor.execute('SELECT * FROM scopes')
    organizations = auth_cursor.fetchall()

    for org in organizations:
        org_name, org_id, org_token = org
        print(org_name, org_id)

        data_cursor = data_db.cursor()

        data_cursor.execute(f'SELECT last_entry FROM "{org_id}_cron" WHERE cron_name = %s ORDER BY last_entry DESC LIMIT 1', ('summary',))
        last_entry = data_cursor.fetchone()
        last_entry_date = datetime(2000, 1, 1, tzinfo=pytz.utc)

        if not last_entry:
            print('no register')
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'summary', '1', last_entry_date))
        else:
            print('register found')
            last_entry_date = last_entry[0]
            data_cursor.execute(f'INSERT INTO "{org_id}_cron" (cron_time, cron_name, cron_active, last_entry) VALUES (%s, %s, %s, %s)', (date, 'summary', '1', last_entry_date))

        data_cursor.execute(f'SELECT count(*) FROM "{org_id}_location" WHERE active = %s AND type = %s', ('1', 'fashion'))
        active_stores = data_cursor.fetchone()[0]
        print('activeStores:', active_stores)

        data_cursor.execute(f"""
            SELECT SUM(boxes) AS sum_boxes, AVG(items) AS average_items
            FROM "{org_id}_receive"
            WHERE wks_start_date >= NOW() - INTERVAL '1 year'
        """)
        res_boxes_received = data_cursor.fetchone()
        sum_boxes = res_boxes_received[0] or 0
        average_items = res_boxes_received[1] or 0
        print('sumBoxes:', sum_boxes)
        print('averageItems:', average_items)

        data_cursor.execute(f"""
            SELECT SUM(sold) AS sold_items, AVG(basket) AS avg_basket
            FROM "{org_id}_checkout"
            WHERE wks_start_date >= NOW() - INTERVAL '1 year'
        """)
        res_sold_items = data_cursor.fetchone()
        sold_items = res_sold_items[0] or 0
        avg_basket = res_sold_items[1] or 0
        print('soldItems:', sold_items)
        print('avgBasket:', avg_basket)

        query = f"""
        INSERT INTO "{org_id}_config" (source, active_stores, boxes_received, avg_items_received, items_sold_per_year, average_basked_size)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (source) DO UPDATE SET
        active_stores = EXCLUDED.active_stores,
        boxes_received = EXCLUDED.boxes_received,
        avg_items_received = EXCLUDED.avg_items_received,
        items_sold_per_year = EXCLUDED.items_sold_per_year,
        average_basked_size = EXCLUDED.average_basked_size
        """
        data_cursor.execute(query, ('idcloud', active_stores, sum_boxes, round(average_items), sold_items, round(avg_basket)))

        update_query = f"""
        UPDATE "{org_id}_cron" SET last_entry = %s, cron_active = '0'
        WHERE cron_name = 'summary' AND cron_time = %s
        """
        data_cursor.execute(update_query, (last_entry_date, date))

        data_db.commit()
        data_cursor.close()
        data_db.close()

    auth_cursor.close()
    auth_db.close()

with DAG(
    dag_id='etl_dag',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    cron_location_task = PythonOperator(
        task_id='cron_location',
        python_callable=cron_location
    )

    cron_receive_task = PythonOperator(
        task_id='cron_receive',
        python_callable=cron_receive
    )

    cron_checkout_task = PythonOperator(
        task_id='cron_checkout',
        python_callable=cron_checkout
    )

    cron_count_task = PythonOperator(
        task_id='cron_count',
        python_callable=cron_count
    )

    cron_osa_task = PythonOperator(
        task_id='cron_osa',
        python_callable=cron_osa
    )

    cron_summary_task = PythonOperator(
        task_id='cron_summary',
        python_callable=cron_summary
    )

    # Define task dependencies without etl_task
    [cron_location_task, cron_receive_task, cron_checkout_task, cron_count_task, cron_osa_task, cron_summary_task]
    
