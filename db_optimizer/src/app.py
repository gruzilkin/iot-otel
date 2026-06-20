import os, asyncio
import psycopg2, psycopg2.extras

import numpy as np
import pandas as pd

import heapq as hq

import time

def create_connection():
    dbname = os.environ['DB_NAME']
    user = os.environ['DB_USER']
    password = os.environ['DB_PASSWORD']
    host = os.environ['DB_HOST']
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host)


def fetch_devices(connection):
	with connection.cursor() as cursor:
		cursor.execute(f"""SELECT DISTINCT device_id, sensor_name FROM sensor_data""")
		return [(row[0], row[1]) for row in cursor.fetchall()]


def fetch_tail(connection, device_id, sensor_name, include_timestamp=False):
	with connection.cursor() as cursor:
		sql = f"""WITH last_received_at AS (
					SELECT MAX(received_at) as last_received_at
					FROM sensor_data
					JOIN sensor_data_weights USING (id)
					WHERE device_id = %s AND sensor_name = %s
				), last_peak AS (
					SELECT *
					FROM sensor_data
					JOIN sensor_data_weights USING (id)
					WHERE device_id = %s AND sensor_name = %s
					AND received_at > (SELECT last_received_at - interval '15 minute'  FROM last_received_at)
					ORDER BY weight DESC
					LIMIT 1
				)
				SELECT id, sensor_value {', received_at ' if include_timestamp else ''}
				FROM sensor_data
				WHERE device_id = %s AND sensor_name = %s AND id >= COALESCE((SELECT id FROM last_peak), 0)
				ORDER BY received_at ASC"""
		cursor.execute(sql, (device_id, sensor_name, device_id, sensor_name, device_id, sensor_name))
		
		columns = ['id', 'sensor_value']
		if include_timestamp:
			columns.append('received_at')
		return pd.DataFrame.from_records(cursor.fetchall(), index=['id'], columns=columns)
      
def fetch(connection, device_id, sensor_name, include_timestamp=False):
	with connection.cursor() as cursor:
		sql = f"""SELECT id, sensor_value {', received_at ' if include_timestamp else ''} 
			FROM sensor_data
			WHERE device_id = %s AND sensor_name = %s
			ORDER BY received_at ASC"""
		cursor.execute(sql, (device_id, sensor_name))
		
		columns = ['id', 'sensor_value']
		if include_timestamp:
			columns.append('received_at')
		return pd.DataFrame.from_records(cursor.fetchall(), index=['id'], columns=columns)


def remove_old_data(connection):
	with connection.cursor() as cursor:
		cursor.execute(f"""DELETE FROM sensor_data
		WHERE received_at < now() - interval '1 week'""")


def update_weight(connection, series, overwrite_with_higher_weights_only = True):
	with connection.cursor() as cursor:
		sql = f"""INSERT INTO sensor_data_weights (id, weight) VALUES %s
		ON CONFLICT (id) DO UPDATE SET weight = EXCLUDED.weight
		{'WHERE sensor_data_weights.weight < EXCLUDED.weight' if overwrite_with_higher_weights_only else ''}"""
		data = [(id, weight) for id, weight in series.items()]
		psycopg2.extras.execute_values(cursor, sql, data)


def calculate_weights(data, ratio = 1):
    y = data
    x = np.arange(len(y))
    indeces = {0:0, len(y)-1:0}

    processed = 2
    limit = max(10, int(len(data) * ratio))

    queue = []
    hq.heappush(queue, (0, (0, len(y)-1)))

    while queue and processed < limit:
        _, (left, right) = hq.heappop(queue)

        if right - left == 1:
            continue

        y_range = y[left:right + 1]
        x_range = x[left:right + 1]
        
        x1, y1, x2, y2 = x_range[0], y_range[0], x_range[-1], y_range[-1]
        a = (y2 - y1) / (x2 - x1)
        b = -x1 * (y2 - y1) / (x2 - x1) + y1
        y_hat = a*x_range + b
        diff = np.abs(y_range - y_hat)
        diff = diff[1:-1]

        i = np.argmax(diff)
        error = diff[i]
        i += left + 1

        indeces[i] = error
        hq.heappush(queue, (-error, (left, i)))
        hq.heappush(queue, (-error, (i, right)))
        processed += 1 

    indeces = dict(sorted(indeces.items(), key=lambda item: item[0]))
    return np.array([indeces[x] if x in indeces.keys() else 0 for x in x])


def calculate_weights_for_series(series):
    data = series.to_numpy()
    start = time.time()
    weight = calculate_weights(data, ratio = 1)
    end = time.time()
    print(f"weight calculation took {end-start:.2f}")

    return pd.Series(index=series.index, data=weight)


def process_weights(recalculate = True):
    with create_connection() as connection:
        remove_old_data(connection)
        sensor_streams = fetch_devices(connection)
        for device_id, sensor_name in sensor_streams:
            if recalculate:
                df = fetch(connection, device_id, sensor_name)
            else:
                df = fetch_tail(connection, device_id, sensor_name)
            
            print(f"{device_id}/{sensor_name} tail length {len(df)}")
            
            df.sensor_value = df.sensor_value.astype(float)

            if len(df.sensor_value) == 0:
                    continue
            
            weights = calculate_weights_for_series(df.sensor_value)
            weights = weights[weights > 0]
            update_weight(connection, weights, overwrite_with_higher_weights_only = not recalculate)
        connection.commit()


async def calculate_tail_worker():
    while True:
        process_weights(recalculate = False)
        await asyncio.sleep(60)

async def recalculate_worker():
    while True:
        process_weights(recalculate = True)
        await asyncio.sleep(60 * 60)


async def main():
    try:
        coros = [calculate_tail_worker(), recalculate_worker()]
        tasks = [asyncio.create_task(coro) for coro in coros]
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == '__main__': 
    asyncio.run(main())