from datetime import datetime, timedelta
import pandas as pd
import pandahouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20230420'
}

connection_test = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}


default_args = {
    'owner': 'a-kruk',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 3),
}

schedule_interval = '0 9 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def task6_kruk():

    @task
    def extract_feed():
        query = """
                SELECT 
                    toDate(time) as event_date, 
                    user_id, 
                    gender,
                    age,
                    os,
                    countIf(post_id, action = 'like') AS likes,
                    countIf(post_id, action = 'view') AS views
                FROM 
                    simulator_20230420.feed_actions 
                WHERE 
                    toDate(time) = toDate(now()) -1
                group by
                    event_date,
                    user_id,
                    gender,
                    age,
                    os
                """
        df_cube_feed = pandahouse.read_clickhouse(query, connection=connection)
        return df_cube_feed

    @task
    def extract_messenger():
        query = """
                SELECT
                event_date,
                user_id,
                gender,
                age,
                os,
                users_sent,
                messages_sent,
                users_received,
                messages_received
                FROM(
                    SELECT
                    toDate(time) as event_date,
                    user_id,
                    gender,
                    age,
                    os,
                    COUNT(DISTINCT reciever_id) AS users_sent,
                    COUNT(reciever_id) AS messages_sent
                    FROM
                    simulator_20230420.message_actions
                    WHERE
                    toDate(time) = toDate(now()) -1
                    group by
                    event_date,
                    user_id,
                    gender,
                    age,
                    os
                ) AS t1
                LEFT JOIN (
                    --LEFT JOIN becacuse we don`t have got full info about recievers if they didn`t send messages
                    SELECT
                    reciever_id,
                    COUNT(DISTINCT user_id) AS users_received,
                    COUNT(user_id) AS messages_received
                    FROM
                    simulator_20230420.message_actions
                    WHERE
                    toDate(time) = toDate(now()) -1
                    group by
                    reciever_id
                ) AS t2 ON t1.user_id = t2.reciever_id
                """
        df_cube_messenger = pandahouse.read_clickhouse(query, connection=connection)
        return df_cube_messenger
    
    @task
    def mergered_data(df1, df2):
        full_data = pd.merge(df1, df2, how='outer', on = ['user_id', 'event_date', 'gender', 'age', 'os']).fillna(0)
        return full_data

    @task
    def gender_slice(df):
        gender_slice = df \
                            .assign(dimension = 'gender') \
                            .rename(columns = {'gender':'dimension_value'}) \
                            .groupby(['event_date', 'dimension', 'dimension_value'], as_index = False) \
                            .agg({'views':'sum',
                                'likes':'sum',
                                'messages_received':'sum',
                                'messages_sent':'sum',
                                'users_received':'sum',
                                'users_sent':'sum'}) 
        return gender_slice

    @task
    def os_slice(df):
        os_slice = df \
                        .assign(dimension = 'os') \
                        .rename(columns = {'os':'dimension_value'}) \
                        .groupby(['event_date', 'dimension', 'dimension_value'], as_index = False) \
                        .agg({'views':'sum',
                            'likes':'sum',
                            'messages_received':'sum',
                            'messages_sent':'sum',
                            'users_received':'sum',
                            'users_sent':'sum'}) 
        return os_slice

    @task
    def age_slice(df):
        age_slice = df \
                    .assign(dimension = 'age') \
                    .rename(columns = {'age':'dimension_value'}) \
                    .groupby(['event_date', 'dimension', 'dimension_value'], as_index = False) \
                    .agg({'views':'sum',
                        'likes':'sum',
                        'messages_received':'sum',
                        'messages_sent':'sum',
                        'users_received':'sum',
                        'users_sent':'sum'}) 
        return age_slice

    @task
    def concatenated_data(df1, df2, df3):
        data = pd.concat([df1, df2, df3]).astype({'views': 'int', 'likes': 'int', 'messages_received': 'int', 'messages_sent': 'int', 'users_received': 'int', 'users_sent': 'int'})

        return data

    @task
    def upload(df):
        query = """
                CREATE TABLE IF NOT EXISTS test.a_kruk (
                    event_date Date,
                    dimension String,
                    dimension_value String,
                    views UInt32,
                    likes UInt32,
                    messages_received UInt32,
                    messages_sent UInt32,
                    users_received UInt32,
                    users_sent UInt32
                ) ENGINE = MergeTree ORDER BY event_date
        """
        pandahouse.execute(query, connection = connection_test)
        pandahouse.to_clickhouse(df, table='a_kruk', index=False, connection=connection_test)

    df_cube_feed = extract_feed()
    df_cube_messenger = extract_messenger()
    full_data = mergered_data(df_cube_feed, df_cube_messenger)
    gender_slice = gender_slice(full_data)
    os_slice = os_slice(full_data)
    age_slice = age_slice(full_data)
    concatenated_data = concatenated_data(gender_slice, os_slice, age_slice)
    upload(concatenated_data)

task6_kruk = task6_kruk()



