import telegram
import io
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandahouse
from datetime import date, datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


my_token = "5648317219:AAFc-J1oDURMvMVTq3NLMeOxExaI_qJcMYY"
bot = telegram.Bot(token=my_token)
chat_id = 438948435
group_id = -992300980

connection = {
    "host": "https://clickhouse.lab.karpov.courses",
    "password": "dpo_python_2020",
    "user": "student",
    "database": "simulator_20230420",
}

default_args = {
    "owner": "a-kruk",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 5, 3),
}

schedule_interval = "0 11 * * *"


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def task71_test_kruk():
    @task
    def load_daily_data():
        metrics_yesterday = """
                        SELECT 
                        COUNT(DISTINCT user_id) FILTER(WHERE time::date = today() - 1) AS DAU,
                        count(user_id) FILTER(WHERE time::date = today() - 1 AND action = 'like') AS likes,
                        count(user_id) FILTER(WHERE time::date = today() - 1 AND action = 'view') AS views,
                        ROUND(likes/ views * 100, 2) AS CTR,
                        ROUND(DAU / COUNT(DISTINCT user_id) FILTER(WHERE time::date = today() - 2) * 100, 2) AS DAU_db_ratio,
                        ROUND(likes/ count(user_id) FILTER(WHERE time::date = today() - 2 AND action = 'like') * 100, 2) AS likes_db_ratio,
                        ROUND(views/ count(user_id) FILTER(WHERE time::date = today() - 2 AND action = 'view') * 100, 2) AS views_db_ratio,
                        ROUND(CTR/
                        (count(user_id) FILTER(WHERE time::date = today() - 2 AND action = 'like') / count(user_id) FILTER(WHERE time::date = today() - 2 AND action = 'view') * 100) *100, 2) AS CTR_db_ratio
                        FROM simulator_20230420.feed_actions
                        WHERE time::date IN (today() - 1, today() -2) 
                            """
        metrics_yesterday = pandahouse.read_clickhouse(
            query=metrics_yesterday, connection=connection
        )
        return metrics_yesterday

    @task
    def send_info(df):
        msg = (
            f"On {date.today() - timedelta(days = 1)}\n\n"
            f"There were {df.DAU[0]} active users ({df.DAU_db_ratio[0]}% of the previous day).\n\n"
            f"Users liked {df.likes[0]} posts ({df.likes_db_ratio[0]}% of the previous day).\n\n"
            f"Users viewed {df.views[0]} posts ({df.views_db_ratio[0]}% of the previous day).\n\n"
            f"CTR was {df.CTR[0]} ({df.CTR_db_ratio[0]}% of the previous day)."
        )
        bot.sendMessage(chat_id=chat_id, text=msg)
        bot.sendMessage(chat_id=group_id, text=msg)

    @task
    def load_weekly_data():
        metrics_weekly = """
                    SELECT 
                    time::date AS date, 
                    COUNT(DISTINCT user_id) AS DAU,
                    countIf(user_id, action = 'like') AS likes,
                    countIf(user_id, action = 'view') AS views,
                    ROUND(likes/ views * 100, 2) AS CTR 
                    FROM simulator_20230420.feed_actions
                    WHERE time::date BETWEEN today() - 7 AND today() - 1
                    GROUP BY date
                        """
        metrics_weekly = pandahouse.read_clickhouse(
            query=metrics_weekly, connection=connection
        )
        return metrics_weekly

    @task
    def send_chart(df):
        df = df.assign(dm=df.date.dt.strftime("%d-%m"))

        today = (date.today() - timedelta(days=1)).strftime("%d.%m.%y")
        week_start = (date.today() - timedelta(days=7)).strftime("%d.%m.%y")

        fig, axes = plt.subplots(2, 2, figsize=(12, 6))

        sns.lineplot(ax=axes[0, 0], x=df.dm, y=df.DAU, color="forestgreen").set(
            xlabel=None, ylabel=None
        )
        sns.lineplot(ax=axes[0, 1], x=df.dm, y=df.CTR, color="limegreen").set(
            xlabel=None, ylabel=None
        )
        sns.lineplot(ax=axes[1, 0], x=df.dm, y=df.likes, color="mediumseagreen").set(
            xlabel=None, ylabel=None
        )
        sns.lineplot(ax=axes[1, 1], x=df.dm, y=df.views, color="mediumaquamarine").set(
            xlabel=None, ylabel=None
        )

        for axis in axes:
            for ax in axis:
                ax.set_ylim(bottom=0)

        sns.despine()

        fig.suptitle(f"Metrics {week_start} - {today}", size=13)

        axes[0, 0].set_title("DAU")
        axes[0, 1].set_title("CTR, %")
        axes[1, 0].set_title("Likes")
        axes[1, 1].set_title("Views")

        fig.tight_layout()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = "metrics.png"
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        bot.sendPhoto(chat_id=group_id, photo=plot_object)

    metrics_yesterday = load_daily_data()
    send_info(metrics_yesterday)
    metrics_weekly = load_weekly_data()
    send_chart(metrics_weekly)


task71_test_kruk = task71_test_kruk()
