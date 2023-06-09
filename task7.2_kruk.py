import io
from datetime import date, datetime, timedelta

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandahouse
import seaborn as sns
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

my_token = "5648317219:AAFc-J1oDURMvMVTq3NLMeOxExaI_qJcMYY"
bot = telegram.Bot(token=my_token)
#chat_id = 438948435
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
    "start_date": datetime(2023, 5, 10),
}

schedule_interval = "0 11 * * *"


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def task72_kruk():
    @task
    def load_involvement1():
        q = """
            SELECT
            count(user_id) FILTER(WHERE involvement = 1) AS superactive,
            count(user_id) AS all_users,
            ROUND(quantile(involvement), 2) AS median_involvement,
            ROUND(superactive/all_users*100, 2) AS superactive_p
            FROM(
            SELECT
            user_id, min(time) as first_use, max(time) as last_use, max(time)::date - min(time)::date + 1 AS usage,
            uniqExact(time::date) AS active, active / usage AS involvement

            FROM (
            SELECT
            user_id, time, action
            FROM simulator_20230420.feed_actions
            UNION ALL
            SELECT
            user_id, time, toString(reciever_id) AS action
            FROM simulator_20230420.message_actions)
            GROUP BY user_id)
            """
        involvement1 = pandahouse.read_clickhouse(query=q, connection=connection)
        return involvement1



    @task
    def load_involvement2():
        q = """
            SELECT
            date,
            count(distinct user_id) AS users,
            sum(likes) + SUM(views) + SUM(messages) AS all_actions,
            ROUND(all_actions/users, 2) AS actions_peruser,
            count(user_id) FILTER(WHERE messages!=0 AND views!=0) AS feed_mes,
            ROUND(feed_mes/users*100, 2) feed_mes_p,
            count(user_id) FILTER(WHERE messages!=0 AND views=0) AS only_mes,
            ROUND(only_mes/users*100, 2) AS only_mes_p
            FROM(
            SELECT
            date, user_id,
            countIf(action = 'like') AS likes,
            countIf(action = 'view') AS views,
            countIf(action NOT IN('like', 'view')) AS messages
            FROM (
            SELECT
            user_id, time::date AS date, action
            FROM simulator_20230420.feed_actions
            WHERE date IN (today() - 1, today() -2)
            UNION ALL
            SELECT
            user_id, time::date AS date, toString(reciever_id) AS action
            FROM simulator_20230420.message_actions
            WHERE date IN (today() - 1, today() -2))
            GROUP BY date, user_id)
            GROUP BY date
            """
        involvement2 = pandahouse.read_clickhouse(query=q, connection=connection)
        return involvement2


    @task
    def send_involvement(df1, df2):
        yesterday = date.today() - timedelta(days=1)
        msg = (
            f"{df1['superactive_p'][0]}% of the total number of users are active every day. \n\n"
            f"Median engagement is {df1['median_involvement'][0]}. \n\n"
            f"On {yesterday}\n\n"
            f"{df2['feed_mes_p'][1]}% of the total users used the feed and messenger. \n\n"
            f"{df2['only_mes_p'][1]}% of the total users used the messenger only. \n\n"
            f"{df2['actions_peruser'][1]}% actions per user. \n\n"
        )
        bot.sendMessage(chat_id=group_id, text=msg)


    @task
    def load_auditory():
        q = """
        SELECT
        time::date AS date,
        count(DISTINCT user_id) AS unique_users,
        countIf(action='like') AS likes,
        countIf(action='view') AS views,
        ROUND(countIf(action='like')/count(DISTINCT user_id), 2) AS likes_norm,
        ROUND(countIf(action='view')/count(DISTINCT user_id), 2) AS views_norm,
        ROUND(likes/views*100, 2) AS CTR
        FROM simulator_20230420.feed_actions
        WHERE date BETWEEN today()-7 AND today()-1
        GROUP BY date
        """
        auditory = pandahouse.read_clickhouse(query=q, connection=connection)
        return auditory


    @task
    def load_activity():
        q = """
        SELECT
        date,
        ROUND(SUM(session), 1) AS sessions_count,
        ROUND(SUM(session)/count(distinct user_id), 2) AS sessions_count_normalized,
        ROUND(SUM(diff_min) FILTER(WHERE diff_min <20), 1) AS time_in,
        ROUND((SUM(diff_min) FILTER(WHERE diff_min <20))/count(distinct user_id)/60, 2) AS time_in_normalized,
        countIf(action NOT IN('like', 'view')) AS messeges,
        ROUND(countIf(action NOT IN('like', 'view'))/count(distinct user_id), 2) AS messeges_normalized

        FROM(
        SELECT
        user_id, time, date, action,
        IF(lagged = '1970-01-01T03:00:00', 0, (time - lagged)/60) AS diff_min,
        multiIf(diff_min = 0, 1, diff_min > 20, 1, 0) AS session
        FROM(
        SELECT
        user_id, time, time::date AS date, action,
        lagInFrame(time) OVER(PARTITION BY user_id, date, user_id ORDER BY time) AS lagged
        FROM(
        SELECT
        user_id, time, action
        FROM simulator_20230420.feed_actions
        WHERE time::date BETWEEN today()-7 AND today()-1
        UNION ALL
        SELECT
        user_id, time, toString(reciever_id) AS action
        FROM simulator_20230420.message_actions
        WHERE time::date BETWEEN today()-7 AND today()-1)))
        GROUP BY date
        """
        activity = pandahouse.read_clickhouse(query=q, connection=connection)
        return activity


    @task
    def send_plot_metrics(df1, df2):
        day = mdates.DayLocator()
        day_fmt = mdates.DateFormatter("%d-%m")

        fig, axes = plt.subplots(2, 2, figsize=(16, 9))
        sns.lineplot(
            ax=axes[0, 0], data=df1, x="date", y="likes", color="indigo", marker="o"
        ).set(xlabel=None, ylabel=None)
        sns.lineplot(
            ax=axes[0, 0],
            data=df1,
            x="date",
            y="views",
            color="lightseagreen",
            marker="o",
        ).set(xlabel=None, ylabel=None)
        sns.lineplot(
            ax=axes[0, 0],
            data=df2,
            x="date",
            y="messeges",
            color="midnightblue",
            marker="o",
        ).set(xlabel=None, ylabel=None)

        sns.lineplot(
            ax=axes[1, 0],
            data=df1,
            x="date",
            y="likes_norm",
            color="indigo",
            marker="o",
        ).set(xlabel=None, ylabel=None)
        sns.lineplot(
            ax=axes[1, 0],
            data=df1,
            x="date",
            y="views_norm",
            color="lightseagreen",
            marker="o",
        ).set(xlabel=None, ylabel=None)
        sns.lineplot(
            ax=axes[1, 0],
            data=df2,
            x="date",
            y="messeges_normalized",
            color="midnightblue",
            marker="o",
        ).set(xlabel=None, ylabel=None)

        sns.lineplot(
            ax=axes[0, 1],
            data=df1,
            x="date",
            y="unique_users",
            color="lightseagreen",
            marker="o",
        ).set(xlabel=None, ylabel=None)

        sns.lineplot(
            ax=axes[1, 1],
            data=df1,
            x="date",
            y="CTR",
            color="lightseagreen",
            marker="o",
        ).set(xlabel=None, ylabel=None)

        for x in range(2):
            for y in range(2):
                ax = axes[x, y]
                for line in ax.get_lines():
                    xdata = line.get_xdata()
                    ydata = line.get_ydata()
                    for i in range(len(ydata) - 3, len(ydata)):
                        ax.annotate(
                            ydata[i],
                            (xdata[i], ydata[i]),
                            textcoords="offset points",
                            xytext=(0, 10),
                        )
                ax.xaxis.set_major_locator(day)
                ax.xaxis.set_major_formatter(day_fmt)
                ax.tick_params(axis="x")
                ax.set_ylim(bottom=0)

        axes[0, 0].set_title("Likes, views, messeges absolute values", pad=35)
        axes[0, 1].set_title("DAU", pad=35)
        axes[1, 0].set_title("Likes, views, messeges normalized", pad=35)
        axes[1, 1].set_title("CTR, %", pad=35)

        axes[0, 0].legend(
            labels=["likes", "views", "messeges"], bbox_to_anchor=(1.25, 1.02)
        )
        axes[1, 0].legend(
            labels=["likes", "views", "messeges"], bbox_to_anchor=(1.25, 1.02)
        )

        sns.despine()
        plt.subplots_adjust(hspace=0.4, wspace=0.4)

        plot_object = io.BytesIO()
        plt.savefig(plot_object, bbox_inches="tight")
        plot_object.seek(0)
        plot_object.name = "metrics.png"
        plt.close()

        bot.sendPhoto(chat_id=group_id, photo=plot_object)


    @task
    def send_plot_activity(df):
        day = mdates.DayLocator()
        day_fmt = mdates.DateFormatter("%d-%m")

        fig, axes = plt.subplots(1, 2, figsize=(16, 5))

        sns.lineplot(
            ax=axes[0],
            data=df,
            x="date",
            y="sessions_count",
            color="indigo",
            marker="o",
        ).set(xlabel=None, ylabel=None)
        sns.lineplot(
            ax=axes[0],
            data=df,
            x="date",
            y="time_in",
            color="lightseagreen",
            marker="o",
        ).set(xlabel=None, ylabel=None)

        sns.lineplot(
            ax=axes[1],
            data=df,
            x="date",
            y="sessions_count_normalized",
            color="indigo",
            marker="o",
        ).set(xlabel=None, ylabel=None)
        sns.lineplot(
            ax=axes[1],
            data=df,
            x="date",
            y="time_in_normalized",
            color="lightseagreen",
            marker="o",
        ).set(xlabel=None, ylabel=None)

        for x in range(2):
            ax = axes[x]
            ax.xaxis.set_major_locator(day)
            ax.xaxis.set_major_formatter(day_fmt)
            ax.tick_params(axis="x")
            ax.set_ylim(bottom=0)
            for line in ax.get_lines():
                xdata = line.get_xdata()
                ydata = line.get_ydata()

                for i in range(len(ydata) - 3, len(ydata)):
                    ax.annotate(
                        ydata[i],
                        (xdata[i], ydata[i]),
                        textcoords="offset points",
                        xytext=(0, 10),
                    )

        axes[0].legend(labels=["sessions", "time in product"], bbox_to_anchor=(1.2, 1.02))
        axes[1].legend(labels=["sessions", "time in product"], bbox_to_anchor=(1.2, 1.02))

        axes[0].set_title("Time in product, sessions absolute values", pad=35)
        axes[1].set_title("Time in product, sessions normalized", pad=35)

        plt.subplots_adjust(wspace=0.3)
        sns.despine()

        plot_object = io.BytesIO()
        plt.savefig(plot_object, bbox_inches="tight")
        plot_object.seek(0)
        plot_object.name = "activity.png"
        plt.close()

        bot.sendPhoto(chat_id=group_id, photo=plot_object)


    involvement1 = load_involvement1()
    involvement2 = load_involvement2()
    auditory = load_auditory()
    activity = load_activity()
    send_involvement(involvement1, involvement2)
    send_plot_metrics(auditory, activity)
    send_plot_activity(activity)


task72_kruk = task72_kruk()
