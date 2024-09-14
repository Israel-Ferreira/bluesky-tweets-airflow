from airflow.models import DAG

from airflow.models.variable import Variable

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from airflow.providers.mongo.hooks.mongo import MongoHook


import pendulum

import requests

import json

from urllib.parse import quote


start_date = pendulum.today(tz="America/Sao_Paulo").now()


def extract_tweet_data_and_save_in_mongo(**kwargs):
    ti =  kwargs["ti"]
    task_data = json.loads(ti.xcom_pull(task_ids='get_bsky_tweets'))["posts"][0]

    tweet_data = {
        "author": task_data["author"]["handle"],
        "author_did": task_data["author"]["did"],
        "text": task_data["record"]["text"],
        "createdAt": task_data["record"]["createdAt"],
        "lang": task_data["record"]["langs"]
    }


    mongo_hook = MongoHook(mongo_conn_id="mongo_conn")
    client = mongo_hook.get_conn()
    db =  client.tweets

    collection = db.get_collection("tweets_collection")

    collection.insert_one(tweet_data)


    print(tweet_data)






def authenticate_on_bsky(bsky_handle, bsky_pswd):

    URL_BSKY_AUTH = "https://bsky.social/xrpc/com.atproto.server.createSession"

    print(bsky_handle)

    payload_auth = {
        "identifier": bsky_handle,
        "password": bsky_pswd
    }

    response_auth = requests.post(url=URL_BSKY_AUTH, json=payload_auth)


    print(str(response_auth.content))
    print(response_auth.headers)

    if response_auth.status_code >= 400:
        raise Exception("Erro ao fazer o login na API do Blue Sky")
    

    return response_auth.json()




with DAG(dag_id="bluesky_dag", schedule="*/10 * * * *", start_date=start_date) as dag:
    
    start_tsk = EmptyOperator(
        task_id="start_tsk"
    )


    auth_bsky_task = PythonOperator(
        task_id="auth_bsky_task",
        python_callable=authenticate_on_bsky,
        op_kwargs = {
            "bsky_handle": Variable.get("BLUESKY_HANDLE"),
            "bsky_pswd": Variable.get("BLUESKY_PASSWORD")
        }
    )



    get_bsky_tweets = SimpleHttpOperator(
        task_id="get_bsky_tweets",
        http_conn_id="bsky-api",
        method="GET",
        headers={"Content-Type": "application/json", "Authorization": "Bearer {{ task_instance.xcom_pull(task_ids='auth_bsky_task')['accessJwt'] }}"},
        endpoint=f'/app.bsky.feed.searchPosts?q={quote('#TesteAirflow')}&limit=2&sort=latest'
    )



    extract_tweet_data_task =  PythonOperator(
        task_id="extract_tweet_data_tsk",
        python_callable=extract_tweet_data_and_save_in_mongo
    )


    start_tsk >> auth_bsky_task >> get_bsky_tweets >> extract_tweet_data_task
