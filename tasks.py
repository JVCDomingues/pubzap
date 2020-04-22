import requests
from celery import Celery
from config import PROJECT_ID, URL_API
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound

app = Celery('tasks', broker='redis://localhost:6379/0')

@app.task
def subscribe_message(subscription_name):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, subscription_name)

    try:
        def callback(message):
            print(message)
            message.ack()

            data = message.data.decode("utf-8")
            HEADERS = {'Content-Type': 'application/json'}

            # ESCREVE NO BANCO DE DADOS VIA API
            requests.post('{}/api/write/post/{}'.format(URL_API, subscription_name), headers=HEADERS, data=data)
    except Exception as e:
        print('DEU EXCEPTION', e)

    subscriber.subscribe(subscription_path, callback=callback)

@app.task
def publish_pubsub(topic_name, message):
    publisher = pubsub_v1.PublisherClient()

    try:
        topic_path = publisher.topic_path(PROJECT_ID, topic_name)
        publisher.get_topic(topic_path)
        future = publisher.publish(topic_path, message.encode())
    except NotFound:
        message_error = '[ {} ] topic does not exist'.format(topic_name)
        return message_error, 404

    return future.result(), 201