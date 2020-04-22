import time
import datetime
from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound
from flask import Flask, request, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy
from flask_script import Manager
from flask_migrate import Migrate, MigrateCommand

PROJECT_ID = 'bee-bit-tech-2020'





def publish_pubsub(topic_name, message):
    publisher = pubsub_v1.PublisherClient()

    try:
        topic_path = publisher.topic_path(PROJECT_ID, topic_name)
        publisher.get_topic(topic_path)
        future = publisher.publish(topic_path, message)
    except NotFound:
        error_message = '[ {} ] topic does not exist'.format(topic_name)
        return error_message, 404

    return future.result(), 201

@app.route('/send_message/topic/<string:topic_name>', methods=['GET', 'POST'])
def send_message_pubsub(topic_name):
    if request.method == 'GET':
        return render_template('publisher.html'), 200

    message = request.form['message']
    content, status_code = publish_pubsub(topic_name, message.encode())
    response = {'content': content, 'status_code': status_code}

    return render_template('publisher.html', response=response), 200

@app.route('/receive_messages/subscription/<string:subscription_name>')
def receive_message_pubsub(subscription_name):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, subscription_name)

    def callback(message):
        message.ack()
        text_message = message.data.decode('utf-8')

        post = Post(text_message, subscription_name, 'joao')
        db.session.add(post)
        db.session.commit()

    subscriber.subscribe(subscription_path, callback=callback)
    time.sleep(5)

    posts = reversed(Post.query.filter_by(topic=subscription_name).all())
    return render_template('subscriber.html', posts=posts), 200

@app.route('/create-topic/<string:name>')
def create_topic_and_subscription(name):
    publisher = pubsub_v1.PublisherClient()
    tp = publisher.topic_path(PROJECT_ID, name)
    publisher.create_topic(tp)

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, name)
    subscriber.create_subscription(subscription_path)

    return 'Sucesso  => {}'.format(name), 200

# --------------------------------------------------------------------
@app.route('/api/')
def health_check():
    return '<h1>Funcionando!!</h1>', 200

@app.route('/api/post/topic/<string:topic_name>', methods=['POST'])
def post_pubsub(topic_name):
    message = request.data
    content, status_code = publish_pubsub(topic_name, message)
    response = {'content': content, 'status_code': status_code, 'topic': topic_name}

    if status_code == 201:
        return jsonify(response), 201
    return jsonify(response), status_code

if __name__ == '__main__':
    db.create_all()
    manager.run()