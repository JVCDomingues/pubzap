import json

from flask import jsonify, render_template, request, redirect
from app import app, db
from app.models.tables import Post
from tasks import subscribe_message, publish_pubsub
from google.cloud import pubsub_v1
from config import PROJECT_ID, MY_USERNAME, USERNAME


@app.route('/')
def home():
    return render_template('home.html')

@app.route('/chats', methods=['GET', 'POST'])
def list_chats():
    # if request.method == 'POST':
    #     chat_name = request.form['select']
    #     publisher = pubsub_v1.PublisherClient()
    #     tp = publisher.topic_path(PROJECT_ID, chat_name)
    #     subscriptions = publisher.list_topic_subscriptions(tp)
    #
    #     subscriber = pubsub_v1.SubscriberClient()
    #     subscription_path = subscriber.subscription_path(PROJECT_ID, chat_name)
    #     subscription = subscriber.get_subscription(subscription_path)
    #     if subscriptions == subscription.topic:
    #         subscribe_message.apply_async([chat_name])
    #     return redirect('/chat/{}/{}'.format(chat_name, subscription.name[41:]), 302)
    #
    # publisher = pubsub_v1.PublisherClient()
    # project_path = publisher.project_path(PROJECT_ID)
    #
    # topics = []
    #
    # for t_obj in publisher.list_topics(project_path):
    #     topic_name = t_obj.name.split('/')[-1]
    #     topics.append(topic_name)
    if request.method == 'POST':
        topic = request.form['topic']
        subscription = request.form['subscription']

        subscribe_message.apply_async([subscription])
        return redirect('/chat/{}'.format(topic), 302)
    return render_template('list_chats.html')

@app.route('/chat/<string:topic_name>/', methods=['GET', 'POST'])
def chat(topic_name):

    if request.method == 'GET':
        return render_template('publisher.html', topic_name=topic_name), 200

    message = request.form['message']
    data = {'message': message, 'username': USERNAME}

    publish_pubsub.apply_async([topic_name, json.dumps(data)])

    return render_template('publisher.html', topic_name=topic_name), 200

@app.route('/refresh/<string:chat_name>')
def refresh_chat(chat_name):
    subscribe_message.apply_async([chat_name])
    return redirect('/chat/{}'.format(chat_name), 302)

@app.route('/receive_message/subscription/<string:subscription_name>')
def receive_message_pubsub(subscription_name):

    posts = Post.query.filter_by(topic=subscription_name).all()
    return render_template('subscriber.html', posts=posts, username=MY_USERNAME), 200

@app.route('/api/write/post/<string:topic_name>', methods=['POST'])
def write_post(topic_name):
    data = request.get_json()
    post = Post(data['message'], topic_name, data['username'])
    db.session.add(post)
    db.session.commit()
    return "post saved!", 201

@app.route('/create/', methods=['GET', 'POST'])
def create_topic_and_subscription():
    if request.method == 'GET':
        return render_template('create.html'), 200

    publisher = pubsub_v1.PublisherClient()
    topic_name = request.form['topic']
    tp = publisher.topic_path(PROJECT_ID, topic_name)
    topic_created = publisher.create_topic(tp)

    subscriber = pubsub_v1.SubscriberClient()
    subscription_name = request.form['subscription']
    subscription_path = subscriber.subscription_path(PROJECT_ID, subscription_name)
    subscription_created = subscriber.create_subscription(subscription_path, tp)

    return render_template('create.html', topic_created=topic_created, subscription_created=subscription_created), 201

@app.route('/create_subscription', methods=['GET', 'POST'])
def create_subscription():
    if request.method == 'GET':
        topics = []
        publisher = pubsub_v1.PublisherClient()
        project_path = publisher.project_path(PROJECT_ID)

        for t_obj in publisher.list_topics(project_path):
            topic_name = t_obj.name.split('/')[-1]
            topics.append(topic_name)

        return render_template('create_sub.html', topics=topics), 200

    topic_name = request.form['topic']
    subscription_name = request.form['subscription']

    subscriber = pubsub_v1.SubscriberClient()
    tp = subscriber.topic_path(PROJECT_ID, topic_name)
    subscription_path = subscriber.subscription_path(PROJECT_ID, subscription_name)
    subscriber.create_subscription(subscription_path, tp)

    return render_template('create_sub.html'), 200