import datetime
from app import db

class Post(db.Model):
    _tablename_ = 'posts'

    id = db.Column(db.Integer, primary_key=True)
    message = db.Column(db.Text)
    topic = db.Column(db.String(20))
    username = db.Column(db.String(20))
    created_at = db.Column(db.String(20), default=datetime.datetime.now().strftime('%d-%m-%Y %H:%M'))

    def __init__(self, message, topic, username):
        self.message = message
        self.topic = topic
        self.username = username

    def __repr__(self):
        return '<Post %r>' % self.id