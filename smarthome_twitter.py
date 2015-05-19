# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, unicode_literals

from collections import defaultdict
from datetime import datetime, timedelta
import logging
import pika
from pprint import PrettyPrinter
import pytils
import re
import requests
from sqlalchemy import event, exc, create_engine
from sqlalchemy.orm import create_session
from sqlalchemy.pool import Pool
from sqlalchemy.sql import func
import sys
import time
import twitter

from themylog.client import setup_logging_handler
from themylog.handler.sql import SQLRecord
import themyutils.json
import twitter_overkill
from twitter_overkill.utils import join_list

setup_logging_handler("smarthome_twitter")
logging.getLogger("pika").setLevel(logging.INFO)

consumer_key = ""
consumer_secret = ""
access_token_key = ""
access_token_secret = "" 

last_fm_username = "themylogin"

api = twitter.Api(consumer_key=consumer_key,
                  consumer_secret=consumer_secret,
                  access_token_key=access_token_key,
                  access_token_secret=access_token_secret)

from last_fm.db import db as last_fm_db
from last_fm.models import User as LastFMUser, Scrobble
last_fm_user = last_fm_db.session.query(LastFMUser).\
                                  filter(LastFMUser.username == last_fm_username).\
                                  one()

@event.listens_for(Pool, "checkout")
def ping_connection(dbapi_connection, connection_record, connection_proxy):
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SELECT 1")
    except:
        # optional - dispose the whole pool
        # instead of invalidating one at a time
        # connection_proxy._pool.dispose()

        # raise DisconnectionError - pool will try
        # connecting again up to three times before raising.
        raise exc.DisconnectionError()
    cursor.close()


class MyPrettyPrinter(PrettyPrinter):
    def format(self, *args, **kwargs):
        repr, readable, recursive = PrettyPrinter.format(self, *args, **kwargs)
        if repr:
            if repr[0] in ('"', "'"):
                repr = repr.decode('string_escape')
            elif repr[0:2] in ("u'", 'u"'):
                repr = repr.decode('unicode_escape').encode(sys.stdout.encoding)
        return repr, readable, recursive


def pprint(obj, stream=None, indent=1, width=80, depth=None):
    printer = MyPrettyPrinter(stream=stream, indent=indent, width=width, depth=depth)
    printer.pprint(obj)


subscriptions = []
def tweet_event(event):
    def decorator(generate_tweet):
        subscriptions.append((event, generate_tweet))
        return generate_tweet

    return decorator


def post_tweet_callback(generate_tweet):                    
    def post_tweet(ch, method, properties, body):
        tweet = generate_tweet(**themyutils.json.loads(body)["args"])
        if tweet:
            twitter_overkill.tweet(api, tweet)

    return post_tweet


def timedelta_in_words(td, accuracy=1):
    return pytils.dt.distance_of_time_in_words(datetime.now(), accuracy, datetime.now() + td).replace(" назад", "")


def update_scrobbles():    
    try:
        return requests.get("http://127.0.0.1:46400/update_scrobbles/%s" % last_fm_username).json()
    except Exception as e:
        logging.exception("Unable to update scrobbles")


@tweet_event("smarthome.sleep_tracker.woke_up")
def wake_up(start, end, **kwargs):
    last_fm_db_session = last_fm_db.create_scoped_session()

    sentences = []

    sentences.append("Спал %s" % timedelta_in_words(end - start, 3))

    now_playing = update_scrobbles()

    first_track = last_fm_db_session.query(Scrobble).\
                                     filter(Scrobble.user == last_fm_user,
                                            Scrobble.uts >= time.mktime(end.timetuple())).\
                                     order_by(Scrobble.uts).\
                                     first()
    if first_track is None and now_playing is not None:
        first_track = type(b"Fake", (object,), now_playing)()

    if first_track:
        success = None
        def key(scrobble):
            s1 = int(time.mktime(end.timetuple())) % 86400
            s2 = scrobble.uts % 86400
            return min([
                abs(s1 - s2),
                86400 - abs(s1 - s2),
            ])
        for scrobble in sorted(last_fm_db_session.query(Scrobble).filter(Scrobble.user == last_fm_user,
                                                                         Scrobble.artist == first_track.artist,
                                                                         Scrobble.track == first_track.track,
                                                                         Scrobble.uts <= time.mktime((end - timedelta(days=180)).timetuple())),
                               key=key):
            if key(scrobble) < 3 * 3600:
                prev_scrobble = last_fm_db_session.query(Scrobble).filter(Scrobble.user == last_fm_user,
                                                                          Scrobble.uts < scrobble.uts).order_by(Scrobble.uts.desc()).first()
                if prev_scrobble:
                    if scrobble.uts - prev_scrobble.uts > 4 * 3600:
                        success = scrobble
                        break

        if success:
            sentences.append("Начал день под %(artist)s – %(track)s, прямо как %(date)s" % {
                "artist"    : first_track.artist,
                "track"     : first_track.track,
                "date"      : pytils.dt.ru_strftime(u"%d %B %Y", inflected=True, date=datetime.fromtimestamp(success.uts)),
            })
        else:
            sentences.append("Начал день под %(artist)s – %(track)s" % {
                "artist"    : first_track.artist,
                "track"     : first_track.track,
            })

    return ". ".join(sentences)


@tweet_event("smarthome.sleep_tracker.fall_asleep")
def fall_asleep(start, end, **kwargs):
    db = create_session(create_engine("mysql://root@localhost/themylog?charset=utf8"))

    activities = []

    #
    def find_work(title):
        if " - Qt Creator" in title:
            return "чем-то на Qt"

        m = re.search("\] - \~/(Dev|Server)(/apps/|/libs/|/www/|/visio/|/)([0-9A-Za-z\-_.]+)/", title)
        if m:
            return m.group(3)
        else:
            return None

    works = defaultdict(lambda: [])
    odometer_logs = db.query(SQLRecord).filter(SQLRecord.application == "usage_stats",
                                               SQLRecord.datetime >= start,
                                               SQLRecord.datetime <= end).\
                                        order_by(SQLRecord.datetime.asc()).\
                                        all()
    for log in odometer_logs:
        if log.args["keys"] > 0 or log.args["pixels"] > 0:
            for work in set(filter(None, [find_work(title) for title in log.args["titles"]])):
                if (works[work] and log.datetime - works[work][-1][1] <= timedelta(minutes=15)):
                    works[work][-1][1] = log.datetime
                else:
                    works[work].append([log.datetime, log.datetime])

    for work in works:
        length = sum([w[1] - w[0] for w in works[work]], timedelta(seconds=0))
        if length >= timedelta(minutes=15):
            activities.append(("работал над %s" % work, length))

    #

    activities = sorted(activities, key=lambda (title, length): -length)
    activities = map(lambda (title, length): " ".join([title, timedelta_in_words(length, 2)]), activities)

    #

    music = None
    update_scrobbles()
    last_fm_db_session = last_fm_db.create_scoped_session()
    today_scrobbles_count = last_fm_db_session.query(func.count(Scrobble)).\
                                               filter(Scrobble.user == last_fm_user,
                                                      Scrobble.uts >= time.mktime(start.timetuple()),
                                                      Scrobble.uts <= time.mktime(end.timetuple())).\
                                               scalar()
    if today_scrobbles_count > 50:
        artist, scrobbles = last_fm_db_session.query(Scrobble.artist, func.count(Scrobble)).\
                                               filter(Scrobble.user == last_fm_user,
                                                      Scrobble.uts >= time.mktime(start.timetuple()),
                                                      Scrobble.uts <= time.mktime(end.timetuple())).\
                                               group_by(Scrobble.artist).\
                                               order_by(func.count(Scrobble).desc()).\
                                               first()
        if scrobbles > today_scrobbles_count * 0.5:
            music = artist

    if music:
        activities = ["весь день слушал %s" % music] + activities

    #

    variants = []
    if activities:
        variants.append("Лёг спать. Сегодня %s" % (join_list(activities),))
        for l in range(len(activities)):
            if l:
                variants.append("Лёг спать. Сегодня %s" % (join_list(activities[:-l]),))
    variants.append("Лёг спать")

    return variants


if __name__ == "__main__":
    if len(sys.argv) == 1:
        while True:
            try:
                mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
                mq_channel = mq_connection.channel()

                mq_channel.exchange_declare(exchange="themylog", type="topic")

                for event, generate_tweet in subscriptions:
                    result = mq_channel.queue_declare(exclusive=True)
                    queue_name = result.method.queue

                    mq_channel.queue_bind(exchange="themylog", queue=queue_name, routing_key=event)
                    mq_channel.basic_consume(post_tweet_callback(generate_tweet), queue=queue_name, no_ack=True)

                mq_channel.start_consuming()
            except Exception as e:
                logging.exception("AMQP Error")
                time.sleep(1)
    elif len(sys.argv) == 3:
        pprint(globals()[sys.argv[1]](**themyutils.json.loads(sys.argv[2])))
