# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, unicode_literals

import json
import logging
import mpd
import pika
import socket
import sys
import themyutils.json
import time
import urllib

from themylog.client import setup_logging_handler

setup_logging_handler("move_player_to_mpd")


def superseed():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("192.168.0.7", 20139))
    s.sendall("become_superseeded\r\n")
    r = ""
    while True:
        data = s.recv(1024)
        if not data:
            break
        r += data
    r = json.loads(r)
    s.close()

    if r:        
        client = mpd.MPDClient()
        client.connect("192.168.0.4", 6600)

        client.clear()
        for track in r["playlist"]:
            client.add(urllib.unquote(str(track)))

        client.seek(r["current"]["index"], int(r["current"]["position"]))

if __name__ == "__main__":
    if len(sys.argv) > 1:
        superseed()
        sys.exit(0)

    while True:
        try:
            mq_connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
            mq_channel = mq_connection.channel()

            mq_channel.exchange_declare(exchange="themylog", type="topic")

            result = mq_channel.queue_declare(exclusive=True)
            queue_name = result.method.queue

            mq_channel.queue_bind(exchange="themylog", queue=queue_name,
                                  routing_key="smarthome.owner_watcher.themylogin_at_home_changed")

            def callback(ch, method, properties, body):
                if themyutils.json.loads(body)["args"]["value"]:
                    for i in range(60):
                        try:
                            superseed()
                        except socket.error:
                            logging.debug("socket.error, this may be temporary", exc_info=True)
                            time.sleep(1)
                            pass

            mq_channel.basic_consume(callback, queue=queue_name, no_ack=True)
            mq_channel.start_consuming()
        except:
            logging.exception("An exception occured")
            time.sleep(1)
