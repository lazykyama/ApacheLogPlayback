#!/usr/bin/env python
# -*- coding: utf-8-unix -*-

"""playback the httpd accesslog.

Usage: 
    access_log_playback.py [-i INPUT] [-o OUTPUT] [-d DELIMITER] [--playback_speed SPEED]
        [--scheme SCHEME] [--host HOST] [--port PORT] [-v]
    access_log_playback.py -h|--help
    access_log_playback.py --version

Options: 
    -i --input INPUT          Specify input file. If omitted, default is stdin.
    -o --output OUTPUT        Specify output file. If omitted, default is stdout.
    -d --delimiter DELIMITER  Specify delimiter for input data [default: \t].
    --playback_speed SPEED    Specify speed of playbacking [default: 1.0].
    --scheme SCHEME           Specify url scheme to access [default: http].
    --host HOST               Specify hostname to access [default: localhost].
    --port PORT               Specify port number to access [default: 80].
    -h --help                 Show this help message.
    --version                 Show this script version.
    -v --verbose              Specify logging level [default: False].
"""

import time

import concurrent.futures
import threading
import queue

# 3rd-party's library.
import requests

REQUEST_WORKER_DEFAULT_SIZE = 30
TASKQUEUE_LIMIT = 100

class ResultWriter(threading.Thread): 
    def __init__(self, result_queue): 
        super(ResultWriter, self).__init__()
        self.__result_queue = result_queue

    def run(self): 
        running = True
        while running: 
            result = self.__result_queue.get()
            if result is not None:
                logging.debug('result: {}'.format(result.result()))
            else: 
                logging.info('result writer goes to finish...')
                running = False
            self.__result_queue.task_done()

    def put(self, item): 
        self.__result_queue.put(item)

    def shutdown(self, wait=True): 
        if wait: 
            self.__result_queue.join()
        self.__result_queue.put_nowait(None)

def send_request(url, sending_time):
    current_unixtime = int(time.time())
    waittime = (sending_time - current_unixtime)
    logging.debug('{} -> {}'.format(url, waittime))
    if waittime > 0:
        time.sleep(waittime)
    logging.debug('exec: {}'.format(url))

    res = requests.get(url)
    if 'Content-Length' in res.headers: 
        content_length = res.headers['Content-Length']
    else: 
        content_length = len(res.text)
    return '{}\t{}\t{}\t{}'.format(
        url, res.status_code, res.reason, 
        res.elapsed.total_seconds(), content_length)

class TaskDispatchThread(threading.Thread):
    def __init__(self, 
        task_queue=None, request_worker_size=REQUEST_WORKER_DEFAULT_SIZE):
        super(TaskDispatchThread, self).__init__()
        self.__task_queue = task_queue
        self.__result_writer = ResultWriter(queue.Queue())
        self.__executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=request_worker_size)

    def run(self):
        running = True
        self.__result_writer.start()

        while running: 
            task = self.__task_queue.get()
            if task is not None:
                future = self.__executor.submit(send_request, 
                    task.url, task.sending_time)
                self.__result_writer.put(future)
            else:
                logging.info('task dispatcher goes to finish...')
                self.__result_writer.shutdown()
                running = False
            self.__task_queue.task_done()

    def shutdown(self, wait=True): 
        if wait: 
            self.__task_queue.join()
            self.__executor.shutdown()
        self.__task_queue.put_nowait(None)

class PlaybackTask(object):
    def __init__(self, url, sending_time, responsetime_microsec):
        self.url = url
        self.sending_time = sending_time
        self.responsetime_microsec = responsetime_microsec

    def __eq__(self, other): 
        return (self.sending_time == other.sending_time 
            and self.url == other.url)
    def __ne__(self, other): 
        return (self.sending_time != other.sending_time 
            or self.url != other.url)

    def __lt__(self, other): 
        if self.sending_time < other.sending_time:
            return True
        if self.sending_time > other.sending_time:
            return False
        
        if self.url < other.url: 
            return True
        return False

    def __gt__(self, other):
        if self.sending_time > other.sending_time:
            return True
        if self.sending_time < other.sending_time:
            return False
        
        if self.url > other.url: 
            return True
        return False

    def __le__(self, other): 
        return self.__eq__(other) or self.__lt__(other)
    def __ge__(self, other): 
        return self.__eq__(other) or self.__gt__(other)

def __calculate_sendtime(playbackstart, logstart, logtime, playback_speed=1.0):
    return ((logtime - logstart) * (1.0 / playback_speed)) + playbackstart

def __make_url(path, args): 
    return args['--scheme'] + '://' + args['--host'] + ':' + str(args['--port']) + path

def __put_task(task_queue, args, row, playbackstart_unixtime, logstart_unixtime):
    log_unixtime = int(row[0])
    sending_unixtime = __calculate_sendtime(
        playbackstart_unixtime, logstart_unixtime, log_unixtime, args['--playback_speed'])
    logging.info('send at {}'.format(sending_unixtime))
    task_queue.put(PlaybackTask(
        __make_url(row[1], args), sending_unixtime, int(row[2])))

if __name__=='__main__':
    import docopt
    import schema

    import sys
    import os

    import logging

    args = docopt.docopt(__doc__, version='0.0.1')
    try: 
        s = schema.Schema({
            '--input': schema.Or(None, os.path.exists), 
            '--output': schema.Or(None, str), 
            '--delimiter': schema.And(schema.Use(str), lambda d: len(d) == 1, 
                error='deny multiple characters as delimiter: {}.'.format(args['--delimiter'])), 
            '--playback_speed': schema.And(schema.Use(float), lambda s: 0.0 < s, 
                error='deny negative speed value: {}.'.format(args['--playback_speed'])), 
            '--scheme': str, 
            '--host': str, 
            '--port': schema.And(schema.Use(int), lambda p: 0 <= p <= 65535, 
                error='unexpected port number {}.'.format(args['--port'])), 
            '--help': bool, 
            '--version': bool, 
            '--verbose': bool
            })
        args = s.validate(args)
    except schema.SchemaError as e: 
        sys.stderr.write(e)
        sys.exit(1)

    if args['--verbose']: 
        # debug.
        logging.basicConfig(level=logging.DEBUG, 
            format='%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s')
    else:
        # release.
        logging.basicConfig(level=logging.INFO, 
            format='%(asctime)s [%(levelname)s] %(message)s')

    if args['--input'] is None: 
        args['--input'] = sys.stdin
    if args['--output'] is None: 
        args['--output'] = sys.stdout

    logging.debug('{}'.format(args))

    # preapre worker thread.
    task_queue = queue.PriorityQueue(maxsize=TASKQUEUE_LIMIT)
    task_dispatcher = TaskDispatchThread(task_queue)
    task_dispatcher.start()

    # main procedures.
    import csv
    reader = csv.reader(args['--input'], delimiter=args['--delimiter'])
    first_row = next(reader)
    logstart_unixtime = int(first_row[0])
    playbackstart_unixtime = int(time.time())
    __put_task(task_queue, args, first_row, playbackstart_unixtime, logstart_unixtime)

    for row in reader:
        __put_task(task_queue, args, row, playbackstart_unixtime, logstart_unixtime)

    logging.info('complete to put tasks.')
    task_dispatcher.shutdown()

    logging.info('finish.')
