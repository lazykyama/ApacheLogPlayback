#!/usr/bin/env python
# -*- coding: utf-8-unix -*-

"""playback the httpd accesslog.

Usage: 
    access_log_playback.py [-i INPUT] [-o OUTPUT] [-d DELIMITER] [-v]
    access_log_playback.py -h|--help
    access_log_playback.py --version

Options: 
    -i --input INPUT          Specify input file. If omitted, default is stdin.
    -o --output OUTPUT        Specify output file. If omitted, default is stdout.
    -d --delimiter DELIMITER  Specify delimiter for input data [default: \t].
    -h --help                 Show this help message.
    --version                 Show this script version.
    -v --verbose              Specify logging level [default: False].
"""


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

    # main procedures.
    import csv
    import time
    reader = csv.reader(args['--input'], delimiter=args['--delimiter'])
    first_row = next(reader)
    logstart_unixtime = int(first_row[0])
    playbackstart_unixtime = int(time.time())
    for row in reader:
        current_unixtime = int(time.time())
        log_unixtime = int(row[0])

        logging.info('send after {}[sec.]'.format(
            (log_unixtime - logstart_unixtime) - (current_unixtime - playbackstart_unixtime)))
