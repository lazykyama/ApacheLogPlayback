#!/usr/bin/env python
# -*- coding: utf-8-unix -*-

"""parses and converts the httpd accesslog.

Usage: 
    access_log_parser.py [-i INPUT] [-o OUTPUT] [-d DELIMITER] [--convert_millisec] [--convert_unixtime] [-v]
    access_log_parser.py -h|--help
    access_log_parser.py --version

Options: 
    -i --input INPUT          Specify input file. If omitted, default is stdin.
    -o --output OUTPUT        Specify output file. If omitted, default is stdout.
    -d --delimiter DELIMITER  Specify delimiter for input data [default:  ].
    --convert_millisec        Specify the whether this script converts response time format 
                              from microsec to millisec [default: False].
    --convert_unixtime        Specify the whether 
                              this script converts received time to unixtime [default: False].
    -h --help                 Show this help message.
    --version                 Show this script version.
    -v --verbose              Specify logging level [default: False].
"""

import dateutil.parser

class AccessLogEntity(object):
    def __init__(self, row, indices, convert_millisec=False, convert_unixtime=False):
        if 0 >= min(indices.values()) or len(row) <= max(indices.values()):
            raise RuntimeError('')

        self.__received_time = row[indices['received_time']].replace('[', '')
        if convert_unixtime:
            self.__received_time = int(dateutil.parser.parse(
                self.__received_time, fuzzy=True).timestamp())
        self.__query = row[indices['query']].split(' ')[1]
        self.__response_time = int(row[indices['response_time_microsec']])
        if convert_millisec:
            self.__response_time = (self.__response_time / 1000.0)

    def __str__(self): 
        return '{}\t{}\t{}'.format(self.__received_time, 
            self.__query, self.__response_time)

class AccessLogParser(object):
    def __init__(self, in_stream, indicies, 
        delimiter=' ', convert_millisec=False, convert_unixtime=False):
        import csv
        self.__log_reader = csv.reader(in_stream, delimiter=delimiter)
        self.__indicies = indicies
        self.__convert_millisec = convert_millisec
        self.__convert_unixtime = convert_unixtime

    def __iter__(self):
        return self

    def __next__(self): 
        row = next(self.__log_reader)
        return AccessLogEntity(row, self.__indicies, 
            convert_millisec=self.__convert_millisec, convert_unixtime=self.__convert_unixtime)

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
            '--convert_millisec': bool, 
            '--convert_unixtime': bool, 
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
    parser = AccessLogParser(args['--input'], 
        {'received_time': 3, 'query': 5, 'response_time_microsec': 10}, 
        delimiter=args['--delimiter'], 
        convert_millisec=args['--convert_millisec'], 
        convert_unixtime=args['--convert_unixtime'])
    for single_log in parser:
        args['--output'].write('{}\n'.format(single_log))

