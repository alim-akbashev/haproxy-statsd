#!/usr/bin/env python
"""
usage: report_haproxy.py [-h] [-c CONFIG] [-1] [--excludeproxies]

Report haproxy stats to statsd

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Config file location
  -1, --once        Run once and exit
  --excludeproxies  Exclude proxies, ie only show BACKEND & FRONTEND stats

Config file format:
---
haproxy_url: "http://127.0.0.1:1936/;csv"
haproxy_user:
haproxy_password:
statsd_host: "127.0.0.1"
statsd_port: "8125"
statsd_namespace: "haproxy.(HOSTNAME)"
interval = "5"
"""

import time
import csv
import socket
import argparse
from yaml import load
import requests
from requests.auth import HTTPBasicAuth


def get_haproxy_report(url, user=None, password=None):
    if isinstance(url, str):
        url = [url]
    auth = None
    if user:
        auth = HTTPBasicAuth(user, password)
    aggregated = []
    for u in url:
        r = requests.get(u, auth=auth)
        r.raise_for_status()
        lines = r.content.splitlines()
        header = lines.pop(0).lstrip('# ')
        aggregated += lines
    aggregated.insert(0, header)
    return csv.DictReader(aggregated)


def report_to_statsd(stat_rows,
                     host='127.0.0.1',
                     port=8125,
                     namespace='haproxy',
                     excludeproxies=False):
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    stat_count = 0

    # Report for each row
    for row in stat_rows:
        if not row['svname'] in [ 'BACKEND', 'FRONTEND' ] and excludeproxies:
            continue
        path = '.'.join([namespace, row['pxname'], row['svname']])

        # Report each stat that we want in each row
        for stat in ['scur', 'smax', 'ereq', 'econ', 'rate', 'bin', 'bout', 'hrsp_1xx', 'hrsp_2xx', 'hrsp_3xx', 'hrsp_4xx', 'hrsp_5xx', 'qtime', 'ctime', 'rtime', 'ttime']:
            val = row.get(stat) or 0
            udp_sock.sendto(
                '%s.%s:%s|g' % (path, stat, val), (host, port))
            stat_count += 1
    return stat_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Report haproxy stats to statsd')
    parser.add_argument('-c', '--config',
                        help='Config file location',
                        default='./haproxy-statsd.yaml')
    parser.add_argument('-1', '--once',
                        action='store_true',
                        help='Run once and exit',
                        default=False)
    parser.add_argument('--excludeproxies',
                        action='store_true',
                        help='Exclude proxies',
                        default=False)

    args = parser.parse_args()
    default_config = {
        'haproxy_url': 'http://127.0.0.1:1936/;csv',
        'haproxy_user': '',
        'haproxy_password': '',
        'statsd_namespace': 'haproxy.(HOSTNAME)',
        'statsd_host': '127.0.0.1',
        'statsd_port': '8125',
        'interval': '5',
    }
    config_from_file = load(file(args.config, 'r'))
    config = default_config.copy()
    config.update(config_from_file)

    # Generate statsd namespace
    namespace = config['statsd_namespace']
    if '(HOSTNAME)' in namespace:
        namespace = namespace.replace('(HOSTNAME)', socket.gethostname())

    interval = int(config['interval'])

    try:
        while True:
            report_data = get_haproxy_report(
                config['haproxy_url'],
                user=config['haproxy_user'],
                password=config['haproxy_password'])

            report_num = report_to_statsd(
                report_data,
                namespace=namespace,
                host=config['statsd_host'],
                port=int(config['statsd_port']),
                excludeproxies=args.excludeproxies)

            print("Reported %s stats" % report_num)
            if args.once:
                exit(0)
            else:
                time.sleep(interval)
    except KeyboardInterrupt:
        exit(0)
