#!/usr/bin/env python3
import re
import etcd3
import pymysql
import yaml


def recovery(db, etcd, prefix):
    cursor = db.cursor()
    cursor.execute('select name, value from configs where status=0')
    configs = list(cursor.fetchall())
    for name, value in configs:
        print('%s/%s' % (prefix, name))
        etcd.put('%s/%s' % (prefix, name), value)
    print('finished %d' % len(configs))


r_db = re.compile(r'^(\w+):(\w+)@tcp\(([^:)]+)(:\d+)?\)/(\w+)\?.*$')
r_endpoint = re.compile(r'^https?://([^:]+):(\d+)$')

if __name__ == '__main__':
    with open('config.yaml') as f:
        config = yaml.load(f.read(), Loader=yaml.FullLoader)
    user, pwd, host, port, db = r_db.findall(config['db']['source'])[0]
    port = port and int(port[1:]) or None
    db = pymysql.connect(host=host, port=port, user=user, password=pwd, db=db)
    host, port = r_endpoint.findall(config['etcd']['endpoints'][0])[0]
    etcd = etcd3.client(host=host,
                        port=int(port),
                        ca_cert=config['etcd'].get('cacert', None))
    recovery(db, etcd, config.get('configs', {}).get('key_prefix', '/configs'))
