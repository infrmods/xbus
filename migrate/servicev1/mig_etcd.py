import re
import traceback
import etcd3
from collections import namedtuple, defaultdict

Endpoint = namedtuple('Endpoint', ['lease_id', 'addr', 'data'])


class Service(object):
    def __init__(self):
        self.desc = None
        self.endpoints = []


r_old_key = re.compile(r'^/services/(.+)/(.+)/(desc|node_.+)$')


def get_services(client):
    services = defaultdict(lambda: Service())
    for data, meta in client.get_prefix('/services/'):
        matches = r_old_key.findall(meta.key.decode())
        if not matches:
            print('unknown key', meta.key)
            continue
        name, version, suffix = matches[0]
        service_key = '%s:%s' % (name, version)
        service = services[service_key]
        if suffix == 'desc':
            service.desc = data
        else:
            if not meta.lease_id:
                print('missing lease_id for', meta.key)
                continue
            addr = suffix[5:]
            service.endpoints.append(
                Endpoint(lease_id=meta.lease_id, addr=addr, data=data))
    return services


def migrate(**kwargs):
    client = etcd3.client(**kwargs)
    services = get_services(client)
    lease_ids = set()
    drop_keys = []
    for key, service in services.items():
        name, version = key.split(':')
        client.put('/services/%s/default/desc' % key, service.desc)
        drop_keys.append('/services/%s/%s/desc' % (name, version))
        for endpoint in service.endpoints:
            if endpoint.lease_id:
                lease_ids.add(endpoint.lease_id)
            try:
                client.put('/services/%s/default/node_%s' %
                           (key, endpoint.addr),
                           endpoint.data,
                           lease=endpoint.lease_id)
            except Exception:
                print('put node fail:', key, endpoint.addr)
                traceback.print_exc()
            drop_keys.append('/services/%s/%s/node_%s' %
                             (name, version, endpoint.addr))
        print('%s finished' % key)
    for lease_id in lease_ids:
        try:
            client.refresh_lease(lease_id)
        except Exception:
            print('refresh lease fail:', lease_id)
    for key in drop_keys:
        try:
            client.delete(key)
        except Exception:
            print('delete node fail:', key)
            traceback.print_exc()


if __name__ == '__main__':
    import sys

    kwargs = {}
    addr = sys.argv[1] if len(sys.argv) > 1 else None
    if addr:
        if ':' in addr:
            addr, port = addr.split(':')
            kwargs['host'] = addr
            kwargs['port'] = int(port)
        else:
            kwargs['host'] = addr
    migrate(**kwargs)
