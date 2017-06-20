import os
import time
import logging

from kubernetes import client, config
from kubernetes.client import rest


default_namespace = 'test'
iperf_image = 'smpio/iperf:2'
bottleneck_bandwidth = '250M'
server_pod_name = 'iperf-server'
client_pod_name = 'iperf-client'
log = logging.getLogger(__name__)


def main():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    namespace = os.environ.get('NAMESPACE', default_namespace)
    api = Client(namespace)
    measurer = Measurer(api)

    nodenames = api.get_nodenames()
    log.info('Nodes: %s', nodenames)

    # scheduler = Scheduler(nodenames)
    # for _ in range(56):
    #     from_no, to_no = scheduler.get_next_pair()
    #     print('Measuring speed: {} -> {}'.format(from_no, to_no))

    upload_from = nodenames[3]
    download_to = nodenames[4]

    print('Measuring speed: {} -> {}'.format(upload_from, download_to))
    bps = measurer.measure(upload_from, download_to)
    print('{} Mbits'.format(bps / 1000000))


class Scheduler:
    def __init__(self, nodenames):
        self.db = []
        self.nodenames = nodenames

    @property
    def count(self):
        return len(self.nodenames)

    def get_next_pair(self):
        if not self.db:
            ret = 0, 1
        else:
            prev1, prev2 = self.db[-1]
            shift = prev2 - prev1
            if shift < 0:
                shift += self.count
            next1 = prev1 + 1
            if next1 == self.count:
                next1 = 0
                shift += 1
            next2 = (next1 + shift) % self.count
            ret = next1, next2

        self.db.append(ret)
        return self.nodenames[ret[0]], self.nodenames[ret[1]]


class Measurer:
    def __init__(self, api):
        self.api = api

    def measure(self, upload_from, download_to):
        server_node_name = download_to
        client_node_name = upload_from

        self.api.create_pod({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': server_pod_name,
            },
            'spec': {
                'containers': [
                    {
                        'name': 'main',
                        'image': iperf_image,
                        'args': ['--server', '--udp'],
                    },
                ],
                'restartPolicy': 'Never',
                'nodeName': server_node_name,
            },
        })

        server_pod_ip = self.api.get_pod_ip(server_pod_name)
        log.info('Server pod IP: %s', server_pod_ip)

        self.api.create_pod({
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {
                'name': client_pod_name,
            },
            'spec': {
                'containers': [
                    {
                        'name': 'main',
                        'image': iperf_image,
                        'args': ['--client', server_pod_ip,
                                 '--udp',
                                 '--bandwidth', bottleneck_bandwidth,
                                 '--reportstyle', 'C'],
                    },
                ],
                'restartPolicy': 'Never',
                'nodeName': client_node_name,
            },
        })

        self.api.wait_for_pod(client_pod_name)

        iperf_log = self.api.get_pod_log(client_pod_name)
        iperf_log = iperf_log.splitlines()
        iperf_log = iperf_log[-1]
        timestamp, source_address, source_port, destination_address, destination_port, \
            transfer_id, interval, transferred_bytes, bits_per_second, jitter, \
            lost_datagrams, total_datagrams, list_percent, out_of_order_datagrams = iperf_log.split(',')

        self.api.delete_pod(server_pod_name, ignore_non_exists=True)
        self.api.delete_pod(client_pod_name, ignore_non_exists=True)

        return int(bits_per_second)


class Client:
    def __init__(self, namespace):
        self.namespace = namespace
        config.load_kube_config()
        self.v1 = client.CoreV1Api()

    def get_nodenames(self):
        return [i.metadata.name for i in self.v1.list_node().items]

    def create_pod(self, body):
        pod_name = body['metadata']['name']

        while True:
            try:
                log.info('Creating pod %s', pod_name)
                return self.v1.create_namespaced_pod(self.namespace, body)
            except rest.ApiException as e:
                if e.status == 409:
                    log.info('Pod %s already exists', pod_name)
                    self.delete_pod(pod_name)
                    time.sleep(3)
                else:
                    raise e

    def delete_pod(self, name, ignore_non_exists=False):
        log.info('Deleting pod %s', name)
        try:
            return self.v1.delete_namespaced_pod(name, self.namespace, {})
        except rest.ApiException as e:
            if e.status == 404 and ignore_non_exists:
                log.info('Pod %s does not exist', name)
            else:
                raise e

    def get_pod_ip(self, name):
        while True:
            pod = self.v1.read_namespaced_pod(name, self.namespace)
            if pod.status.pod_ip:
                return pod.status.pod_ip
            log.debug('No pod IP for %s', name)
            time.sleep(3)

    def wait_for_pod(self, name):
        log.info('Waiting for pod %s', name)
        while True:
            pod = self.v1.read_namespaced_pod(name, self.namespace)
            if pod.status.phase in ('Succeeded', 'Failed'):
                return pod
            time.sleep(3)

    def get_pod_log(self, name):
        return self.v1.read_namespaced_pod_log(name, self.namespace)


if __name__ == '__main__':
    main()
