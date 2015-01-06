from cassandra.cluster import Cluster
import logging

log = logging.getLogger()
log.setLevel('INFO')

class SimpleClient:
	session = None

def connect(self, nodes):
    cluster = Cluster(nodes)
    metadata = cluster.metadata
    self.session = cluster.connect()
    log.info('Connected to cluster: ' + metadata.cluster_name)
    for host in metadata.all_hosts():
        log.info('Datacenter: %s; Host: %s; Rack: %s',
            host.datacenter, host.address, host.rack)

def close(self):
    self.session.cluster.shutdown()
    log.info('Connection closed.')

def main():
    client = SimpleClient()
    client.connect(['192.168.56.23'])
    client.close()
