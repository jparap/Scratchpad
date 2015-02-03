#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This is just a simple example to test
# the python driver connecting to a
# cluster

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from ssl import PROTOCOL_TLSv1

import logging

log = logging.getLogger()
log.setLevel('INFO')

# comment in / out the required client.connects below
# to get the right connection test for your cluster

class SimpleClient(object):
	session = None

        def connect(self, nodes):
                cluster = Cluster(nodes)
                metadata = cluster.metadata
                self.session = cluster.connect()
                log.info('Connected to cluster: ' + metadata.cluster_name)
                for host in metadata.all_hosts():
                        log.info('Datacenter: %s; Host: %s; Rack: %s', host.datacenter, host.address, host.rack)

	def connectPwd(self, nodes):
                cluster = Cluster(nodes)
		myauth = PlainTextAuthProvider(username='cassandra', password='cassandra')
		cluster.auth_provider = myauth
		metadata = cluster.metadata
		self.session = cluster.connect()
		log.info('Connected to cluster: ' + metadata.cluster_name)
		for host in metadata.all_hosts():
			log.info('Datacenter: %s; Host: %s; Rack: %s', host.datacenter, host.address, host.rack)

	def connectSsl(self, nodes):
                cluster = Cluster(nodes)
		ssl_opts = {'ca_certs': './python.cer', 'ssl_version': PROTOCOL_TLSv1}
		cluster = Cluster(nodes, ssl_options=ssl_opts)
		metadata = cluster.metadata
		self.session = cluster.connect()
		log.info('Connected to cluster: ' + metadata.cluster_name)
		for host in metadata.all_hosts():
			log.info('Datacenter: %s; Host: %s; Rack: %s', host.datacenter, host.address, host.rack)

	def close(self):
		self.session.cluster.shutdown()
		log.info('Connection closed.')
	
def main():
    logging.basicConfig()
    client = SimpleClient()
    #client.connect(['192.168.56.25'])
    client.connectPwd(['192.168.56.25'])
    #client.connectSsl(['192.168.56.25'])
    client.close()

if __name__ == "__main__":
    main()
