#!/bin/bash
# A very crude example of creating a core file for Solr
# Edit for the solrconfig.xml and schema.xml files

if [ $# -ne 1 ];then
	echo "Create solr core"
	echo ""
	echo "Useage: $0 <keyspace>"
	echo ""
	exit 1
fi
keyspace=$1
curl http://localhost:8983/solr/resource/$keyspace.user_newsfeed/solrconfig.xml --data-binary @solrconfig-t17362.xml -H 'Content-type:text/xml; charset=utf-8'
curl http://localhost:8983/solr/resource/$keyspace.user_newsfeed/schema.xml --data-binary @schema-t17362.xml -H 'Content-type:text/xml; charset=utf-8'
curl "http://localhost:8983/solr/admin/cores?action=CREATE&name=$keyspace.user_newsfeed"
