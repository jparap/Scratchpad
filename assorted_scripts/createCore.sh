#!/bin/bash
# A very crude example of creating a core file for Solr

keyspace=$1
curl http://localhost:8983/solr/resource/$keyspace.user_newsfeed/solrconfig.xml --data-binary @solrconfig-t17362.xml -H 'Content-type:text/xml; charset=utf-8'
curl http://localhost:8983/solr/resource/$keyspace.user_newsfeed/schema.xml --data-binary @schema-t17362.xml -H 'Content-type:text/xml; charset=utf-8'
curl "http://localhost:8983/solr/admin/cores?action=CREATE&name=$keyspace.user_newsfeed"
