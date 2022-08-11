#!/bin/bash
docker pull docker.elastic.co/elasticsearch/elasticsearch:6.8.23
docker run -p 127.0.0.1:9200:9200 -p 127.0.0.1:9300:9300 --name elastic -d --cpus=4 -v /home/ubuntu/data/semmed_raw/elastic_dat:/usr/share/elasticsearch/data:rw -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.8.23
