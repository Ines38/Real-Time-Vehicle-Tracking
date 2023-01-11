<h1 align="center">
  <br>
  Real Time Vehicle Tracking
</h1>
<div align="center">
  <h4>
    <a href="#Context">Context</a> | <a href="#Technologies">Technologies</a> | <a href="#Installation guide">Installation guide</a> 
  </h4>
</div>
<br>

## Contexte
Our project focuses on tracking bus movement in real time to reduce delay problems and eliminate the bus parking problem in Raleigh USA.

This project sets up a complete pipeline for data collection, streaming, ML model training and prediction, indexing and visualization.
<img src="/img/pipeline.png">
<img src="/img/dashboard.png">

## Technologies
The software used
- Spark 3.3.1
- Kafka 3.2.0
- Elasticsearch 7.14.1
- Kibana 7.14.1

## Installation guide
- Start zookeeper
  - cd kafka-directory
    ./bin/zookeeper-server-start.sh ./config/zookeeper.properties

- Start kafka
  - cd kafka-directory
    ./bin/kafka-server-start.sh ./config/server.properties

- Create topic
  - ./bin/kafka-topics.sh --create --topic openapi-vehicule --partitions 10 --bootstrap-server localhost:9092

- Start elasticsearch
  - cd elasticsearch-directory: 
    ./bin/elasticsearch

- Start kibana
  - cd kibana-directory: 
    ./bin/kibana

- Install packages
  - kafka
  - elasticsearch
  - pyspark

- Launch producer
  - python3 producer.py

- Launch consumer
  - ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.14.1 consumer.py

