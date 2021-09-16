A simple project that get information from blockchain and proceed it to HDFS via Kafka and Flink

Pipeline:
GraphQL -> Kafka -> Flink (Table API) -> ~~HDFS~~FileSystem -> HiveQL

# How to Install:
### Install Python library
pip3 install pyyaml
pip3 install kafka-python
pip3 install apache-flink
sudo apt install python-is-python3
### Create topic in kafka …/kafka/bin
kafka-topics.bat --create --zookeeper ~~localhost:2181~~ --replication-factor 1 --partitions 1 --topic blocks


## Blockchain info
Bitcoin

## Kafka

## Spark structured streaming

## ML spark
