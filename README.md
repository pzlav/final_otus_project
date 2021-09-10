A simple project that get information from blockchain and proceed it to MLSaprk job

Pipeline:
GraphQL -> Kafka -> Spark structured streaming -> MLSpark job -> HDFS

# How to Install:
    ### Install python library
           + pip3 install pyyaml
           + pip3 install kafka-python
    ### Create topic in kafka …/kafka/bin
           + kafka-topics.bat --create --zookeeper ~~localhost:2181~~ --replication-factor 1 --partitions 1 --topic blocks


## Blockchain info
Bitcoin

## Kafka

## Spark structured streaming

## ML spark
