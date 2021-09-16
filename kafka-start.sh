#path ot Kafka dir: /usr/local/kafka/kafka_2.12-2.8.0/

cd /usr/local/kafka/kafka_2.12-2.8.0/
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --create --topic blocks --bootstrap-server localhost:9092
#bin/kafka-console-producer.sh --topic blocks --bootstrap-server localhost:9092
bin/kafka-console-consumer.sh --topic blocks --from-beginning --bootstrap-server localhost:9092




# Dounloading Flink
sudo tar -xvzf flink-1.13.2-bin-scala_2.12.tgz -C /usr/local/flink
cd /usr/local/flink/flink-1.13.2/
sudo ./bin/start-cluster.sh
# Flnk WebUI http://localhost:8081/#/overview
sudo ./bin/flink run -py ~/IdeaProjects/final_otus_project/proccessing.py -d
