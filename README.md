A simple project that get information from Etherium blockchain and proceed it to Hive via Kafka and Flink

Pipeline:
Etherium node (Infura) -> Kafka -> Flink (Table API) -> ~~HDFS~~ FileSystem -> HiveQL (planning)

# How to Install:
### Install Python library
pip3 install pyyaml  
pip3 install kafka-python  
pip3 install apache-flink  
sudo apt install python-is-python3  

### Run Kafka and Flink
See details in kafka-start.sh

### Prepare configuration file project.yaml
CURRRENT_BLOCK_NUBER: XXXXXXX  
INFURA_API_KEY: XXXXXXXXXXXXXXXXXXXXXXXXXX  
KAFKA_BOOTSTRAP: localhost:9092  

# QA
Yes, I'm planning to make Docker image for this project

