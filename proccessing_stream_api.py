from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.connectors import FileSink, OutputFileConfig
from pyflink.common.serialization import Encoder
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
# the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
env.add_jars("file:///usr/local/flink/flink-1.13.2/flink-sql-connector-kafka_2.12-1.13.2.jar")
t_env = StreamTableEnvironment.create(stream_execution_environment=env)


type_info=Types.ROW([Types.INT()])
deserialization_schema = JsonRowDeserializationSchema.builder() \
    .type_info(type_info=type_info).build()

kafka_consumer = FlinkKafkaConsumer(
    topics='blocks',
    deserialization_schema=deserialization_schema,
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

ds = env.add_source(kafka_consumer)

ds.print()

output_path = '/tmp/flink/'
file_sink = FileSink \
    .for_row_format(output_path, Encoder.simple_string_encoder()) \
    .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
    .build()
ds.sink_to(file_sink)

env.execute()

# emit ds to print sink
t_env.execute_sql("""
        CREATE TABLE my_sink (
            b INT
        ) WITH (
                'connector' = 'filesystem',
                'format' = 'csv',
                'path' = '{output_path}'
        )
    """)

table = t_env.from_data_stream(ds)
table_result = table.execute_insert("my_sink")