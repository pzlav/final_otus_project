from pyflink.table import TableEnvironment, EnvironmentSettings

def log_processing():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    t_env = TableEnvironment.create(env_settings)
    # specify connector and format jars
    #t_env.get_config().get_configuration().\
    #    set_string("pipeline.jars", "file:///usr/local/flink/flink-1.13.2/flink-sql-connector-kafka_2.12-1.13.2.jar,file:///usr/local/flink/flink-1.13.2/flink-json-1.13.2.jar")
    
    source_ddl = """
            CREATE TABLE source_table(
                blocks_count INT
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'blocks',
              'properties.bootstrap.servers' = 'localhost:9092',
              'properties.group.id' = 'test_3',
              'scan.startup.mode' = 'latest-offset',
              'format' = 'json'
            )
            """

    result_data_path = "/tmp/flink_output"
    sink_ddl = f"""
            create table result_msg(
                blocks_count INT
            ) with (
                'connector' = 'filesystem',
                'format' = 'csv',
                'path' = '{result_data_path}'
            )
            """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    t_env.sql_query("SELECT blocks_count FROM source_table") \
        .execute_insert("result_msg").wait()


if __name__ == '__main__':
    log_processing()