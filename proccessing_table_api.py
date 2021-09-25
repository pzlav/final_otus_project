from pyflink.table import TableEnvironment, EnvironmentSettings

def log_processing():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    t_env = TableEnvironment.create(env_settings)
    
    source_ddl = """
            CREATE TABLE source_table(
                trNumber VARCHAR,
                blockNumber VARCHAR,
                chainId VARCHAR,
                from_field VARCHAR,
                to_field VARCHAR,
                value_field DOUBLE,
                gas DOUBLE
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
                trNumber VARCHAR,
                blockNumber VARCHAR,
                chainId VARCHAR,
                from_field VARCHAR,
                to_field VARCHAR,
                value_field DOUBLE,
                gas DOUBLE
            ) with (
                'connector' = 'filesystem',
                'format' = 'csv',
                'sink.rolling-policy.file-size' = '10KB',
                'path' = '{result_data_path}'
            )
            """


    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    
    #Something like batch processing 
    t_env.sql_query("""SELECT trNumber, blockNumber, chainId, from_field, to_field, value_field, gas FROM source_table""") \
        .execute_insert("result_msg").wait()




if __name__ == '__main__':
    log_processing()