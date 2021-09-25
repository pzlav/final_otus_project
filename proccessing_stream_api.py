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


    sink_ddl_kafka = f"""
            create table result_msg_kafka(
                trNumber VARCHAR,
                blockNumber VARCHAR,
                value_field DOUBLE
            ) with (
              'connector' = 'kafka',
              'topic' = 'output',
              'properties.bootstrap.servers' = 'localhost:9092',
              'format' = 'json'
            )
            """


    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl_kafka)

    #Something like stream processing 
    t_env.sql_query("""
                        SELECT trNumber, blockNumber, value_field
                        FROM source_table
                        WHERE value_field > 10
                    """) \
        .execute_insert("result_msg_kafka").wait()



if __name__ == '__main__':
    log_processing()