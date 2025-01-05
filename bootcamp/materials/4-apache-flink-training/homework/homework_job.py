import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session

    """
    This flink job is configured to be initialized in the make file of the project and can be initialized like "make sessionize_job"
    Once the job starts without issues visit localhost:8081 where flink's webserver is up and running
    
    Validation
    Open postgres and query sessionized_events table periodically to see rows being added like below
    
    "106.179.57.236"	"www.dataexpert.io"	"2025-01-05 15:50:08.83"	"2025-01-05 15:55:08.83"	1
    "184.91.89.78"	"bootcamp.techcreator.io"	"2025-01-05 15:50:55.225"	"2025-01-05 15:55:55.225"	1
    "119.156.124.61"	"bootcamp.techcreator.io"	"2025-01-05 15:50:14.562"	"2025-01-05 15:55:14.562"	1
    "87.217.24.227"	"www.dataexpert.io"	"2025-01-05 15:50:35.208"	"2025-01-05 15:55:42.521"	3
    "175.143.63.148"	"bootcamp.techcreator.io"	"2025-01-05 15:50:21.034"	"2025-01-05 15:55:21.034"	1
    """

def create_session_sink(t_env):
    """
    This creates a sink to store values from kafka
    """
    session_sink_table = "sessionized_events"
    sink_ddl = f"""
        CREATE TABLE {session_sink_table} (
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_hits BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{session_sink_table}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return session_sink_table

def create_processed_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "process_events_kafka"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            referrer VARCHAR,
            host VARCHAR,
            url VARCHAR,
            geodata VARCHAR,
            window_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR window_timestamp AS window_timestamp - INTERVAL '15' SECOND
        ) WITH (
             'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def sessionize_logs():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        
        source_table = create_processed_events_source_kafka(t_env)
        session_sink_table = create_session_sink(t_env)
        # Perform sessionization
        # using session with gap and aggregating hits per session
        t_env.from_path(source_table) \
            .window(
                Session.with_gap(lit(5).minutes).on(col("window_timestamp")).alias("w")
            ).group_by(
                col("ip"), col("host"), col("w")
            ).select(
                col("ip"),
                col("host"),
                col("w").start.alias("session_start"),
                col("w").end.alias("session_end"),
                col("host").count.alias("num_hits")
                ) \
            .execute_insert(session_sink_table) \
            .wait()

    except Exception as e:
        print("Sessionization failed:", str(e))
        
if __name__ == '__main__':
    sessionize_logs()