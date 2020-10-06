# -*- coding: utf-8 -*-
from jobs.job_interface import JobInterface


class MetricsAggregation(JobInterface):
    """MetricsAggregation is a continuous query stream.
    For each defined window, it gives the aggregated metrics.

    Consider the Stream to be a (Dynamic) Table as input,
    the transformation is performed on this un-bounded Dynanic Table
    and outputs the aggregation to the sink.

    Refer to:
    https://flink.apache.org/news/2017/04/04/dynamic-tables.html
    """

    def __init__(self, t_env):
        super().__init__(t_env)

    def build_in_source(self, file_path: str, input_tab: str):
        """build_in_source build sink properly-formatted
        data for the EEC measurement.

        It expects the data to be:

        1. Not NULL
        2. Not empty string
        3. Properly formatted timestamp (no timezone-awareness)
        4. Not negative for measurement values.

        As of now, it is using local file system (CSV) as source data.

        [TODO] Replace source with proper Kafka/Stream topic:

        https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/kafka.html#how-to-create-a-kafka-table

        """
        query = f"""
            CREATE TABLE {input_tab} (
                    meter       STRING,

                    -- 'magic number' 3 indicates the precision timestamp. 3 means it will be rounded up to 3 decimal numbers
                    event_time  TIMESTAMP(3),

                    -- declare event_time as event time attribute and use 30 seconds delayed watermark strategy
                    -- The delay of 30s means that late-arrival data will still be processed if arriving no later
                    -- than 30s.
                    -- NOTE: event time is used in this case. Other type of time can be Ingestion time or Processing time
                    -- https://ci.apache.org/projects/flink/flink-docs-release-1.11/concepts/timely-stream-processing.html
                    
                    WATERMARK  FOR event_time AS event_time - INTERVAL '30' SECOND,

                    energy      DOUBLE,
                    power_str   DOUBLE
                )  
            WITH (
                'connector' = 'filesystem',
                'path' = '{file_path}',
                'format' = 'csv'
        )
        """
        self.t_env.execute_sql(query)

    def build_out_source(self, file_path: str, output_tab: str):
        """build_out_source build sink data for the aggregatedEEC measurement.
        The aggregate metric will be sunk here.

        As of now, it is using local file system (CSV) as source data.

        [TODO] Replace source with proper Kafka/Stream topic:

        https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/table/connectors/kafka.html#how-to-create-a-kafka-table

        """
        query = f"""
            CREATE TABLE {output_tab} (
                meter              STRING,
                start_time         TIMESTAMP,
                end_time           TIMESTAMP,
                energy_consumption DOUBLE,
                mean_power         DOUBLE,
                min_power          DOUBLE,
                max_power          DOUBLE
            ) WITH (
                'connector' = 'filesystem',
                'path' = '{file_path}',
                'format' = 'csv'
            )
        """
        self.t_env.execute_sql(query)

    def transform(self, input_tab: str, output_tab: str):
        """transform calculates the aggregated metrics in the time window"""
        # Verify the existence of data in the stream
        self.t_env.execute_sql(f"SELECT * FROM {input_tab}").print()

        # interval for window, defined in HOUR.
        # for this transformation, non-overlapping TUMBLE window
        # is used.
        # https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/operators/windows.html#tumbling-windows
        interval = "1"

        query = f"""
        SELECT 
            meter,
            -- inclusive lower bound
            TUMBLE_START(event_time, INTERVAL '{interval}' HOUR),
            -- exlusive upper bound
            TUMBLE_END(event_time, INTERVAL '{interval}' HOUR),

            -- difference in energy consumption in the period
            LAST_VALUE(energy) - FIRST_VALUE(energy),

            -- power aggregated metrics
            AVG(power_str),
            MIN(power_str),
            MAX(power_str)

        FROM {input_tab} 
            -- group by windows produce an append-only Dynamic Table
            -- with each line corresponds to the result of that window
            -- NOTE: Flink use UNIX time for interval, so the t0 moment is
            -- 00:00:00 UTC on 1 January 1970.

            GROUP BY meter, 
                    TUMBLE(event_time, INTERVAL '{interval}' HOUR)

        """
        metrics = self.t_env.sql_query(query)

        metrics.execute_insert(output_tab)

        self.t_env.execute_sql(f"SELECT * FROM {input_tab}").print()
        self.t_env.execute_sql(f"SELECT * FROM {output_tab}").print()