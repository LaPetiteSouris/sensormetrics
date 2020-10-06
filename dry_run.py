# -*- coding: utf-8 -*-
from argparse import ArgumentParser

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, BatchTableEnvironment

from jobs.MetricsAggregationStream import MetricsAggregation


def define_stream_env():
    # Declare Stream environment for Stream Jobs
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    return StreamTableEnvironment.create(env)


def main():
    parser = ArgumentParser()
    parser.add_argument("-i", dest="input_file", help="input csv file")

    args = parser.parse_args()

    # define dev Flink environment
    t_env = define_stream_env()

    # JOB

    # Taking source data (cleanised data), it aggregates
    # metric on the given windows (1 HOUR).
    # In this mock-up run, it takes up  data
    # from file to execute the aggregation.

    # Refer to Metrics Aggregation's docstring for details

    stream_eec_metrics = MetricsAggregation(t_env)
    stream_eec_metrics.build_in_source(
        file_path=args.input_file,
        input_tab="eec_no_reordered_no_timezone",
    )
    stream_eec_metrics.build_out_source(file_path="./tmp/data/event_agg",
                                        output_tab="eec_agg_out")
    stream_eec_metrics.transform(input_tab="eec_no_reordered_no_timezone",
                                 output_tab="eec_agg_out")


if __name__ == "__main__":
    main()