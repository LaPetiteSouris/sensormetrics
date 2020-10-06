# -*- coding: utf-8 -*-


class JobInterface(object):
    """JobInterface serves as the interface to define how a Flink job (Stream or Batch)
    should be organized.

    In general, a job is defined as
    input -> transformation -> output

    Consider a job as a function, which takes input and gives output. It can be
    either stateless (as a filter job) or stateful (as in aggregation job). But to facilitate
    the development, any job must have

        1. One input (source)
        2. One output (sink)
        3. Transformation of data from source to sink

    Thus, anything which may involves chaining up data (multiple transfomration) should be devided
    into multiple job. This helps in debugging, failure recovery and above all, easier to comprehend


    """

    def __init__(self, t_env):
        self.t_env = t_env

    def build_in_source(self, intput_tab: str, *args, **kwargs):
        """ build_in_source handle the decleration of source data (csv, table, Kafka stream...etc)"""
        raise NotImplementedError(f"user must define for this method")

    def build_out_source(self, output_tab: str, *args, **kwargs):
        """ build_out_source handle the decleration of source data (csv, table, Kafka stream...etc)"""
        raise NotImplementedError(f"user must define for this method")

    def transform(self, input_tab: str, output_tab: str, *args, **kwargs):
        """transform takes data out of source, perform transformation/aggregation
        and sinks the data back to the defined sink"""
        raise NotImplementedError(f"user must define for this method")
