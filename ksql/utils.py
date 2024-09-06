from typing import Generator

import ksql
import telnetlib
import json


def check_kafka_available(bootstrap_servers):
    host, port = bootstrap_servers.split(":")
    try:
        telnetlib.Telnet(host, port)
        return True
    except Exception:
        return False


def get_all_streams(api_client, prefix=None):
    all_streams = api_client.ksql("""SHOW STREAMS;""")
    filtered_streams = []
    for stream in all_streams[0]["streams"]:
        if stream["type"] != "STREAM":
            continue
        if prefix and not stream["name"].startswith(prefix.upper()):
            continue
        filtered_streams.append(stream["name"])
    return filtered_streams


def get_stream_info(api_client, stream_name):
    try:
        r = api_client.ksql("""DESCRIBE {} EXTENDED""".format(stream_name))
    except ksql.errors.KSQLError as e:
        if e.error_code == 40001:
            return None
        else:
            raise
    stream_info = r[0]["sourceDescription"]
    return stream_info


def drop_all_streams(api_client, prefix=None):
    filtered_streams = get_all_streams(api_client, prefix=prefix)
    second_pass = []
    iterate_and_drop_streams(api_client, filtered_streams, second_pass.append)
    iterate_and_drop_streams(api_client, second_pass)


def iterate_and_drop_streams(api_client, streams: list[str], error_handler=lambda x: None):
    for stream in streams:
        try:
            drop_stream(api_client, stream)
        except Exception:
            error_handler(stream)


def drop_stream(api_client, stream_name):
    read_queries, write_queries = get_dependent_queries(api_client, stream_name)
    dependent_queries = read_queries + write_queries
    for query in dependent_queries:
        api_client.ksql("""TERMINATE {};""".format(query))
    api_client.ksql("""DROP STREAM IF EXISTS {};""".format(stream_name))


def get_dependent_queries(api_client, stream_name):
    stream_info = get_stream_info(api_client, stream_name)
    read_queries = []
    write_queries = []
    if stream_info and stream_info["readQueries"]:
        read_queries = [query["id"] for query in stream_info["readQueries"]]
    if stream_info and stream_info["writeQueries"]:
        write_queries = [query["id"] for query in stream_info["writeQueries"]]

    return read_queries, write_queries


def process_row(row, column_names):
    row = row.strip()
    decoded = json.loads(row)
    return {column: value for column, value in zip(column_names, decoded)}


def parse_query_results(results) -> Generator:
    # parse rows into objects
    try:
        header = next(results)
    except StopIteration:
        return

    decoded = json.loads(header)
    for result in results:
        row_obj = process_row(result, decoded["columnNames"])
        if row_obj is None:
            return
        yield row_obj
