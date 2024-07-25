import secrets
import unittest
import json
from time import sleep

import httpx
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

import ksql
import ksql.utils as utils
from ksql import KSQLAPI
from ksql import SQLBuilder
from ksql.errors import KSQLError


class TestKSQLAPI(unittest.TestCase):
    """Test case for the client methods."""

    def setUp(self):
        self.url = "http://localhost:8088"
        self.api_client = KSQLAPI(url=self.url, check_version=False, timeout=30)
        self.test_prefix = "ksql_python_test"
        self.test_topic = f"test_topic_{secrets.randbelow(10000)}"
        self.bootstrap_servers = "localhost:29092"
        if utils.check_kafka_available(self.bootstrap_servers):
            producer = Producer({"bootstrap.servers": self.bootstrap_servers})
            producer.produce(self.test_topic, "test_message")
            producer.flush()
            admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            while self.test_topic not in set(admin.list_topics().topics.keys()):
                sleep(0.5)

    def tearDown(self):
        if utils.check_kafka_available(self.bootstrap_servers):
            utils.drop_all_streams(self.api_client)

    def test_get_url(self):
        self.assertEqual(self.api_client.get_url(), "http://localhost:8088")

    def test_with_timeout(self):
        api_client = KSQLAPI(url=self.url, timeout=10, check_version=False)
        self.assertEqual(api_client.timeout, 10)

    def test_ksql_server_healthcheck(self):
        """Test GET requests"""
        res = httpx.get(self.url + "/status")
        self.assertEqual(res.status_code, 200)

    def test_get_ksql_version_success(self):
        """Test GET requests"""
        version = self.api_client.get_ksql_version()
        self.assertEqual(version, ksql.__ksql_server_version__)

    def test_get_properties(self):
        properties = self.api_client.get_properties()
        property = [i for i in properties if i["name"] == "ksql.schema.registry.url"][0]
        self.assertEqual(property.get("value"), "http://schema-registry:8081")

    def test_ksql_show_tables_with_api_key(self):
        api_client = KSQLAPI(url=self.url, check_version=False, api_key="foo", secret="bar")
        ksql_string = "show tables;"
        r = api_client.ksql(ksql_string)
        self.assertEqual(r, [{"@type": "tables", "statementText": "show tables;", "tables": [], "warnings": []}])

    def test_ksql_show_tables(self):
        """Test GET requests"""
        ksql_string = "show tables;"
        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r, [{"@type": "tables", "statementText": "show tables;", "tables": [], "warnings": []}])

    def test_ksql_show_tables_with_no_semicolon(self):
        """Test GET requests"""
        ksql_string = "show tables"
        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r, [{"@type": "tables", "statementText": "show tables;", "tables": [], "warnings": []}])

    def test_ksql_create_stream(self):
        """Test GET requests"""
        topic = self.test_topic
        stream_name = self.test_prefix + "test_ksql_create_stream"
        ksql_string = f"CREATE STREAM {stream_name} (viewtime bigint, userid varchar, pageid varchar) \
                       WITH (kafka_topic='{topic}', value_format='DELIMITED');"
        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r[0]["commandStatus"]["status"], "SUCCESS")

    def test_ksql_create_stream_w_properties(self):
        """Test GET requests"""
        topic = self.test_topic
        stream_name = "TEST_KSQL_CREATE_STREAM"
        ksql_string = f"CREATE STREAM {stream_name} (ORDER_ID INT, TOTAL_AMOUNT DOUBLE, CUSTOMER_NAME VARCHAR) \
                       WITH (kafka_topic='{topic}', value_format='JSON');"
        stream_properties = {"ksql.streams.auto.offset.reset": "earliest"}

        if stream_name not in utils.get_all_streams(self.api_client):
            r = self.api_client.ksql(ksql_string, stream_properties=stream_properties)
            self.assertEqual(r[0]["commandStatus"]["status"], "SUCCESS")

        producer = Producer({"bootstrap.servers": self.bootstrap_servers})
        producer.produce(topic, """{"order_id":3,"total_amount":43,"customer_name":"Palo Alto"}""")
        producer.flush()

        # test legacy HTTP/1.1 request
        chunks = self.api_client.stream(
            f"SELECT * FROM {stream_name} EMIT CHANGES;",
            stream_properties=stream_properties,
        )

        header = json.loads(next(chunks))

        self.assertEqual(json.dumps(header["columnNames"]), '["ORDER_ID", "TOTAL_AMOUNT", "CUSTOMER_NAME"]')
        self.assertEqual(json.dumps(header["columnTypes"]), '["INTEGER", "DOUBLE", "STRING"]')

        for chunk in chunks:
            self.assertEqual(chunk.strip(), '[3,43.0,"Palo Alto"]')
            break

        # test new HTTP/2 request

        chunks = self.api_client.stream(
            f"select * from {stream_name} EMIT CHANGES;",
            stream_properties=stream_properties,
            use_http2=True,
        )

        header = json.loads(next(chunks))
        self.assertEqual(json.dumps(header["columnNames"]), '["ORDER_ID", "TOTAL_AMOUNT", "CUSTOMER_NAME"]')
        self.assertEqual(json.dumps(header["columnTypes"]), '["INTEGER", "DOUBLE", "STRING"]')

        for chunk in chunks:
            chunk_obj = json.loads(chunk)
            self.assertEqual(chunk_obj, [3, 43.0, "Palo Alto"])
            break

    def test_ksql_close_query(self):
        result = self.api_client.close_query("123")

        self.assertFalse(result)

    def test_inserts_stream(self):
        topic = self.test_topic
        stream_name = "TEST_INSERTS_STREAM_STREAM"
        ksql_string = "CREATE STREAM {} (ORDER_ID INT, TOTAL_AMOUNT DOUBLE, CUSTOMER_NAME VARCHAR) \
        WITH (kafka_topic='{}', value_format='JSON');".format(
            stream_name, topic
        )

        stream_properties = {"ksql.streams.auto.offset.reset": "earliest"}

        if "TEST_KSQL_CREATE_STREAM" not in utils.get_all_streams(self.api_client):
            r = self.api_client.ksql(ksql_string, stream_properties=stream_properties)
            self.assertEqual(r[0]["commandStatus"]["status"], "SUCCESS")

        rows = [
            {"ORDER_ID": 1, "TOTAL_AMOUNT": 23.5, "CUSTOMER_NAME": "abc"},
            {"ORDER_ID": 2, "TOTAL_AMOUNT": 3.7, "CUSTOMER_NAME": "xyz"},
        ]

        results = self.api_client.inserts_stream(stream_name, rows)

        for result in results:
            self.assertEqual(result["status"], "ok")

    def test_ksql_parse_pull_query_result(self):
        topic = "TEST_KSQL_PARSE_QUERY_RESULT_TOPIC"
        stream_name = "TEST_KSQL_PARSE_QUERY_RESULT_STREAM"

        producer = Producer({"bootstrap.servers": self.bootstrap_servers})
        producer.produce(
            topic,
            json.dumps(
                {
                    "order_id": 3,
                    "my_struct": {"a": 1, "b": "bbb"},
                    "my_map": {"x": 3, "y": 4},
                    "my_array": [1, 2, 3],
                    "total_amount": 43,
                    "customer_name": "Palo Alto",
                }
            ),
        )
        producer.flush()

        ksql_string = "CREATE STREAM {} (ORDER_ID INT, MY_STRUCT STRUCT<A INT, B VARCHAR>, MY_MAP MAP<VARCHAR, INT>, MY_ARRAY ARRAY<INT>, TOTAL_AMOUNT DOUBLE, CUSTOMER_NAME VARCHAR) \
                       WITH (kafka_topic='{}', value_format='JSON');".format(
            stream_name, topic
        )
        stream_properties = {"ksql.streams.auto.offset.reset": "earliest"}

        if stream_name not in utils.get_all_streams(self.api_client):
            r = self.api_client.ksql(ksql_string, stream_properties=stream_properties)
            self.assertEqual(r[0]["commandStatus"]["status"], "SUCCESS")

        chunks = self.api_client.stream(
            "select * from {} EMIT CHANGES".format(stream_name),
            stream_properties=stream_properties,
            parse=True,
        )

        for chunk in chunks:
            self.assertEqual(chunk["ORDER_ID"], 3)
            self.assertEqual(chunk["MY_STRUCT"], {"A": 1, "B": "bbb"})
            self.assertEqual(chunk["MY_MAP"], {"x": 3, "y": 4})
            self.assertEqual(chunk["MY_ARRAY"], [1, 2, 3])
            self.assertEqual(chunk["TOTAL_AMOUNT"], 43)
            self.assertEqual(chunk["CUSTOMER_NAME"], "Palo Alto")
            break

    def test_ksql_parse_push_query(self):
        topic = "TEST_KSQL_PARSE_QUERY_FINAL_MESSAGE_TOPIC"
        stream_name = "TEST_KSQL_PARSE_QUERY_FINAL_MESSAGE_STREAM"

        producer = Producer({"bootstrap.servers": self.bootstrap_servers})
        producer.produce(
            topic,
            json.dumps(
                {
                    "order_id": 3,
                    "my_struct": {"a": 1, "b": "bbb"},
                    "my_map": {"x": 3, "y": 4},
                    "my_array": [1, 2, 3],
                    "total_amount": 43,
                    "customer_name": "Palo Alto",
                }
            ),
        )
        producer.flush()

        ksql_string = f"""
            CREATE STREAM {stream_name} (
                ORDER_ID INT,
                MY_STRUCT STRUCT<A INT, B VARCHAR>,
                MY_MAP MAP<VARCHAR, INT>,
                MY_ARRAY ARRAY<INT>,
                TOTAL_AMOUNT DOUBLE,
                CUSTOMER_NAME VARCHAR
            )
            WITH (kafka_topic='{topic}', value_format='JSON');
        """.strip()
        stream_properties = {"ksql.streams.auto.offset.reset": "earliest"}

        if stream_name not in utils.get_all_streams(self.api_client):
            r = self.api_client.ksql(ksql_string, stream_properties=stream_properties)
            self.assertEqual(r[0]["commandStatus"]["status"], "SUCCESS")

        chunks = self.api_client.stream(
            f"select * from {stream_name} EMIT CHANGES LIMIT 1", stream_properties=stream_properties, parse=True
        )

        for row_obj in chunks:
            self.assertEqual(row_obj["ORDER_ID"], 3)
            self.assertEqual(row_obj["MY_STRUCT"], {"A": 1, "B": "bbb"})
            self.assertEqual(row_obj["MY_MAP"], {"x": 3, "y": 4})
            self.assertEqual(row_obj["MY_ARRAY"], [1, 2, 3])
            self.assertEqual(row_obj["TOTAL_AMOUNT"], 43)
            self.assertEqual(row_obj["CUSTOMER_NAME"], "Palo Alto")

    def test_bad_requests(self):
        broken_ksql_string = "noi"
        with self.assertRaises(KSQLError) as e:
            self.api_client.ksql(broken_ksql_string)

        exception = e.exception
        self.assertEqual(exception.error_code, 40001)

    def test_ksql_create_stream_by_builder(self):
        sql_type = "create"
        table_type = "stream"
        table_name = "test_table"
        columns_type = ["viewtime bigint", "userid varchar", "pageid varchar"]
        topic = self.test_topic
        value_format = "DELIMITED"

        utils.drop_stream(self.api_client, table_name)

        ksql_string = SQLBuilder.build(
            sql_type=sql_type,
            table_type=table_type,
            table_name=table_name,
            columns_type=columns_type,
            topic=topic,
            value_format=value_format,
        )

        r = self.api_client.ksql(ksql_string)
        self.assertEqual(r[0]["commandStatus"]["status"], "SUCCESS")

    def test_ksql_create_stream_by_builder_api(self):
        table_name = "test_table"
        columns_type = ["viewtime bigint", "userid varchar", "pageid varchar"]
        topic = self.test_topic
        value_format = "DELIMITED"

        utils.drop_stream(self.api_client, table_name)

        r = self.api_client.create_stream(
            table_name=table_name, columns_type=columns_type, topic=topic, value_format=value_format
        )

        self.assertTrue(r)

    def test_raise_create_error_topic_already_registered(self):
        table_name = "foo_table"
        columns_type = ["name string", "age bigint"]
        topic = self.test_topic
        value_format = "DELIMITED"
        utils.drop_stream(self.api_client, table_name)
        self.api_client.create_stream(
            table_name=table_name, columns_type=columns_type, topic=topic, value_format=value_format
        )

        with self.assertRaises(KSQLError):
            self.api_client.create_stream(
                table_name=table_name, columns_type=columns_type, topic=topic, value_format=value_format
            )

    def test_raise_create_error_no_topic(self):
        table_name = "foo_table"
        columns_type = ["name string", "age bigint"]
        topic = "this_topic_is_not_exist"
        value_format = "DELIMITED"

        with self.assertRaises(KSQLError):
            self.api_client.create_stream(
                table_name=table_name, columns_type=columns_type, topic=topic, value_format=value_format
            )

    def test_create_stream_as_without_conditions(self):

        src_table = "pageviews_original"
        columns_type = ["name string", "age bigint", "userid string", "pageid bigint"]
        topic = self.test_topic

        table_name = "create_stream_as_without_conditions"
        kafka_topic = "create_stream_as_without_conditions"
        value_format = "DELIMITED"
        select_columns = ["rowtime as logtime", "*"]

        try:
            self.api_client.create_stream(
                table_name=src_table, columns_type=columns_type, topic=topic, value_format=value_format
            )
        except KSQLError:
            pass

        r = self.api_client.create_stream_as(
            table_name=table_name,
            src_table=src_table,
            kafka_topic=kafka_topic,
            select_columns=select_columns,
            timestamp="logtime",
            value_format=value_format,
        )
        self.assertTrue(r)

    def test_create_stream_as_with_conditions_without_startwith(self):

        src_table = "pageviews_original"
        columns_type = ["name string", "age bigint", "userid string", "pageid bigint"]
        topic = self.test_topic

        table_name = "create_stream_as_with_conditions_without_startwith"
        kafka_topic = "create_stream_as_with_conditions_without_startwith"
        value_format = "DELIMITED"
        select_columns = ["rowtime as logtime", "*"]
        conditions = "userid = 'foo'"

        try:
            r = self.api_client.create_stream(
                table_name=src_table, columns_type=columns_type, topic=topic, value_format=value_format
            )
        except KSQLError:
            pass

        r = self.api_client.create_stream_as(
            table_name=table_name,
            src_table=src_table,
            kafka_topic=kafka_topic,
            select_columns=select_columns,
            timestamp="logtime",
            value_format=value_format,
            conditions=conditions,
        )

        self.assertTrue(r)

    def test_create_stream_as_with_conditions_with_startwith(self):

        src_table = "pageviews_original"
        columns_type = ["name string", "age bigint", "userid string", "pageid bigint"]
        topic = self.test_topic

        table_name = "create_stream_as_with_conditions_with_startwith"
        kafka_topic = "create_stream_as_with_conditions_with_startwith"
        value_format = "DELIMITED"
        select_columns = ["rowtime as logtime", "*"]
        conditions = "userid = 'foo_%'"
        utils.drop_stream(self.api_client, src_table)
        utils.drop_stream(self.api_client, table_name)

        try:
            r = self.api_client.create_stream(
                table_name=src_table, columns_type=columns_type, topic=topic, value_format=value_format
            )
        except KSQLError:
            pass

        r = self.api_client.create_stream_as(
            table_name=table_name,
            src_table=src_table,
            kafka_topic=kafka_topic,
            select_columns=select_columns,
            timestamp="logtime",
            value_format=value_format,
            conditions=conditions,
        )

        self.assertTrue(r)

    def test_create_stream_as_with_conditions_with_startwith_with_and(self):

        src_table = "pageviews_original"
        columns_type = ["name string", "age bigint", "userid string", "pageid bigint"]
        topic = self.test_topic

        table_name = "create_stream_as_with_conditions_with_startwith_with_and"
        kafka_topic = "create_stream_as_with_conditions_with_startwith_with_and"
        value_format = "DELIMITED"
        select_columns = ["rowtime as logtime", "*"]
        conditions = "userid = 'foo_%' and age > 10"

        try:
            r = self.api_client.create_stream(
                table_name=src_table, columns_type=columns_type, topic=topic, value_format=value_format
            )
        except KSQLError:
            pass

        r = self.api_client.create_stream_as(
            table_name=table_name,
            src_table=src_table,
            kafka_topic=kafka_topic,
            select_columns=select_columns,
            timestamp="logtime",
            value_format=value_format,
            conditions=conditions,
        )

        self.assertTrue(r)

    def test_ksql_create_stream_as_with_wrong_timestamp(self):
        src_table = "prebid_traffic_log_total_stream"
        columns_type = ["name string", "age bigint", "userid string", "pageid bigint"]
        topic = self.test_topic

        table_name = "prebid_traffic_log_valid_stream"
        kafka_topic = "prebid_traffic_log_valid_topic"
        value_format = "DELIMITED"
        select_columns = ["*"]
        timestamp = "foo"
        utils.drop_stream(self.api_client, src_table)
        utils.drop_stream(self.api_client, table_name)
        try:
            self.api_client.create_stream(
                table_name=src_table, columns_type=columns_type, topic=topic, value_format=value_format
            )
        except KSQLError:
            pass

        with self.assertRaises(KSQLError):
            self.api_client.create_stream_as(
                table_name=table_name,
                src_table=src_table,
                kafka_topic=kafka_topic,
                select_columns=select_columns,
                timestamp=timestamp,
                value_format=value_format,
            )
