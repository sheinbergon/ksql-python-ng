import time

import base64
import functools
import json
import logging
from contextlib import contextmanager
from typing import cast, Generator

import httpx
from copy import deepcopy

from httpx import Client, Response, HTTPError, HTTPStatusError, Timeout, TransportError

from ksql.builder import SQLBuilder
from ksql.errors import CreateError, InvalidQueryError, KSQLError


class BaseAPI(object):
    def __init__(self, url, **kwargs):
        self.url = url
        self.max_retries = kwargs.get("max_retries", 3)
        self.delay = kwargs.get("delay", 0)
        self.timeout = kwargs.get("timeout", 15)
        self.api_key = kwargs.get("api_key")
        self.secret = kwargs.get("secret")
        self.headers = {
            "Content-Type": "application/vnd.ksql.v1+json; charset=utf-8",
        }
        self.cert = kwargs.get("cert")

    def get_timout(self):
        return self.timeout

    @staticmethod
    def _validate_sql_string(sql_string):
        if len(sql_string) > 0:
            if sql_string[-1] != ";":
                sql_string = sql_string + ";"
        else:
            raise InvalidQueryError(sql_string)
        return sql_string.strip()

    @staticmethod
    def _raise_for_status(response: Response, text: str):
        decoded = json.loads(text)
        if response.status_code != 200:
            # seems to be the new API behavior
            if decoded.get("@type") == "statement_error" or decoded.get("@type") == "generic_error":
                error_message = decoded["message"]
                error_code = decoded["error_code"]
                raise KSQLError(error_message, error_code)
            else:
                raise KSQLError("Unknown Error: {}".format(text))
        else:
            # seems to be the old API behavior, so some errors have status 200, bug??
            if decoded and decoded[0]["@type"] == "currentStatus" and decoded[0]["commandStatus"]["status"] == "ERROR":
                error_message = decoded[0]["commandStatus"]["message"]
                error_code = None
                raise KSQLError(error_message, error_code)
            return True

    def ksql(self, ksql_string, stream_properties=None):
        response = self._http1_request(
            endpoint="ksql",
            sql_string=ksql_string.strip(),
            stream_properties=stream_properties,
        )
        text = response.read().decode("utf-8")
        self._raise_for_status(response, text)
        res = json.loads(text)
        return res

    def http2_stream(
        self,
        query_string,
        encoding="utf-8",
        stream_properties=None,
    ):
        """
        Process streaming incoming data with HTTP/2.
        """
        logging.debug("KSQL generated: {}".format(query_string))
        sql_string = self._validate_sql_string(query_string)
        body = {"sql": sql_string, "properties": stream_properties or {}}
        with httpx.Client(http2=True, http1=False) as client:
            with self._http2_stream(endpoint="query-stream", body=body, client=client) as response:
                if response.status_code == 200:
                    for chunk in response.iter_bytes():
                        if chunk != b"\n":
                            yield chunk.decode(encoding)

                else:
                    body = response.read().decode('utf-8')
                    raise ValueError(f"HTTP error encountered: {response.status_code}: {body}")

    def http1_stream(
        self,
        query_string,
        encoding="utf-8",
        stream_properties=None,
    ):
        """
        Process streaming incoming data.
        """

        with self._http1_stream(
            endpoint="query",
            sql_string=query_string,
            stream_properties=stream_properties,
        ) as response:
            if response.status_code == 200:
                for chunk in response.iter_bytes():
                    if chunk != b"\n":
                        yield chunk.decode(encoding)
            else:
                body = response.read().decode('utf-8')
                raise ValueError(f"HTTP error encountered: {response.status_code}: {body}")

    def get_request(self, endpoint):
        auth = (self.api_key, self.secret) if self.api_key or self.secret else None
        return httpx.get(endpoint, headers=self.headers, auth=auth, cert=self.cert)

    @contextmanager
    def _http2_stream(self, endpoint, client: Client, body, method="POST", encoding="utf-8"):
        url = f"{self.url}/{endpoint}"
        data = json.dumps(body).encode(encoding)
        headers = deepcopy(self.headers)
        if self.api_key and self.secret:
            base64string = base64.b64encode(bytes("{}:{}".format(self.api_key, self.secret), "utf-8")).decode("utf-8")
            headers["Authorization"] = "Basic %s" % base64string

        with client.stream(
            method=method.upper(),
            url=url,
            headers=headers,
            content=data,
            timeout=self.timeout,
        ) as response_stream:
            yield response_stream

    def _http2_request(self, endpoint, client: Client, body, method="POST", encoding="utf-8") -> Response:
        url = f"{self.url}/{endpoint}"
        data = json.dumps(body).encode(encoding)
        headers = deepcopy(self.headers)
        if self.api_key and self.secret:
            base64string = base64.b64encode(bytes("{}:{}".format(self.api_key, self.secret), "utf-8")).decode("utf-8")
            headers["Authorization"] = "Basic %s" % base64string

        return client.request(
            method=method.upper(),
            url=url,
            headers=headers,
            content=data,
        )

    @contextmanager
    def _http1_stream(
        self,
        endpoint,
        method="POST",
        sql_string="",
        stream_properties=None,
        encoding="utf-8",
    ) -> Generator:
        url = f"{self.url}/{endpoint}"
        logging.debug(f"KSQL generated: {sql_string}")
        sql_string = self._validate_sql_string(sql_string)
        headers = deepcopy(self.headers)
        body = {"ksql": sql_string, "streamsProperties": stream_properties or {}}
        data = json.dumps(body).encode(encoding)
        if self.api_key and self.secret:
            base64string = base64.b64encode(bytes("{}:{}".format(self.api_key, self.secret), "utf-8")).decode("utf-8")
            headers["Authorization"] = "Basic %s" % base64string

        try:
            with httpx.stream(
                method=method.upper(),
                url=url,
                content=data,
                headers=headers,
                timeout=self.timeout,
                cert=self.cert,
            ) as response_stream:
                yield response_stream
        except TransportError as transpor_error:
            raise transpor_error
        except HTTPError as http_error:
            content = None
            try:
                if isinstance(http_error, HTTPStatusError):
                    http_error = cast(HTTPStatusError, http_error)
                    content = json.loads(http_error.response.read().decode(encoding))
            except Exception:
                raise http_error
            else:
                if content:
                    logging.debug("content: {}".format(content))
                    raise KSQLError(
                        content.get("message"),
                        content.get("error_code"),
                    )

    def _http1_request(
        self,
        endpoint,
        method="POST",
        sql_string="",
        stream_properties=None,
        encoding="utf-8",
    ) -> Response:
        url = f"{self.url}/{endpoint}"
        logging.debug(f"KSQL generated: {sql_string}")
        sql_string = self._validate_sql_string(sql_string)
        headers = deepcopy(self.headers)
        body = {"ksql": sql_string, "streamsProperties": stream_properties or {}}
        data = json.dumps(body).encode(encoding)
        if self.api_key and self.secret:
            base64string = base64.b64encode(bytes("{}:{}".format(self.api_key, self.secret), "utf-8")).decode("utf-8")
            headers["Authorization"] = "Basic %s" % base64string

        try:
            return httpx.request(
                method=method.upper(),
                url=url,
                content=data,
                headers=headers,
                timeout=self.timeout,
                cert=self.cert,
            )
        except TransportError as transpor_error:
            raise transpor_error
        except HTTPError as http_error:
            content = None
            try:
                if isinstance(http_error, HTTPStatusError):
                    http_error = cast(HTTPStatusError, http_error)
                    content = json.loads(http_error.response.read().decode(encoding))
            except Exception:
                raise http_error
            else:
                if content:
                    logging.debug("content: {}".format(content))
                    raise KSQLError(
                        content.get("message"),
                        content.get("error_code"),
                    )
                else:
                    raise http_error

    def close_query(self, query_id):
        body = {"queryId": query_id}
        data = json.dumps(body).encode("utf-8")
        url = "{}/{}".format(self.url, "close-query")

        response = httpx.post(url=url, data=data, verify=self.cert)

        if response.status_code == 200:
            logging.debug("Successfully canceled Query ID: {}".format(query_id))
            return True
        elif response.status_code == 400:
            message = json.loads(response.content)["message"]
            logging.debug("Failed canceling Query ID: {}: {}".format(query_id, message))
            return False
        else:
            raise ValueError("Return code is {}.".format(response.status_code))

    def inserts_stream(self, stream_name, rows):
        body = '{{"target":"{}"}}'.format(stream_name)
        for row in rows:
            body += f"\n{json.dumps(row)}"

        url = f"{self.url}/inserts-stream"
        headers = deepcopy(self.headers)
        with httpx.Client(http2=True, http1=False) as client:
            response = client.post(url=url, content=bytes(body, "utf-8"), headers=headers)
            result = response.read()

        result_str = result.decode("utf-8").strip()
        result_chunks = result_str.split("\n")
        return_arr = []
        for chunk in result_chunks:
            return_arr.append(json.loads(chunk))

        return return_arr

    # TODO - Replace with tenacity
    @staticmethod
    def retry(exceptions, delay=1, max_retries=5):
        """
        A decorator for retrying a function call with a specified delay in case of a set of exceptions

        Parameter List
        -------------
        :param exceptions:  A tuple of all exceptions that need to be caught for retry
                                            e.g. retry(exception_list = (Timeout, Readtimeout))
        :param delay: Amount of delay (seconds) needed between successive retries.
        :param max_retries: no of times the function should be retried
        """

        def outer_wrapper(function):
            @functools.wraps(function)
            def inner_wrapper(*args, **kwargs):
                final_exception = None
                for counter in range(max_retries):
                    if counter > 0:
                        time.sleep(delay)
                    try:
                        value = function(*args, **kwargs)
                        return value
                    except Exception as e:
                        final_exception = e
                        pass  # or log it

                if final_exception is not None:
                    raise final_exception

            return inner_wrapper

        return outer_wrapper


class SimplifiedAPI(BaseAPI):
    def __init__(self, url, **kwargs):
        super(SimplifiedAPI, self).__init__(url, **kwargs)

    def create_stream(self, table_name, columns_type, topic, value_format="JSON"):
        return self._create(
            table_type="stream",
            table_name=table_name,
            columns_type=columns_type,
            topic=topic,
            value_format=value_format,
        )

    def create_table(self, table_name, columns_type, topic, value_format, key):
        if not key:
            raise ValueError("key is required for creating a table.")
        return self._create(
            table_type="table",
            table_name=table_name,
            columns_type=columns_type,
            topic=topic,
            value_format=value_format,
            key=key,
        )

    def create_stream_as(
        self,
        table_name,
        select_columns,
        src_table,
        kafka_topic=None,
        value_format="JSON",
        conditions=(),
        partition_by=None,
        **kwargs,
    ):
        return self._create_as(
            table_type="stream",
            table_name=table_name,
            select_columns=select_columns,
            src_table=src_table,
            kafka_topic=kafka_topic,
            value_format=value_format,
            conditions=conditions,
            partition_by=partition_by,
            **kwargs,
        )

    def _create(self, table_type, table_name, columns_type, topic, value_format="JSON", key=None):
        ksql_string = SQLBuilder.build(
            sql_type="create",
            table_type=table_type,
            table_name=table_name,
            columns_type=columns_type,
            topic=topic,
            value_format=value_format,
            key=key,
        )
        self.ksql(ksql_string)
        return True

    @BaseAPI.retry(exceptions=(Timeout, CreateError))
    def _create_as(
        self,
        table_type,
        table_name,
        select_columns,
        src_table,
        kafka_topic=None,
        value_format="JSON",
        conditions=(),
        partition_by=None,
        **kwargs,
    ):
        ksql_string = SQLBuilder.build(
            sql_type="create_as",
            table_type=table_type,
            table_name=table_name,
            select_columns=select_columns,
            src_table=src_table,
            kafka_topic=kafka_topic,
            value_format=value_format,
            conditions=conditions,
            partition_by=partition_by,
            **kwargs,
        )
        self.ksql(ksql_string)
        return True
