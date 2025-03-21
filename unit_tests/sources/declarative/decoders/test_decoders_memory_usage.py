#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
import gzip
import json
import os

import pytest
import requests

from airbyte_cdk import YamlDeclarativeSource
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.declarative.models import DeclarativeStream as DeclarativeStreamModel
from airbyte_cdk.sources.declarative.parsers.model_to_component_factory import (
    ModelToComponentFactory,
)


@pytest.mark.slow
@pytest.fixture(name="large_events_response")
def large_event_response_fixture():
    data = {"email": "email1@example.com"}
    jsonl_string = f"{json.dumps(data)}\n"
    lines_in_response = 2_000_000  # ≈ 58 MB of response
    dir_path = os.path.dirname(os.path.realpath(__file__))
    file_path = f"{dir_path}/test_response.txt"
    with open(file_path, "w") as file:
        for _ in range(lines_in_response):
            file.write(jsonl_string)
    yield (lines_in_response, file_path)
    os.remove(file_path)


@pytest.mark.slow
@pytest.mark.limit_memory("20 MB")
@pytest.mark.parametrize(
    "decoder_yaml_definition",
    [
        "type: JsonlDecoder",
    ],
)
def test_jsonl_decoder_memory_usage(
    requests_mock, large_events_response, decoder_yaml_definition: str
):
    #
    lines_in_response, file_path = large_events_response
    content = f"""
    name: users
    type: DeclarativeStream
    retriever:
      type: SimpleRetriever
      decoder:
        {decoder_yaml_definition}
      paginator:
        type: "NoPagination"
      requester:
        path: "users/{{{{ stream_slice.slice }}}}"
        type: HttpRequester
        url_base: "https://for-all-mankind.nasa.com/api/v1"
        http_method: GET
        authenticator:
          type: NoAuth
        request_headers: {{}}
        request_body_json: {{}}
      record_selector:
        type: RecordSelector
        extractor:
          type: DpathExtractor
          field_path: []
      partition_router:
        type: ListPartitionRouter
        cursor_field: "slice"
        values:
          - users1
          - users2
          - users3
          - users4
    primary_key: []
        """

    factory = ModelToComponentFactory()
    stream_manifest = YamlDeclarativeSource._parse(content)
    stream = factory.create_component(
        model_type=DeclarativeStreamModel, component_definition=stream_manifest, config={}
    )

    def get_body():
        return open(file_path, "rb", buffering=30)

    counter = 0
    requests_mock.get("https://for-all-mankind.nasa.com/api/v1/users/users1", body=get_body())
    requests_mock.get("https://for-all-mankind.nasa.com/api/v1/users/users2", body=get_body())
    requests_mock.get("https://for-all-mankind.nasa.com/api/v1/users/users3", body=get_body())
    requests_mock.get("https://for-all-mankind.nasa.com/api/v1/users/users4", body=get_body())

    stream_slices = list(stream.stream_slices(sync_mode=SyncMode.full_refresh))
    for stream_slice in stream_slices:
        for _ in stream.retriever.read_records(records_schema={}, stream_slice=stream_slice):
            counter += 1

    assert counter == lines_in_response * len(stream_slices)
