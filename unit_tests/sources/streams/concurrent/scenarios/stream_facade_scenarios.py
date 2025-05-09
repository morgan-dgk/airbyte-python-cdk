#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
from airbyte_cdk.sources.streams.concurrent.cursor import CursorField
from airbyte_cdk.sources.types import StreamSlice
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from unit_tests.sources.file_based.scenarios.scenario_builder import (
    IncrementalScenarioConfig,
    TestScenarioBuilder,
)
from unit_tests.sources.streams.concurrent.scenarios.stream_facade_builder import (
    StreamFacadeSourceBuilder,
)
from unit_tests.sources.streams.concurrent.scenarios.utils import MockStream

_stream1 = MockStream(
    [
        (None, [{"id": "1"}, {"id": "2"}]),
    ],
    "stream1",
    json_schema={
        "type": "object",
        "properties": {
            "id": {"type": ["null", "string"]},
        },
    },
)

_stream_raising_exception = MockStream(
    [
        (None, [{"id": "1"}, ValueError("test exception")]),
    ],
    "stream1",
    json_schema={
        "type": "object",
        "properties": {
            "id": {"type": ["null", "string"]},
        },
    },
)

_stream_with_primary_key = MockStream(
    [
        (None, [{"id": "1"}, {"id": "2"}]),
    ],
    "stream1",
    json_schema={
        "type": "object",
        "properties": {
            "id": {"type": ["null", "string"]},
        },
    },
    primary_key="id",
)

_stream2 = MockStream(
    [
        (None, [{"id": "A"}, {"id": "B"}]),
    ],
    "stream2",
    json_schema={
        "type": "object",
        "properties": {
            "id": {"type": ["null", "string"]},
        },
    },
)

_stream_with_single_slice = MockStream(
    [
        ({"slice_key": "s1"}, [{"id": "1"}, {"id": "2"}]),
    ],
    "stream1",
    json_schema={
        "type": "object",
        "properties": {
            "id": {"type": ["null", "string"]},
        },
    },
)

_stream_with_multiple_slices = MockStream(
    [
        ({"slice_key": "s1"}, [{"id": "1"}, {"id": "2"}]),
        ({"slice_key": "s2"}, [{"id": "3"}, {"id": "4"}]),
    ],
    "stream1",
    json_schema={
        "type": "object",
        "properties": {
            "id": {"type": ["null", "string"]},
        },
    },
)

test_stream_facade_single_stream = (
    TestScenarioBuilder()
    .set_name("test_stream_facade_single_stream")
    .set_config({})
    .set_source_builder(StreamFacadeSourceBuilder().set_streams([_stream1]))
    .set_expected_records(
        [
            {"data": {"id": "1"}, "stream": "stream1"},
            {"data": {"id": "2"}, "stream": "stream1"},
        ]
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["null", "string"]},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh"],
                    "is_resumable": False,
                    "is_file_based": False,
                }
            ]
        }
    )
    .set_expected_logs(
        {
            "read": [
                {"level": "INFO", "message": "Starting syncing"},
                {"level": "INFO", "message": "Marking stream stream1 as STARTED"},
                {"level": "INFO", "message": "Syncing stream: stream1"},
                {"level": "INFO", "message": "Marking stream stream1 as RUNNING"},
                {"level": "INFO", "message": "Read 2 records from stream1 stream"},
                {"level": "INFO", "message": "Marking stream stream1 as STOPPED"},
                {"level": "INFO", "message": "Finished syncing stream1"},
                {"level": "INFO", "message": "Finished syncing"},
            ]
        }
    )
    .set_log_levels({"ERROR", "WARN", "WARNING", "INFO", "DEBUG"})
    .build()
)

test_stream_facade_raises_exception = (
    TestScenarioBuilder()
    .set_name("test_stream_facade_raises_exception")
    .set_config({})
    .set_source_builder(StreamFacadeSourceBuilder().set_streams([_stream_raising_exception]))
    .set_expected_records(
        [
            {"data": {"id": "1"}, "stream": "stream1"},
        ]
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["null", "string"]},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh"],
                    "is_resumable": False,
                    "is_file_based": False,
                }
            ]
        }
    )
    .set_expected_read_error(AirbyteTracedException, "Concurrent read failure")
    .build()
)

test_stream_facade_single_stream_with_primary_key = (
    TestScenarioBuilder()
    .set_name("test_stream_facade_stream_with_primary_key")
    .set_config({})
    .set_source_builder(StreamFacadeSourceBuilder().set_streams([_stream1]))
    .set_expected_records(
        [
            {"data": {"id": "1"}, "stream": "stream1"},
            {"data": {"id": "2"}, "stream": "stream1"},
        ]
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["null", "string"]},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh"],
                    "is_resumable": False,
                    "is_file_based": False,
                }
            ]
        }
    )
    .build()
)

test_stream_facade_multiple_streams = (
    TestScenarioBuilder()
    .set_name("test_stream_facade_multiple_streams")
    .set_config({})
    .set_source_builder(StreamFacadeSourceBuilder().set_streams([_stream1, _stream2]))
    .set_expected_records(
        [
            {"data": {"id": "1"}, "stream": "stream1"},
            {"data": {"id": "2"}, "stream": "stream1"},
            {"data": {"id": "A"}, "stream": "stream2"},
            {"data": {"id": "B"}, "stream": "stream2"},
        ]
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["null", "string"]},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh"],
                    "is_resumable": False,
                    "is_file_based": False,
                },
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["null", "string"]},
                        },
                    },
                    "name": "stream2",
                    "supported_sync_modes": ["full_refresh"],
                    "is_resumable": False,
                    "is_file_based": False,
                },
            ]
        }
    )
    .build()
)

test_stream_facade_single_stream_with_single_slice = (
    TestScenarioBuilder()
    .set_name("test_stream_facade_single_stream_with_single_slice")
    .set_config({})
    .set_source_builder(StreamFacadeSourceBuilder().set_streams([_stream1]))
    .set_expected_records(
        [
            {"data": {"id": "1"}, "stream": "stream1"},
            {"data": {"id": "2"}, "stream": "stream1"},
        ]
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["null", "string"]},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh"],
                    "is_resumable": False,
                    "is_file_based": False,
                }
            ]
        }
    )
    .build()
)

test_stream_facade_single_stream_with_multiple_slices = (
    TestScenarioBuilder()
    .set_name("test_stream_facade_single_stream_with_multiple_slice")
    .set_config({})
    .set_source_builder(StreamFacadeSourceBuilder().set_streams([_stream_with_multiple_slices]))
    .set_expected_records(
        [
            {"data": {"id": "1"}, "stream": "stream1"},
            {"data": {"id": "2"}, "stream": "stream1"},
            {"data": {"id": "3"}, "stream": "stream1"},
            {"data": {"id": "4"}, "stream": "stream1"},
        ]
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["null", "string"]},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh"],
                    "is_resumable": False,
                    "is_file_based": False,
                }
            ]
        }
    )
    .build()
)

test_stream_facade_single_stream_with_multiple_slices_with_concurrency_level_two = (
    TestScenarioBuilder()
    .set_name("test_stream_facade_single_stream_with_multiple_slice_with_concurrency_level_two")
    .set_config({})
    .set_source_builder(StreamFacadeSourceBuilder().set_streams([_stream_with_multiple_slices]))
    .set_expected_records(
        [
            {"data": {"id": "1"}, "stream": "stream1"},
            {"data": {"id": "2"}, "stream": "stream1"},
            {"data": {"id": "3"}, "stream": "stream1"},
            {"data": {"id": "4"}, "stream": "stream1"},
        ]
    )
    .set_expected_catalog(
        {
            "streams": [
                {
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["null", "string"]},
                        },
                    },
                    "name": "stream1",
                    "supported_sync_modes": ["full_refresh"],
                    "is_resumable": False,
                    "is_file_based": False,
                }
            ]
        }
    )
    .build()
)


test_incremental_stream_with_slice_boundaries = (
    TestScenarioBuilder()
    .set_name("test_incremental_stream_with_slice_boundaries")
    .set_config({})
    .set_source_builder(
        StreamFacadeSourceBuilder()
        .set_streams(
            [
                MockStream(
                    [
                        (
                            StreamSlice(partition={"from": 0, "to": 1}, cursor_slice={}),
                            [{"id": "1", "cursor_field": 0}, {"id": "2", "cursor_field": 1}],
                        ),
                        (
                            StreamSlice(partition={"from": 1, "to": 2}, cursor_slice={}),
                            [{"id": "3", "cursor_field": 2}, {"id": "4", "cursor_field": 3}],
                        ),
                    ],
                    "stream1",
                    cursor_field="cursor_field",
                    json_schema={
                        "type": "object",
                        "properties": {
                            "id": {"type": ["null", "string"]},
                        },
                    },
                )
            ]
        )
        .set_incremental(CursorField("cursor_field"), ("from", "to"))
    )
    .set_expected_records(
        [
            {"data": {"id": "1", "cursor_field": 0}, "stream": "stream1"},
            {"data": {"id": "2", "cursor_field": 1}, "stream": "stream1"},
            {"cursor_field": 1},
            {"data": {"id": "3", "cursor_field": 2}, "stream": "stream1"},
            {"data": {"id": "4", "cursor_field": 3}, "stream": "stream1"},
            {"cursor_field": 3},
            {"cursor_field": 3},  # see Cursor.ensure_at_least_one_state_emitted
        ]
    )
    .set_log_levels({"ERROR", "WARN", "WARNING", "INFO", "DEBUG"})
    .set_incremental_scenario_config(
        IncrementalScenarioConfig(
            input_state=[],
        )
    )
    .build()
)


_NO_SLICE_BOUNDARIES = None
test_incremental_stream_without_slice_boundaries = (
    TestScenarioBuilder()
    .set_name("test_incremental_stream_without_slice_boundaries")
    .set_config({})
    .set_source_builder(
        StreamFacadeSourceBuilder()
        .set_streams(
            [
                MockStream(
                    [
                        (None, [{"id": "1", "cursor_field": 0}, {"id": "2", "cursor_field": 3}]),
                    ],
                    "stream1",
                    cursor_field="cursor_field",
                    json_schema={
                        "type": "object",
                        "properties": {
                            "id": {"type": ["null", "string"]},
                        },
                    },
                )
            ]
        )
        .set_incremental(CursorField("cursor_field"), _NO_SLICE_BOUNDARIES)
    )
    .set_expected_records(
        [
            {"data": {"id": "1", "cursor_field": 0}, "stream": "stream1"},
            {"data": {"id": "2", "cursor_field": 3}, "stream": "stream1"},
            {"cursor_field": 3},
            {"cursor_field": 3},  # see Cursor.ensure_at_least_one_state_emitted
        ]
    )
    .set_log_levels({"ERROR", "WARN", "WARNING", "INFO", "DEBUG"})
    .set_incremental_scenario_config(
        IncrementalScenarioConfig(
            input_state=[],
        )
    )
    .build()
)

test_incremental_stream_with_many_slices_but_without_slice_boundaries = (
    TestScenarioBuilder()
    .set_name("test_incremental_stream_with_many_slices_but_without_slice_boundaries")
    .set_config({})
    .set_source_builder(
        StreamFacadeSourceBuilder()
        .set_streams(
            [
                MockStream(
                    [
                        (
                            StreamSlice(partition={"parent_id": 1}, cursor_slice={}),
                            [{"id": "1", "cursor_field": 0}],
                        ),
                        (
                            StreamSlice(partition={"parent_id": 309}, cursor_slice={}),
                            [{"id": "3", "cursor_field": 0}],
                        ),
                    ],
                    "stream1",
                    cursor_field="cursor_field",
                    json_schema={
                        "type": "object",
                        "properties": {
                            "id": {"type": ["null", "string"]},
                        },
                    },
                )
            ]
        )
        .set_incremental(CursorField("cursor_field"), _NO_SLICE_BOUNDARIES)
    )
    .set_expected_read_error(AirbyteTracedException, "Concurrent read failure")
    .set_log_levels({"ERROR", "WARN", "WARNING", "INFO", "DEBUG"})
    .set_incremental_scenario_config(
        IncrementalScenarioConfig(
            input_state=[],
        )
    )
    .build()
)
