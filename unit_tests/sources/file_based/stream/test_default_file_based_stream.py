#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import traceback
import unittest
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator, Mapping
from unittest import mock
from unittest.mock import Mock

import pytest

from airbyte_cdk.models import (
    AirbyteLogMessage,
    AirbyteMessage,
    AirbyteRecordMessageFileReference,
    AirbyteStream,
    Level,
)
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.sources.file_based import FileBasedStreamConfig
from airbyte_cdk.sources.file_based.availability_strategy import (
    AbstractFileBasedAvailabilityStrategy,
)
from airbyte_cdk.sources.file_based.discovery_policy import AbstractDiscoveryPolicy
from airbyte_cdk.sources.file_based.exceptions import (
    DuplicatedFilesError,
    FileBasedErrorsCollector,
    FileBasedSourceError,
)
from airbyte_cdk.sources.file_based.file_based_stream_reader import AbstractFileBasedStreamReader
from airbyte_cdk.sources.file_based.file_record_data import FileRecordData
from airbyte_cdk.sources.file_based.file_types import FileTransfer
from airbyte_cdk.sources.file_based.file_types.file_type_parser import FileTypeParser
from airbyte_cdk.sources.file_based.remote_file import RemoteFile
from airbyte_cdk.sources.file_based.schema_validation_policies import AbstractSchemaValidationPolicy
from airbyte_cdk.sources.file_based.stream.cursor import AbstractFileBasedCursor
from airbyte_cdk.sources.file_based.stream.default_file_based_stream import DefaultFileBasedStream
from airbyte_cdk.utils.traced_exception import AirbyteTracedException


class MockFormat:
    pass


@pytest.mark.parametrize(
    "input_schema, expected_output",
    [
        pytest.param({}, {}, id="empty-schema"),
        pytest.param(
            {"type": "string"},
            {"type": ["null", "string"]},
            id="simple-schema",
        ),
        pytest.param(
            {"type": ["string"]},
            {"type": ["null", "string"]},
            id="simple-schema-list-type",
        ),
        pytest.param(
            {"type": ["null", "string"]},
            {"type": ["null", "string"]},
            id="simple-schema-already-has-null",
        ),
        pytest.param(
            {"properties": {"type": "string"}},
            {"properties": {"type": ["null", "string"]}},
            id="nested-schema",
        ),
        pytest.param(
            {"items": {"type": "string"}},
            {"items": {"type": ["null", "string"]}},
            id="array-schema",
        ),
        pytest.param(
            {"type": "object", "properties": {"prop": {"type": "string"}}},
            {
                "type": ["null", "object"],
                "properties": {"prop": {"type": ["null", "string"]}},
            },
            id="deeply-nested-schema",
        ),
    ],
)
def test_fill_nulls(input_schema: Mapping[str, Any], expected_output: Mapping[str, Any]) -> None:
    assert DefaultFileBasedStream._fill_nulls(input_schema) == expected_output


class DefaultFileBasedStreamTest(unittest.TestCase):
    _NOW = datetime(2022, 10, 22, tzinfo=timezone.utc)
    _A_RECORD = {"a_record": 1}

    def setUp(self) -> None:
        self._stream_config = Mock()
        self._stream_config.format = MockFormat()
        self._stream_config.name = "a stream name"
        self._catalog_schema = Mock()
        self._stream_reader = Mock(spec=AbstractFileBasedStreamReader)
        self._availability_strategy = Mock(spec=AbstractFileBasedAvailabilityStrategy)
        self._discovery_policy = Mock(spec=AbstractDiscoveryPolicy)
        self._parser = Mock(spec=FileTypeParser)
        self._validation_policy = Mock(spec=AbstractSchemaValidationPolicy)
        self._validation_policy.name = "validation policy name"
        self._cursor = Mock(spec=AbstractFileBasedCursor)

        self._stream = DefaultFileBasedStream(
            config=self._stream_config,
            catalog_schema=self._catalog_schema,
            stream_reader=self._stream_reader,
            availability_strategy=self._availability_strategy,
            discovery_policy=self._discovery_policy,
            parsers={MockFormat: self._parser},
            validation_policy=self._validation_policy,
            cursor=self._cursor,
            errors_collector=FileBasedErrorsCollector(),
        )

    def test_when_read_records_from_slice_then_return_records(self) -> None:
        self._parser.parse_records.return_value = [self._A_RECORD]
        messages = list(
            self._stream.read_records_from_slice(
                {"files": [RemoteFile(uri="uri", last_modified=self._NOW)]}
            )
        )
        assert list(map(lambda message: message.record.data["data"], messages)) == [self._A_RECORD]

    def test_when_transform_record_then_return_updated_record(self) -> None:
        file = RemoteFile(uri="uri", last_modified=self._NOW)
        last_updated = self._NOW.isoformat()
        transformed_record = self._stream.transform_record(self._A_RECORD, file, last_updated)
        assert transformed_record[self._stream.ab_last_mod_col] == last_updated
        assert transformed_record[self._stream.ab_file_name_col] == file.uri

    def test_given_exception_when_read_records_from_slice_then_do_process_other_files(
        self,
    ) -> None:
        """
        The current behavior for source-s3 v3 does not fail sync on some errors and hence, we will keep this behaviour for now. One example
        we can easily reproduce this is by having a file with gzip extension that is not actually a gzip file. The reader will fail to open
        the file but the sync won't fail.
        Ticket: https://github.com/airbytehq/airbyte/issues/29680
        """
        self._parser.parse_records.side_effect = [
            ValueError("An error"),
            [self._A_RECORD],
        ]

        messages = list(
            self._stream.read_records_from_slice(
                {
                    "files": [
                        RemoteFile(uri="invalid_file", last_modified=self._NOW),
                        RemoteFile(uri="valid_file", last_modified=self._NOW),
                    ]
                }
            )
        )

        assert messages[0].log.level == Level.ERROR
        assert messages[1].record.data["data"] == self._A_RECORD

    def test_given_traced_exception_when_read_records_from_slice_then_fail(
        self,
    ) -> None:
        """
        When a traced exception is raised, the stream shouldn't try to handle but pass it on to the caller.
        """
        self._parser.parse_records.side_effect = [AirbyteTracedException("An error")]

        with pytest.raises(AirbyteTracedException):
            list(
                self._stream.read_records_from_slice(
                    {
                        "files": [
                            RemoteFile(uri="invalid_file", last_modified=self._NOW),
                            RemoteFile(uri="valid_file", last_modified=self._NOW),
                        ]
                    }
                )
            )

    def test_given_exception_after_skipping_records_when_read_records_from_slice_then_send_warning(
        self,
    ) -> None:
        self._stream_config.schemaless = False
        self._validation_policy.record_passes_validation_policy.return_value = False
        self._parser.parse_records.side_effect = [
            self._iter([self._A_RECORD, ValueError("An error")])
        ]

        messages = list(
            self._stream.read_records_from_slice(
                {
                    "files": [
                        RemoteFile(uri="invalid_file", last_modified=self._NOW),
                        RemoteFile(uri="valid_file", last_modified=self._NOW),
                    ]
                }
            )
        )

        assert messages[0].log.level == Level.ERROR
        assert messages[1].log.level == Level.WARN

    def test_override_max_n_files_for_schema_inference_is_respected(self) -> None:
        self._discovery_policy.n_concurrent_requests = 1
        self._discovery_policy.get_max_n_files_for_schema_inference.return_value = 3
        self._stream.config.input_schema = None
        self._stream.config.schemaless = None
        self._parser.infer_schema.return_value = {"data": {"type": "string"}}
        files = [RemoteFile(uri=f"file{i}", last_modified=self._NOW) for i in range(10)]
        self._stream_reader.get_matching_files.return_value = files
        self._stream.config.recent_n_files_to_read_for_schema_discovery = 3

        schema = self._stream.get_json_schema()

        assert schema == {
            "type": "object",
            "properties": {
                "_ab_source_file_last_modified": {"type": "string"},
                "_ab_source_file_url": {"type": "string"},
                "data": {"type": ["null", "string"]},
            },
        }
        assert self._parser.infer_schema.call_count == 3

    def _iter(self, x: Iterable[Any]) -> Iterator[Any]:
        for item in x:
            if isinstance(item, Exception):
                raise item
            yield item


class TestFileBasedErrorCollector:
    test_error_collector: FileBasedErrorsCollector = FileBasedErrorsCollector()

    @pytest.mark.parametrize(
        "stream, file, line_no, n_skipped, collector_expected_len",
        (
            ("stream_1", "test.csv", 1, 1, 1),
            ("stream_2", "test2.csv", 2, 2, 2),
        ),
        ids=[
            "Single error",
            "Multiple errors",
        ],
    )
    def test_collect_parsing_error(
        self, stream, file, line_no, n_skipped, collector_expected_len
    ) -> None:
        test_error_pattern = "Error parsing record."
        # format the error body
        test_error = (
            AirbyteMessage(
                type=MessageType.LOG,
                log=AirbyteLogMessage(
                    level=Level.ERROR,
                    message=f"{FileBasedSourceError.ERROR_PARSING_RECORD.value} stream={stream} file={file} line_no={line_no} n_skipped={n_skipped}",
                    stack_trace=traceback.format_exc(),
                ),
            ),
        )
        # collecting the error
        self.test_error_collector.collect(test_error)
        # check the error has been collected
        assert len(self.test_error_collector.errors) == collector_expected_len
        # check for the patern presence for the collected errors
        for error in self.test_error_collector.errors:
            assert test_error_pattern in error[0].log.message

    def test_yield_and_raise_collected(self) -> None:
        # we expect the following method will raise the AirbyteTracedException
        with pytest.raises(AirbyteTracedException) as parse_error:
            list(self.test_error_collector.yield_and_raise_collected())
        assert parse_error.value.message == "Some errors occured while reading from the source."
        assert (
            parse_error.value.internal_message
            == "Please check the logged errors for more information."
        )


class DefaultFileBasedStreamFileTransferTest(unittest.TestCase):
    _NOW = datetime(2022, 10, 22, tzinfo=timezone.utc)
    _A_FILE_RECORD_DATA = FileRecordData(
        folder="/absolute/path/",
        file_name="file.csv",
        bytes=10,
        source_uri="file:///absolute/path/file.csv",
    )
    _A_FILE_REFERENCE_MESSAGE = AirbyteRecordMessageFileReference(
        file_size_bytes=10,
        source_file_relative_path="relative/path/file.csv",
        staging_file_url="/absolute/path/file.csv",
    )

    def setUp(self) -> None:
        self._stream_config = Mock()
        self._stream_config.format = MockFormat()
        self._stream_config.name = "a stream name"
        self._catalog_schema = Mock()
        self._stream_reader = Mock(spec=AbstractFileBasedStreamReader)
        self._availability_strategy = Mock(spec=AbstractFileBasedAvailabilityStrategy)
        self._discovery_policy = Mock(spec=AbstractDiscoveryPolicy)
        self._parser = Mock(spec=FileTypeParser)
        self._validation_policy = Mock(spec=AbstractSchemaValidationPolicy)
        self._validation_policy.name = "validation policy name"
        self._cursor = Mock(spec=AbstractFileBasedCursor)

        self._stream = DefaultFileBasedStream(
            config=self._stream_config,
            catalog_schema=self._catalog_schema,
            stream_reader=self._stream_reader,
            availability_strategy=self._availability_strategy,
            discovery_policy=self._discovery_policy,
            parsers={MockFormat: self._parser},
            validation_policy=self._validation_policy,
            cursor=self._cursor,
            errors_collector=FileBasedErrorsCollector(),
            use_file_transfer=True,
        )

    def test_when_read_records_from_slice_then_return_records(self) -> None:
        """Verify that we have the new file method and data is empty"""
        with mock.patch.object(
            FileTransfer,
            "upload",
            return_value=[(self._A_FILE_RECORD_DATA, self._A_FILE_REFERENCE_MESSAGE)],
        ):
            remote_file = RemoteFile(uri="uri", last_modified=self._NOW)
            messages = list(self._stream.read_records_from_slice({"files": [remote_file]}))

            assert list(map(lambda message: message.record.file_reference, messages)) == [
                self._A_FILE_REFERENCE_MESSAGE
            ]
            assert list(map(lambda message: message.record.data, messages)) == [
                {
                    "bytes": 10,
                    "file_name": "file.csv",
                    "folder": "/absolute/path/",
                    "source_uri": "file:///absolute/path/file.csv",
                }
            ]

    def test_when_compute_slices(self) -> None:
        all_files = [
            RemoteFile(
                uri="mirror_paths_testing/not_duplicates/data/jan/monthly-kickoff-202402.mpeg",
                last_modified=datetime(2025, 1, 9, 11, 27, 20),
                mime_type=None,
            ),
            RemoteFile(
                uri="mirror_paths_testing/not_duplicates/data/feb/monthly-kickoff-202401.mpeg",
                last_modified=datetime(2025, 1, 9, 11, 27, 20),
                mime_type=None,
            ),
            RemoteFile(
                uri="mirror_paths_testing/not_duplicates/data/mar/monthly-kickoff-202403.mpeg",
                last_modified=datetime(2025, 1, 9, 11, 27, 20),
                mime_type=None,
            ),
        ]
        with (
            mock.patch.object(DefaultFileBasedStream, "list_files", return_value=all_files),
            mock.patch.object(self._stream._cursor, "get_files_to_sync", return_value=all_files),
        ):
            returned_slices = self._stream.compute_slices()
        assert returned_slices == [
            {"files": sorted(all_files, key=lambda f: (f.last_modified, f.uri))}
        ]


class DefaultFileBasedStreamFileTransferTestNotMirroringDirectories(unittest.TestCase):
    _NOW = datetime(2022, 10, 22, tzinfo=timezone.utc)

    def setUp(self) -> None:
        self._stream_config = Mock()
        self._stream_config.format = MockFormat()
        self._stream_config.name = "a stream name"
        self._catalog_schema = Mock()
        self._stream_reader = Mock(spec=AbstractFileBasedStreamReader)
        self._availability_strategy = Mock(spec=AbstractFileBasedAvailabilityStrategy)
        self._discovery_policy = Mock(spec=AbstractDiscoveryPolicy)
        self._parser = Mock(spec=FileTypeParser)
        self._validation_policy = Mock(spec=AbstractSchemaValidationPolicy)
        self._validation_policy.name = "validation policy name"
        self._cursor = Mock(spec=AbstractFileBasedCursor)

        self._stream = DefaultFileBasedStream(
            config=self._stream_config,
            catalog_schema=self._catalog_schema,
            stream_reader=self._stream_reader,
            availability_strategy=self._availability_strategy,
            discovery_policy=self._discovery_policy,
            parsers={MockFormat: self._parser},
            validation_policy=self._validation_policy,
            cursor=self._cursor,
            errors_collector=FileBasedErrorsCollector(),
            use_file_transfer=True,
            preserve_directory_structure=False,
        )

        self._all_files = [
            RemoteFile(
                uri="mirror_paths_testing/not_duplicates/data/jan/monthly-kickoff-202402.mpeg",
                last_modified=datetime(2025, 1, 9, 11, 27, 20),
                mime_type=None,
            ),
            RemoteFile(
                uri="mirror_paths_testing/not_duplicates/data/feb/monthly-kickoff-202401.mpeg",
                last_modified=datetime(2025, 1, 9, 11, 27, 20),
                mime_type=None,
            ),
            RemoteFile(
                uri="mirror_paths_testing/not_duplicates/data/mar/monthly-kickoff-202403.mpeg",
                last_modified=datetime(2025, 1, 9, 11, 27, 20),
                mime_type=None,
            ),
        ]

    def test_when_compute_slices_with_not_duplicates(self) -> None:
        with (
            mock.patch.object(DefaultFileBasedStream, "list_files", return_value=self._all_files),
            mock.patch.object(
                self._stream._cursor, "get_files_to_sync", return_value=self._all_files
            ),
        ):
            returned_slices = self._stream.compute_slices()
        assert returned_slices == [
            {"files": sorted(self._all_files, key=lambda f: (f.last_modified, f.uri))}
        ]

    def test_when_compute_slices_with_duplicates(self) -> None:
        all_files = deepcopy(self._all_files)
        all_files.append(
            RemoteFile(
                uri="mirror_paths_testing/not_duplicates/data/apr/monthly-kickoff-202402.mpeg",
                last_modified=datetime(2025, 1, 9, 11, 27, 20),
                mime_type=None,
            )
        )
        all_files.append(
            RemoteFile(
                uri="mirror_paths_testing/not_duplicates/data/may/monthly-kickoff-202401.mpeg",
                last_modified=datetime(2025, 1, 9, 11, 27, 20),
                mime_type=None,
            )
        )
        all_files.append(
            RemoteFile(
                uri="mirror_paths_testing/not_duplicates/data/jun/monthly-kickoff-202403.mpeg",
                last_modified=datetime(2025, 1, 9, 11, 27, 20),
                mime_type=None,
            )
        )
        all_files.append(
            RemoteFile(
                uri="mirror_paths_testing/not_duplicates/data/jul/monthly-kickoff-202403.mpeg",
                last_modified=datetime(2025, 1, 9, 11, 27, 20),
                mime_type=None,
            )
        )
        with (
            mock.patch.object(DefaultFileBasedStream, "list_files", return_value=all_files),
            mock.patch.object(self._stream._cursor, "get_files_to_sync", return_value=all_files),
        ):
            with pytest.raises(DuplicatedFilesError) as exc_info:
                self._stream.compute_slices()
        assert "Duplicate filenames found for stream" in str(exc_info.value)
        assert "2 duplicates found for file name monthly-kickoff-202402.mpeg" in str(exc_info.value)
        assert "2 duplicates found for file name monthly-kickoff-202401.mpeg" in str(exc_info.value)
        assert "3 duplicates found for file name monthly-kickoff-202403.mpeg" in str(exc_info.value)


class DefaultFileBasedStreamSchemaTest(unittest.TestCase):
    _NOW = datetime(2022, 10, 22, tzinfo=timezone.utc)
    _A_FILE_REFERENCE_MESSAGE = AirbyteRecordMessageFileReference(
        file_size_bytes=10,
        source_file_relative_path="relative/path/file.csv",
        staging_file_url="/absolute/path/file.csv",
    )

    def setUp(self) -> None:
        self._stream_config = Mock(spec=FileBasedStreamConfig)
        self._stream_config.format = MockFormat()
        self._stream_config.name = "a stream name"
        self._stream_config.input_schema = ""
        self._stream_config.schemaless = False
        self._stream_config.primary_key = []
        self._catalog_schema = Mock()
        self._stream_reader = Mock(spec=AbstractFileBasedStreamReader)
        self._availability_strategy = Mock(spec=AbstractFileBasedAvailabilityStrategy)
        self._discovery_policy = Mock(spec=AbstractDiscoveryPolicy)
        self._parser = Mock(spec=FileTypeParser)
        self._validation_policy = Mock(spec=AbstractSchemaValidationPolicy)
        self._validation_policy.name = "validation policy name"
        self._cursor = Mock(spec=AbstractFileBasedCursor)

    def test_non_file_based_stream(self) -> None:
        """
        Test that the stream is correct when file transfer is not used.
        """
        non_file_based_stream = DefaultFileBasedStream(
            config=self._stream_config,
            catalog_schema=self._catalog_schema,
            stream_reader=self._stream_reader,
            availability_strategy=self._availability_strategy,
            discovery_policy=self._discovery_policy,
            parsers={MockFormat: self._parser},
            validation_policy=self._validation_policy,
            cursor=self._cursor,
            errors_collector=FileBasedErrorsCollector(),
            use_file_transfer=False,
        )
        with (
            mock.patch.object(non_file_based_stream, "get_json_schema", return_value={}),
            mock.patch.object(
                DefaultFileBasedStream,
                "primary_key",
                new_callable=mock.PropertyMock,
                return_value=["id"],
            ),
        ):
            airbyte_stream = non_file_based_stream.as_airbyte_stream()
            assert isinstance(airbyte_stream, AirbyteStream)
            assert not airbyte_stream.is_file_based

    def test_file_based_stream(self) -> None:
        """
        Test that the stream is correct when file transfer used.
        """
        non_file_based_stream = DefaultFileBasedStream(
            config=self._stream_config,
            catalog_schema=self._catalog_schema,
            stream_reader=self._stream_reader,
            availability_strategy=self._availability_strategy,
            discovery_policy=self._discovery_policy,
            parsers={MockFormat: self._parser},
            validation_policy=self._validation_policy,
            cursor=self._cursor,
            errors_collector=FileBasedErrorsCollector(),
            use_file_transfer=True,
        )
        with (
            mock.patch.object(non_file_based_stream, "get_json_schema", return_value={}),
            mock.patch.object(
                DefaultFileBasedStream,
                "primary_key",
                new_callable=mock.PropertyMock,
                return_value=["id"],
            ),
        ):
            airbyte_stream = non_file_based_stream.as_airbyte_stream()
            assert isinstance(airbyte_stream, AirbyteStream)
            assert airbyte_stream.is_file_based
