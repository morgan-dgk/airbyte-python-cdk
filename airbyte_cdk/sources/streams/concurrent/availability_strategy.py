#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from abc import ABC, abstractmethod
from typing import Optional

from typing_extensions import deprecated

from airbyte_cdk.sources.source import ExperimentalClassWarning


class StreamAvailability(ABC):
    @abstractmethod
    def is_available(self) -> bool:
        """
        :return: True if the stream is available. False if the stream is not
        """

    @abstractmethod
    def message(self) -> Optional[str]:
        """
        :return: A message describing why the stream is not available. If the stream is available, this should return None.
        """


class StreamAvailable(StreamAvailability):
    def is_available(self) -> bool:
        return True

    def message(self) -> Optional[str]:
        return None


class StreamUnavailable(StreamAvailability):
    def __init__(self, message: str):
        self._message = message

    def is_available(self) -> bool:
        return False

    def message(self) -> Optional[str]:
        return self._message


# Singleton instances of StreamAvailability to avoid the overhead of creating new dummy objects
STREAM_AVAILABLE = StreamAvailable()


@deprecated(
    "This class is experimental. Use at your own risk.",
    category=ExperimentalClassWarning,
)
class AbstractAvailabilityStrategy(ABC):
    """
    AbstractAvailabilityStrategy is an experimental interface developed as part of the Concurrent CDK.
    This interface is not yet stable and may change in the future. Use at your own risk.

    Why create a new interface instead of using the existing AvailabilityStrategy?
    The existing AvailabilityStrategy is tightly coupled with Stream and Source, which yields to circular dependencies and makes it difficult to move away from the Stream interface to AbstractStream.
    """

    @abstractmethod
    def check_availability(self, logger: logging.Logger) -> StreamAvailability:
        """
        Checks stream availability.

        :param logger: logger object to use
        :return: A StreamAvailability object describing the stream's availability
        """


@deprecated(
    "This class is experimental. Use at your own risk.",
    category=ExperimentalClassWarning,
)
class AlwaysAvailableAvailabilityStrategy(AbstractAvailabilityStrategy):
    """
    An availability strategy that always indicates a stream is available.

    This strategy is used to avoid breaking changes and serves as a soft
    deprecation of the availability strategy, allowing a smoother transition
    without disrupting existing functionality.
    """

    def check_availability(self, logger: logging.Logger) -> StreamAvailability:
        """
        Checks stream availability.

        :param logger: logger object to use
        :return: A StreamAvailability object describing the stream's availability
        """
        return StreamAvailable()
