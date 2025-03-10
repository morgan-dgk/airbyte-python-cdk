from typing import Generator, Mapping, Any

from dataclasses import dataclass

import requests

from airbyte_cdk.sources.declarative.decoders import Decoder

logger = logging.getLogger("airbyte")


@dataclass
class TextDecoder(Decoder):
    def is_stream_response(self) -> bool:
        return False

    def decode(  # type: ignore[override]  # Signature doesn't match base class
        self,
        response: requests.Response,
    ) -> Generator[Mapping[str, Any], None, None]:
        """Currently does nothing since requests handles decoding the text content of the response.
        Maybe parse query string responses in future?
        """
        yield from [{}]
