"""Reading a pipeline's logs stream from a cursor."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generator, Mapping

import requests

from feldera.rest.errors import FelderaError

EPOCH_HEADER = "feldera-logs-epoch"
SEQ_HEADER = "feldera-logs-seq"
GAP_HEADER = "feldera-logs-gap"

# One log line can be long, so read the body in large chunks.
_CHUNK_SIZE = 50000000


@dataclass(frozen=True)
class LogPosition:
    """
    Where a logs stream starts, as reported by the response that opens it.
    """

    epoch: str
    """
    Identifies the lifetime of the pipeline's logs buffer, which is what gives the
    sequence number meaning: the buffer lives in memory, so a runner restart resets
    numbering to zero while a reader still holds a cursor issued before it.

    The buffer outlives a pipeline run: stopping and starting a pipeline keeps the epoch
    and continues the numbering, so only a runner restart or a deleted pipeline ends it.
    """

    seq: int
    """Sequence number of the line preceding the stream's first line."""

    gap: int
    """
    Lines discarded between the requested cursor and `seq`, and which this stream will
    therefore never deliver. Zero means the resume is exact.
    """

    def cursor(self, lines_read: int = 0) -> str:
        """
        The cursor that resumes this stream after `lines_read` of its lines.

        The body holds log lines and nothing else, one per sequence number, so a reader
        derives its position by counting rather than by inspecting what it receives.
        """
        return f"{self.epoch}:{self.seq + lines_read}"

    @staticmethod
    def from_headers(headers: Mapping[str, str]) -> LogPosition:
        """
        The position a logs response reports.

        All three headers are required. A response carrying only some of them could not
        be turned into a cursor, so it is an error rather than something to interpret.
        """
        missing = [
            header
            for header in (EPOCH_HEADER, SEQ_HEADER, GAP_HEADER)
            if header not in headers
        ]
        if missing:
            raise FelderaError(
                f"logs response is missing the position headers {missing}; "
                "the Feldera instance may predate the logs cursor protocol"
            )
        return LogPosition(
            epoch=headers[EPOCH_HEADER],
            seq=int(headers[SEQ_HEADER]),
            gap=int(headers[GAP_HEADER]),
        )


class LogStream:
    """
    An open logs stream: where it starts, the lines that follow, and how far they reach.

    Iterating yields one log line at a time, blocking for the next one until the pipeline
    is deleted or the connection drops. Close the stream when done reading, or use it as a
    context manager, which closes it on the way out.

    Pass :meth:`cursor` to the next connection to carry on from the line this one reached.
    """

    def __init__(self, response: requests.Response, position: LogPosition) -> None:
        self._response = response
        self.position = position
        self._lines_read = 0

    @property
    def lines_read(self) -> int:
        """Log lines this stream has yielded."""
        return self._lines_read

    def cursor(self) -> str:
        """
        The cursor that resumes the log after the last line this stream yielded.

        The stream counts the lines it delivers, so a reader resumes by handing this back
        rather than by keeping a count of its own.
        """
        return self.position.cursor(self._lines_read)

    def __iter__(self) -> Generator[str, None, None]:
        """
        Yields one line per newline in the body, which is one line per sequence number.

        The count has to match the server's exactly, so the split is on `\n` alone and
        empty lines count. `requests.iter_lines` suits neither: it drops empty lines and
        splits on `\r`, `\v` and other separators a log line is free to contain.

        A trailing fragment with no newline is a line the connection cut short. Dropping it
        leaves the cursor pointing at the line before, which the next connection redelivers
        whole.
        """
        pending = b""
        for chunk in self._response.iter_content(chunk_size=_CHUNK_SIZE):
            pending += chunk
            start = 0
            while (end := pending.find(b"\n", start)) != -1:
                line = pending[start:end].decode("utf-8")
                start = end + 1
                # Counted before the yield, which is where a reader that stops early leaves
                # the generator suspended for good.
                self._lines_read += 1
                yield line
            pending = pending[start:]

    def close(self) -> None:
        self._response.close()

    def __enter__(self) -> LogStream:
        return self

    def __exit__(self, *exception) -> None:
        self.close()
