"""Paginate SELECT result and serialize to Arrow IPC."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft import DataFrame


@dataclass
class PageMetadata:
    """Metadata for a single page of results."""

    page: int
    page_size: int
    total_count: int
    format: str = "arrow_ipc"


class Paginator:
    """Slice a materialized result by page and serialize to Arrow IPC."""

    def __init__(self, page: int = 1, page_size: int = 1000) -> None:
        self.page = max(1, page)
        self.page_size = max(1, min(page_size, 100_000))

    def get_slice_indices(self, total_count: int) -> tuple[int, int]:
        """Return (start, end) for the current page (0-based)."""
        start = (self.page - 1) * self.page_size
        end = min(start + self.page_size, total_count)
        return start, end

    def paginate_and_serialize(
        self,
        df: DataFrame,
    ) -> tuple[bytes, PageMetadata]:
        """
        Materialize the DataFrame, slice the requested page, serialize to Arrow IPC.
        Returns (arrow_ipc_bytes, metadata).
        """
        pdf = df.to_pandas()
        total_count = len(pdf)
        start, end = self.get_slice_indices(total_count)
        page_df = pdf.iloc[start:end]
        arrow_bytes = serialize_page_to_arrow(page_df)
        metadata = PageMetadata(
            page=self.page,
            page_size=self.page_size,
            total_count=total_count,
        )
        return arrow_bytes, metadata


def serialize_page_to_arrow(pandas_df) -> bytes:
    """Serialize a pandas DataFrame (or Arrow table) to Arrow IPC format bytes."""
    try:
        import pyarrow as pa
    except ImportError as e:
        raise ImportError("pyarrow is required for Arrow IPC output. Install with: pip install pyarrow") from e
    if hasattr(pandas_df, "to_arrow"):
        table = pandas_df.to_arrow()
    else:
        table = pa.Table.from_pandas(pandas_df)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()
