"""Source connectors: one sub-package per kind of source. Each reads its manifest and hands raw items to
the ingest bridge (``src.handler.ingest``); it never reads or judges their content.

Here: document sources such as PDFs (``documents``, NIK-2209-05). Planned: Nango-managed sources
(NIK-2209-01).
"""
