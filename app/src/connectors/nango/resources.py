"""Bounded JSON requests through Nango; pagination never changes the provider's host."""

import json
from urllib.parse import parse_qsl, urlsplit

import httpx

from src.error import UpstreamError
from src.models.plugins import PluginResource


def value_at(body, path):
    value = body
    for part in path.split(".") if path else []:
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def fetch_resource(
    client,
    integration,
    connection_id,
    resource: PluginResource,
    *,
    max_bytes,
    base_url=None,
):
    limit = min(max_bytes, 20 * 1024 * 1024)
    query = dict(resource.query)
    paging = resource.pagination
    if paging.mode == "offset":
        query[paging.limit_param] = str(paging.page_size)
        query[paging.offset_param] = "0"
    pages = []
    seen = set()
    total = 0
    for page in range(paging.max_pages):
        signature = json.dumps(query, sort_keys=True)
        if signature in seen:
            raise UpstreamError("The API repeated a pagination cursor")
        seen.add(signature)
        chunks = []
        headers = {
            "Connection-Id": connection_id,
            "Provider-Config-Key": integration,
            "Accept": "application/json",
        }
        if base_url:
            headers["Base-Url-Override"] = base_url
        try:
            with client._http.stream(
                resource.method,
                f"/proxy{resource.path}",
                params=query,
                json=resource.body,
                headers=headers,
            ) as response:
                if not 200 <= response.status_code < 300:
                    raise UpstreamError(
                        f"The API request through Nango returned {response.status_code}"
                    )
                for chunk in response.iter_bytes():
                    total += len(chunk)
                    if total > limit:
                        raise UpstreamError(
                            "The API response exceeded the configured size limit"
                        )
                    chunks.append(chunk)
        except httpx.HTTPError:
            raise UpstreamError(
                "Nango could not complete the resource request"
            ) from None
        raw = b"".join(chunks) or b"[]"
        try:
            data = json.loads(raw)
        except (ValueError, UnicodeError):
            raise UpstreamError("The API did not return valid JSON") from None
        pages.append(raw)
        if paging.mode == "none":
            break
        if paging.mode == "fhir":
            links = data.get("link", []) if isinstance(data, dict) else None
            if not isinstance(links, list):
                raise UpstreamError("Expected a FHIR Bundle with a link array")
            following = next(
                (
                    link.get("url")
                    for link in links
                    if isinstance(link, dict) and link.get("relation") == "next"
                ),
                None,
            )
            if not following:
                break
            if not isinstance(following, str):
                raise UpstreamError("Invalid FHIR next-page link")
            parsed = urlsplit(following)
            # Take only the query. Never forward the link's host or change the resource path.
            if (
                parsed.scheme not in {"", "http", "https"}
                or parsed.fragment
                or parsed.path
                and not parsed.path.endswith(resource.path)
                or not parsed.query
            ):
                raise UpstreamError("FHIR pagination changed the resource path")
            query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        elif paging.mode == "cursor":
            cursor = value_at(data, paging.cursor_path)
            if cursor is None or cursor == "":
                break
            if not isinstance(cursor, (str, int)) or isinstance(cursor, bool):
                raise UpstreamError("Invalid pagination cursor")
            query[paging.cursor_param] = str(cursor)
        else:
            items = value_at(data, paging.items_path)
            if not isinstance(items, list):
                raise UpstreamError(
                    "The pagination items path did not contain an array"
                )
            if len(items) < paging.page_size:
                break
            query[paging.offset_param] = str((page + 1) * paging.page_size)
    else:
        raise UpstreamError(
            "Pagination exceeded max_pages; increase the limit or narrow the query"
        )
    # Preserve each page's JSON bytes. The wrapper makes all pages viewable as one stream payload.
    return pages[0] if len(pages) == 1 else b'{"pages":[' + b",".join(pages) + b"]}"
