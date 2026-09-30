"""Prepare or register the envelope using a Confluent-compatible Schema Registry API.

Default: write the registration JSON to stdout, without making network calls.
--register: require NICKEL_SCHEMA_REGISTRY_URL and NICKEL_SCHEMA_REGISTRY_SUBJECT.
Optional basic auth is read from NICKEL_SCHEMA_REGISTRY_USERNAME/PASSWORD.
"""

import argparse
import json
import os
from urllib.parse import quote, urlsplit

import httpx

from src.contracts import envelope_schema, schema, validator


def payload():
    contract = schema("envelope")
    validator(contract)
    if contract != envelope_schema():
        raise ValueError("Envelope artifact is stale; regenerate and review it")
    return {"schemaType": "JSON", "schema": json.dumps(contract)}


def register(client, subject, body):
    path = "/subjects/" + quote(subject, safe="")
    existing = client.get(path + "/versions/latest")
    if existing.status_code == 200:
        compatible = client.post(
            "/compatibility" + path + "/versions/latest", json=body
        )
        compatible.raise_for_status()
        if compatible.json().get("is_compatible") is not True:
            raise ValueError("Registry rejected envelope compatibility")
    elif existing.status_code == 404 and existing.json().get("error_code") == 40401:
        pass  # The subject has never been registered.
    else:
        existing.raise_for_status()
        raise ValueError("Unexpected registry response")
    response = client.post(path + "/versions", json=body)
    response.raise_for_status()
    registered = response.json()
    if not isinstance(registered.get("id"), int):
        raise ValueError("Registry returned no schema ID")
    # Verify the exact schema was registered, not just that the server returned success.
    found = client.post(path, json=body)
    found.raise_for_status()
    result = found.json()
    if result.get("id") != registered["id"] or json.loads(
        result.get("schema", "null")
    ) != json.loads(body["schema"]):
        raise ValueError("Registry read-back did not match the envelope")
    return {"subject": subject, "id": result["id"], "version": result["version"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--register", action="store_true")
    args = parser.parse_args()
    try:
        body = payload()
        if not args.register:
            print(json.dumps(body, indent=2))
            return
        url = os.environ.get("NICKEL_SCHEMA_REGISTRY_URL", "")
        subject = os.environ.get("NICKEL_SCHEMA_REGISTRY_SUBJECT", "")
        parsed = urlsplit(url)
        local = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        if (
            not subject
            or parsed.username
            or parsed.password
            or not parsed.hostname
            or (parsed.scheme != "https" and not (local and parsed.scheme == "http"))
        ):
            raise ValueError(
                "Set the registry URL (HTTPS, or loopback HTTP) and subject"
            )
        user = os.environ.get("NICKEL_SCHEMA_REGISTRY_USERNAME")
        password = os.environ.get("NICKEL_SCHEMA_REGISTRY_PASSWORD")
        if bool(user) != bool(password):
            raise ValueError("Set both registry username and password")
        with httpx.Client(
            base_url=url,
            timeout=30,
            trust_env=False,
            follow_redirects=False,
            auth=(user, password) if user else None,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        ) as client:
            print(json.dumps(register(client, subject, body)))
    except (ValueError, httpx.HTTPError) as exc:
        # HTTP errors may contain request URLs or response details; do not echo credentials.
        parser.exit(
            1,
            f"Envelope registration failed ({type(exc).__name__}); check registry configuration and compatibility.\n",
        )


if __name__ == "__main__":
    main()
