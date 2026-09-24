"""Tests for scheduler configuration validation."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from nickel_connection.scheduler.models import Schedule


@pytest.fixture
def schedule_data():
    return {
        "tenant_id": UUID("00000000-0000-4000-8000-000000000001"),
        "name": "Demo source",
        "connector_type": "demo",
        "connection_id": "demo-connection",
        "interval_seconds": 30,
        "next_run_at": datetime(2026, 9, 23, 12, 0, tzinfo=UTC),
    }


def test_create_schedule(schedule_data):
    schedule = Schedule(**schedule_data)

    assert isinstance(schedule.id, UUID)
    assert schedule.enabled is True
    assert schedule.interval_seconds == 30
    assert schedule.next_run_at == schedule_data["next_run_at"]


def test_schedule_ids_are_unique(schedule_data):
    first = Schedule(**schedule_data)
    second = Schedule(**schedule_data)

    assert first.id != second.id


@pytest.mark.parametrize("field", ["name", "connector_type", "connection_id"])
def test_strip_whitespace(schedule_data, field):
    schedule_data[field] = "  example  "

    schedule = Schedule(**schedule_data)

    assert getattr(schedule, field) == "example"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", "   "),
        ("name", "x" * 121),
        ("connector_type", ""),
        ("connector_type", "x" * 81),
        ("connection_id", " "),
        ("connection_id", "x" * 201),
        ("tenant_id", "invalid-uuid"),
        ("interval_seconds", 0),
        ("interval_seconds", -1),
        ("interval_seconds", 1.5),
        ("interval_seconds", "30"),
        ("interval_seconds", True),
        ("enabled", "true"),
        ("next_run_at", datetime(2026, 9, 23, 12, 0)),
    ],
)
def test_reject_invalid_configuration(schedule_data, field, value):
    schedule_data[field] = value

    with pytest.raises(ValidationError):
        Schedule(**schedule_data)


def test_reject_unknown_fields(schedule_data):
    schedule_data["unexpected_setting"] = "value"

    with pytest.raises(ValidationError):
        Schedule(**schedule_data)


def test_update_frequency_and_pause(schedule_data):
    schedule = Schedule(**schedule_data)

    schedule.interval_seconds = 60
    schedule.enabled = False

    assert schedule.interval_seconds == 60
    assert schedule.enabled is False


def test_reject_invalid_frequency_update(schedule_data):
    schedule = Schedule(**schedule_data)

    with pytest.raises(ValidationError):
        schedule.interval_seconds = 0

    assert schedule.interval_seconds == 30


def test_json_round_trip_preserves_configuration(schedule_data):
    schedule = Schedule(**schedule_data)

    restored = Schedule.model_validate_json(schedule.model_dump_json())

    assert restored == schedule


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_retries", -1),
        ("max_retries", 6),
        ("max_retries", True),
        ("retry_delay_seconds", 0),
        ("retry_delay_seconds", 3601),
        ("retry_delay_seconds", "5"),
    ],
)
def test_reject_invalid_retry_settings(schedule_data, field, value):
    schedule_data[field] = value

    with pytest.raises(ValidationError):
        Schedule(**schedule_data)


def test_retry_settings_survive_serialization(schedule_data):
    schedule = Schedule(
        **schedule_data,
        max_retries=2,
        retry_delay_seconds=10,
    )

    restored = Schedule.model_validate_json(schedule.model_dump_json())

    assert restored.max_retries == 2
    assert restored.retry_delay_seconds == 10


@pytest.mark.parametrize("value", [0, -1, 3601, True, "30", 1.5])
def test_reject_invalid_timeout(schedule_data, value):
    schedule_data["timeout_seconds"] = value

    with pytest.raises(ValidationError):
        Schedule(**schedule_data)
