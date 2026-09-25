"""Tests for scheduler configuration validation."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from src.scheduler.models import Schedule, ScheduleSettings, ScheduleUpdate


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


@pytest.fixture
def settings_data():
    return {
        "name": "Source refresh",
        "connector_type": "demo",
        "connection_id": "demo-connection",
        "interval_seconds": 300,
    }


@pytest.mark.parametrize("expression", ["0 9 * * *", "*/15 * * * *", "0 9 * * mon-fri"])
def test_cron_settings_accept_valid_expressions(settings_data, expression):
    settings_data.pop("interval_seconds")
    settings = ScheduleSettings(
        **settings_data, trigger_type="cron", cron_expression=expression
    )

    assert settings.interval_seconds is None
    assert settings.cron_expression == expression
    assert settings.timezone == "UTC"
    assert ScheduleSettings.model_validate_json(settings.model_dump_json()) == settings


@pytest.mark.parametrize(
    "expression", ["", "   ", "bad cron", "60 * * * *", "* * * * * *", "x" * 101]
)
def test_cron_settings_reject_invalid_expressions(settings_data, expression):
    settings_data.pop("interval_seconds")

    with pytest.raises(ValidationError):
        ScheduleSettings(
            **settings_data, trigger_type="cron", cron_expression=expression
        )


@pytest.mark.parametrize(
    "trigger_settings",
    [
        {},
        {"trigger_type": "interval", "interval_seconds": None},
        {"interval_seconds": 30, "cron_expression": "0 9 * * *"},
        {"trigger_type": "cron"},
        {
            "trigger_type": "cron",
            "cron_expression": "0 9 * * *",
            "interval_seconds": 30,
        },
        {"trigger_type": "unknown", "interval_seconds": 30},
    ],
)
def test_reject_missing_or_conflicting_trigger_settings(
    settings_data, trigger_settings
):
    settings_data.pop("interval_seconds")

    with pytest.raises(ValidationError):
        ScheduleSettings(**settings_data, **trigger_settings)


def test_interval_settings_accept_explicit_null_cron(settings_data):
    settings = ScheduleSettings(**settings_data, cron_expression=None)

    assert settings.trigger_type == "interval"
    assert settings.interval_seconds == 300
    assert settings.cron_expression is None


def test_reject_excessive_interval(settings_data):
    settings_data["interval_seconds"] = 31_536_001

    with pytest.raises(ValidationError):
        ScheduleSettings(**settings_data)


def test_reject_non_utc_timezone(settings_data):
    with pytest.raises(ValidationError):
        ScheduleSettings(**settings_data, timezone="Europe/London")


@pytest.mark.parametrize("payload", [None, [], "invalid"])
def test_reject_non_object_settings(payload):
    with pytest.raises(ValidationError):
        ScheduleSettings.model_validate(payload)


def test_rejected_trigger_change_preserves_existing_settings(settings_data):
    settings = ScheduleSettings(**settings_data)

    with pytest.raises(ValidationError):
        settings.trigger_type = "cron"

    assert settings.trigger_type == "interval"
    assert settings.interval_seconds == 300


def test_update_requires_a_version(settings_data):
    with pytest.raises(ValidationError):
        ScheduleUpdate(**settings_data)


@pytest.mark.parametrize("version", [0, -1, True, "1", 1.5, None])
def test_update_rejects_invalid_versions(settings_data, version):
    with pytest.raises(ValidationError):
        ScheduleUpdate(**settings_data, version=version)


def test_update_preserves_expected_version(settings_data):
    update = ScheduleUpdate(**settings_data, version=3)

    assert update.version == 3
    assert ScheduleUpdate.model_validate_json(update.model_dump_json()) == update


@pytest.mark.parametrize("model", [ScheduleSettings, ScheduleUpdate])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tenant_id", "00000000-0000-4000-8000-000000000001"),
        ("changed_by", "another-admin"),
        ("created_at", "2026-09-23T12:00:00Z"),
        ("updated_at", "2026-09-23T12:00:00Z"),
        ("next_run_at", "2026-09-23T12:00:00Z"),
    ],
)
def test_client_settings_reject_server_owned_fields(settings_data, model, field, value):
    if model is ScheduleUpdate:
        settings_data["version"] = 1
    settings_data[field] = value

    with pytest.raises(ValidationError) as error:
        model(**settings_data)

    assert any(
        item["loc"] == (field,) and item["type"] == "extra_forbidden"
        for item in error.value.errors()
    )


def test_server_audit_metadata_survives_serialization(schedule_data):
    created = datetime(2026, 9, 23, 10, 0, tzinfo=UTC)
    updated = datetime(2026, 9, 23, 11, 0, tzinfo=UTC)
    schedule = Schedule(
        **schedule_data,
        created_at=created,
        updated_at=updated,
        changed_by="admin-subject",
        version=2,
    )

    restored = Schedule.model_validate_json(schedule.model_dump_json())

    assert restored.created_at == created
    assert restored.updated_at == updated
    assert restored.changed_by == "admin-subject"
    assert restored.version == 2


@pytest.mark.parametrize("field", ["created_at", "updated_at"])
def test_audit_timestamps_require_timezone(schedule_data, field):
    schedule_data[field] = datetime(2026, 9, 23, 12, 0)

    with pytest.raises(ValidationError):
        Schedule(**schedule_data)


@pytest.mark.parametrize("actor", ["", "   ", "x" * 256])
def test_reject_invalid_recorded_actor(schedule_data, actor):
    with pytest.raises(ValidationError):
        Schedule(**schedule_data, changed_by=actor)


def test_legacy_schedule_has_unknown_actor_and_aware_timestamps(schedule_data):
    schedule = Schedule(**schedule_data)

    assert schedule.changed_by is None
    assert schedule.version == 1
    assert schedule.created_at.utcoffset().total_seconds() == 0
    assert schedule.updated_at.utcoffset().total_seconds() == 0


def test_paused_schedule_can_have_no_next_run(schedule_data):
    schedule_data.pop("next_run_at")
    schedule = Schedule(**schedule_data, enabled=False)

    assert schedule.next_run_at is None
    assert Schedule.model_validate_json(schedule.model_dump_json()).next_run_at is None
