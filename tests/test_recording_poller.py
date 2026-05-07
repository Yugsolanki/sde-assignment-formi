import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch, MagicMock

from src.services.recording_poller import RecordingPoller
from src.models.postcall_task import PostCallTask, RecordingStatus, TaskStatus


@pytest.fixture
def poller():
    return RecordingPoller()


@pytest.fixture
def mock_task():
    return PostCallTask(
        id="test-task-id",
        interaction_id="test-interaction-id",
        customer_id="test-customer-id",
        recording_status=RecordingStatus.PENDING.value,
        recording_retry_count=0,
        next_poll_at=datetime.now(timezone.utc),
        priority_class=1,
        estimated_tokens=1000,
        status=TaskStatus.QUEUED.value,
    )


@pytest.fixture
def mock_interaction():
    interaction = MagicMock()
    interaction.call_sid = "test-call-sid"
    interaction.exotel_account_id = "test-account-id"
    return interaction


@pytest.mark.asyncio
async def test_recording_ready_success(poller, mock_task, mock_interaction):
    """Test successful recording fetch and upload."""
    with (
        patch("src.services.recording_poller.async_session_factory") as session_mock,
        patch("src.services.recording_poller.httpx.AsyncClient") as httpx_mock,
        patch.object(poller, "_load_interaction", return_value=mock_interaction),
        patch.object(
            poller, "_upload_to_s3", return_value="recordings/test.mp3"
        ) as upload_mock,
    ):

        # Setup session mock
        session = AsyncMock()
        session.execute = AsyncMock()
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        session_factory = AsyncMock()
        session_factory.__aenter__ = AsyncMock(return_value=session)
        session_factory.__aexit__ = AsyncMock()
        session_mock.return_value = session_factory

        # Setup HTTP mock - return recording URL
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "recording_url": "https://exotel.com/recording.mp3"
        }
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock()
        httpx_mock.return_value = mock_client

        # Setup query result
        mock_result = MagicMock()
        mock_result.scalars.return_value.all = AsyncMock(return_value=[mock_task])
        session.execute.return_value = mock_result

        uploaded = await poller.poll_pending_recordings()

        assert uploaded == 1
        assert mock_task.recording_status == RecordingStatus.READY.value
        assert mock_task.recording_s3_key == "recordings/test.mp3"
        upload_mock.assert_called_once()


@pytest.mark.asyncio
async def test_recording_not_ready_schedules_retry(poller, mock_task, mock_interaction):
    """Test scheduling retry when recording not available."""
    with (
        patch("src.services.recording_poller.async_session_factory") as session_mock,
        patch("src.services.recording_poller.httpx.AsyncClient") as httpx_mock,
        patch.object(poller, "_load_interaction", return_value=mock_interaction),
    ):

        # Setup session mock
        session = AsyncMock()
        session.execute = AsyncMock()
        session.commit = AsyncMock()
        session_factory = AsyncMock()
        session_factory.__aenter__ = AsyncMock(return_value=session)
        session_factory.__aexit__ = AsyncMock()
        session_mock.return_value = session_factory

        # Setup HTTP mock - 404 (not ready)
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock()
        httpx_mock.return_value = mock_client

        # Setup query result
        mock_result = MagicMock()
        mock_result.scalars.return_value.all = AsyncMock(return_value=[mock_task])
        session.execute.return_value = mock_result

        uploaded = await poller.poll_pending_recordings()

        assert uploaded == 0
        assert mock_task.recording_status == RecordingStatus.PENDING.value
        assert mock_task.recording_retry_count == 1
        # Verify next_poll_at is set to future time
        assert mock_task.next_poll_at > datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_recording_max_retries_marks_failed(poller, mock_task, mock_interaction):
    """Test marking as failed after max retries."""
    # Set retry count to max
    mock_task.recording_retry_count = poller.MAX_RETRIES - 1

    with (
        patch("src.services.recording_poller.async_session_factory") as session_mock,
        patch("src.services.recording_poller.httpx.AsyncClient") as httpx_mock,
        patch.object(poller, "_load_interaction", return_value=mock_interaction),
        patch.object(
            poller, "_emit_recording_failure_alert", new_callable=AsyncMock
        ) as alert_mock,
    ):

        # Setup session mock
        session = AsyncMock()
        session.execute = AsyncMock()
        session.commit = AsyncMock()
        session_factory = AsyncMock()
        session_factory.__aenter__ = AsyncMock(return_value=session)
        session_factory.__aexit__ = AsyncMock()
        session_mock.return_value = session_factory

        # Setup HTTP mock - 404
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock()
        httpx_mock.return_value = mock_client

        # Setup query result
        mock_result = MagicMock()
        mock_result.scalars.return_value.all = AsyncMock(return_value=[mock_task])
        session.execute.return_value = mock_result

        uploaded = await poller.poll_pending_recordings()

        assert uploaded == 0
        assert mock_task.recording_status == RecordingStatus.FAILED.value
        assert mock_task.recording_retry_count == poller.MAX_RETRIES
        # Verify alert was emitted
        alert_mock.assert_called_once()


@pytest.mark.asyncio
async def test_recording_missing_metadata_marks_failed(poller, mock_task):
    """Test marking as failed when interaction metadata is missing."""
    mock_interaction = MagicMock()
    mock_interaction.call_sid = None  # Missing call_sid

    with (
        patch("src.services.recording_poller.async_session_factory") as session_mock,
        patch.object(poller, "_load_interaction", return_value=mock_interaction),
        patch.object(poller, "_emit_recording_failure_alert", new_callable=AsyncMock),
    ):

        # Setup session mock
        session = AsyncMock()
        session.execute = AsyncMock()
        session.commit = AsyncMock()
        session_factory = AsyncMock()
        session_factory.__aenter__ = AsyncMock(return_value=session)
        session_factory.__aexit__ = AsyncMock()
        session_mock.return_value = session_factory

        # Setup query result
        mock_result = MagicMock()
        mock_result.scalars.return_value.all = AsyncMock(return_value=[mock_task])
        session.execute.return_value = mock_result

        uploaded = await poller.poll_pending_recordings()

        assert uploaded == 0
        assert mock_task.recording_status == RecordingStatus.FAILED.value


def test_calculate_backoff(poller):
    """Test exponential backoff calculation."""
    assert poller._calculate_backoff(0) == 30
    assert poller._calculate_backoff(1) == 60
    assert poller._calculate_backoff(2) == 120
    assert poller._calculate_backoff(3) == 240
    assert poller._calculate_backoff(4) == 480
    # Should cap at max
    assert poller._calculate_backoff(10) == poller.MAX_DELAY_SECONDS
