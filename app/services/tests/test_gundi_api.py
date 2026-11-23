import anyio
import pytest
from app.services.gundi import send_events_to_gundi, send_observations_to_gundi, send_event_attachments_to_gundi, _get_gundi_api_key


@pytest.mark.asyncio
async def test_send_events_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    events = [
        {
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-08 21:51:10-03:00",
            "location": {
                "lat": -51.688645,
                "lon": -72.704421
            },
            "event_details": {
                "site_name": "MM Spot",
                "species": "lion"
            }
        },
        {
            "title": "Animal Sighting",
            "event_type": "wildlife_sighting_rep",
            "recorded_at": "2024-01-08 21:51:10-03:00",
            "location": {
                "lat": -51.688645,
                "lon": -72.704421
            },
            "event_details": {
                "site_name": "MM Spot",
                "species": "lion"
            }
        }
    ]
    response = await send_events_to_gundi(
        events=events,
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_events.assert_called_once_with(data=events)


@pytest.mark.asyncio
async def test_send_event_attachments_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    attachments = [
        ("file1.png", b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00x\x00x\x00\x00\xff\xdb\x00C\x00\x02\x01\x01\x02'),
        ("file2.png", b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x06\x01\x01\x00x\x00x\x01\x00\xff\xd5\x00C\x00\x98\x01\x01\x56')
    ]
    response = await send_event_attachments_to_gundi(
        event_id="dummy-1234",
        attachments=attachments,
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_event_attachments.assert_called_once_with(
        event_id="dummy-1234",
        attachments=attachments
    )


@pytest.mark.asyncio
async def test_send_observations_to_gundi(
        mocker, mock_gundi_client_v2_class, mock_gundi_sensors_client_class,
        mock_get_gundi_api_key, integration_v2
):
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    observations = [
        {
            "source": "device-xy123",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:03:00-0300",
            "location": {
                "lat": -51.748,
                "lon": -72.720
            },
            "additional": {
                "speed_kmph": 5
            }
        },
        {
            "source": "test-device-mariano",
            "type": "tracking-device",
            "subject_type": "puma",
            "recorded_at": "2024-01-24 09:05:00-0300",
            "location": {
                "lat": -51.755,
                "lon": -72.755
            },
            "additional": {
                "speed_kmph": 5
            }
        }
    ]
    response = await send_observations_to_gundi(
        observations=observations,
        integration_id=integration_v2.id
    )

    # Data is sent to gundi using the REST API for now
    assert len(response) == 2
    assert mock_gundi_sensors_client_class.called
    mock_gundi_sensors_client_class.return_value.post_observations.assert_called_once_with(data=observations)


@pytest.mark.asyncio
async def test_send_observations_to_gundi_retries_connection_closed_during_tls_handshake(mocker, integration_v2):
    # httpcore 0.17 raises a bare anyio.EndOfStream when the connection closes during the TLS handshake
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)  # skip the wait between retries
    sensors_api_client = mocker.MagicMock()
    sensors_api_client.post_observations = mocker.AsyncMock(side_effect=[anyio.EndOfStream(), [{"object_id": "1"}]])
    observations = [{"source": "device-xy123", "recorded_at": "2024-01-24 09:03:00-0300", "location": {"lat": -51.7, "lon": -72.7}}]

    response = await send_observations_to_gundi(
        observations=observations,
        sensors_api_client=sensors_api_client,
        integration_id=integration_v2.id
    )

    assert response == [{"object_id": "1"}]
    assert sensors_api_client.post_observations.await_count == 2


@pytest.mark.asyncio
async def test_send_observations_to_gundi_does_not_retry_other_errors(mocker, integration_v2):
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
    sensors_api_client = mocker.MagicMock()
    sensors_api_client.post_observations = mocker.AsyncMock(side_effect=ValueError("bad data"))

    with pytest.raises(ValueError):
        await send_observations_to_gundi(
            observations=[{"source": "device-xy123"}],
            sensors_api_client=sensors_api_client,
            integration_id=integration_v2.id
        )

    assert sensors_api_client.post_observations.await_count == 1


@pytest.mark.asyncio
async def test_get_gundi_api_key_retries_connection_closed_during_tls_handshake(mocker, integration_v2):
    mocker.patch("asyncio.sleep", new_callable=mocker.AsyncMock)
    gundi_client = mocker.MagicMock()
    gundi_client.get_integration_api_key = mocker.AsyncMock(side_effect=[anyio.EndOfStream(), "test-api-key"])
    gundi_client_class = mocker.MagicMock()
    gundi_client_class.return_value.__aenter__ = mocker.AsyncMock(return_value=gundi_client)
    gundi_client_class.return_value.__aexit__ = mocker.AsyncMock(return_value=False)
    mocker.patch("app.services.gundi.GundiClient", gundi_client_class)

    api_key = await _get_gundi_api_key(integration_id=str(integration_v2.id))

    assert api_key == "test-api-key"
    assert gundi_client.get_integration_api_key.await_count == 2
