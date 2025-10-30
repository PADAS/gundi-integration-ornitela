import pytest
from unittest.mock import Mock, patch, MagicMock, mock_open
from datetime import datetime, timedelta, timezone
import aiohttp
from aioresponses import aioresponses

from app.actions.configurations import ProcessTelemetryDataActionConfiguration
from app.actions.handlers import action_process_new_files, _process_telemetry_file, _process_csv_file_streaming
from app.services.file_storage import FileMetadata


@pytest.fixture
def mock_integration():
    integration = Mock()
    integration.id = "test-integration-123"
    return integration


@pytest.fixture
def action_config():
    return ProcessTelemetryDataActionConfiguration(
        bucket_path="telemetry-data",
        archive_days=30,
        delete_after_archive_days=90
    )


@pytest.fixture
def mock_aiohttp():
    """Mock aiohttp calls to prevent real HTTP requests during tests"""
    with aioresponses() as m:
        # Mock Google OAuth2 token refresh
        m.post(
            "https://oauth2.googleapis.com/token",
            payload={
                "access_token": "test-access-token",
                "token_type": "Bearer",
                "expires_in": 3600
            },
            status=200
        )
        # Mock Google Pub/Sub API calls
        m.post(
            "https://pubsub.googleapis.com/v1/projects/*/topics/*:publish",
            payload={"messageIds": ["test-message-id"]},
            status=200
        )
        # Mock Google Cloud Storage API calls
        m.get(
            "https://storage.googleapis.com/storage/v1/b/*/o/*",
            payload={
                "timeCreated": "2024-01-01T00:00:00Z",
                "updated": "2024-01-01T00:00:00Z",
                "size": "1024",
                "contentType": "application/octet-stream"
            },
            status=200
        )
        m.post(
            "https://storage.googleapis.com/upload/storage/v1/b/*/o",
            payload={"name": "test-file"},
            status=200
        )
        m.delete(
            "https://storage.googleapis.com/storage/v1/b/*/o/*",
            status=204
        )
        yield m


def test_process_telemetry_file_json():
    """Test processing JSON telemetry data"""
    json_content = '[{"device_id": "bird001", "timestamp": "2024-01-01T10:00:00Z", "location": {"lat": 40.7128, "lon": -74.0060}}]'
    
    result = _process_telemetry_file(json_content, "test_data.json")
    
    assert len(result) == 1
    assert result[0]["device_id"] == "bird001"
    assert result[0]["location"]["lat"] == 40.7128


def test_process_telemetry_file_invalid_json():
    """Test processing invalid JSON data"""
    invalid_content = "invalid json content"
    
    result = _process_telemetry_file(invalid_content, "test_data.json")
    
    assert len(result) == 1
    assert "parse_error" in result[0]
    assert result[0]["file"] == "test_data.json"


@pytest.mark.asyncio
@patch('app.actions.handlers.trigger_action')
@patch('app.actions.handlers.CloudFileStorage')
@patch('app.actions.handlers.IntegrationStateManager')
async def test_action_process_new_files_success(mock_state_manager, mock_file_storage, mock_trigger_action, mock_integration, action_config, mock_aiohttp):
    """Test successful processing of new files"""
    # Mock state manager
    async def mock_get_state(*args, **kwargs):
        return {"processed_files": [], "archived_files": []}
    
    async def mock_set_state(*args, **kwargs):
        return None
    
    mock_state_manager.return_value.get_state = mock_get_state
    mock_state_manager.return_value.set_state = mock_set_state
    
    # Mock CloudFileStorage
    mock_file_storage_instance = Mock()
    
    async def mock_list_files(*args, **kwargs):
        return ["bird001_20240101.csv"]  # No integration ID in path
    
    async def mock_get_file_metadata(*args, **kwargs):
        return FileMetadata(
            timeCreated=datetime.now(timezone.utc) - timedelta(hours=1),
            updated=datetime.now(timezone.utc) - timedelta(hours=1),
            size=1024,
            contentType="application/json"
        )
    
    async def mock_download_file(*args, **kwargs):
        return None
    
    async def mock_upload_file(*args, **kwargs):
        return None
    
    async def mock_delete_file(*args, **kwargs):
        return None
    
    mock_file_storage_instance.list_files = Mock(side_effect=mock_list_files)
    mock_file_storage_instance.get_file_metadata = Mock(side_effect=mock_get_file_metadata)
    mock_file_storage_instance.download_file = Mock(side_effect=mock_download_file)
    mock_file_storage_instance.upload_file = Mock(side_effect=mock_upload_file)
    mock_file_storage_instance.delete_file = Mock(side_effect=mock_delete_file)
    
    async def mock_stream_file(*args, **kwargs):
        # Mock CSV content
        csv_content = [
            "device_id,device_name,UTC_datetime,UTC_date,UTC_time,datatype,satcount,U_bat_mV,bat_soc_pct,solar_I_mA,hdop,Latitude,Longitude,MSL_altitude_m,Reserved,speed_km/h,direction_deg,int_temperature_C,mag_x,mag_y,mag_z,acc_x,acc_y,acc_z,UTC_timestamp,milliseconds,light,altimeter_m,depth_m,conductivity_mS/cm,ext_temperature_C\n",
            "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 09:10:11,2025-01-18,09:10:11,GPSS,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 09:10:11.0,0,,,,,\n"
        ]
        for chunk in csv_content:
            yield chunk.encode('utf-8')
    
    mock_file_storage_instance.stream_file = mock_stream_file
    mock_file_storage.return_value = mock_file_storage_instance
    
    # Mock trigger_action
    async def mock_trigger_action_func(*args, **kwargs):
        return None
    
    mock_trigger_action.side_effect = mock_trigger_action_func
    
    # Test the action
    result = await action_process_new_files(mock_integration, action_config)
    
    assert result["status"] == "success"
    assert result["new_files_found"] == 1
    assert result["subactions_triggered"] == 1
    assert result["files_archived"] == 0
    assert result["files_deleted"] == 0


@pytest.mark.asyncio
@patch('app.actions.handlers.CloudFileStorage')
@patch('app.actions.handlers.IntegrationStateManager')
async def test_action_process_new_files_with_archiving(mock_state_manager, mock_file_storage, mock_integration, action_config, mock_aiohttp):
    """Test processing with file archiving"""
    # Mock state with processed files that should be archived
    old_time = datetime.now(timezone.utc) - timedelta(days=35)  # Older than archive_days (30)
    
    async def mock_get_state(*args, **kwargs):
        return {
            "processed_files": ["old_file.json"],  # No integration ID in path
            "archived_files": []
        }
    
    async def mock_set_state(*args, **kwargs):
        return None
    
    mock_state_manager.return_value.get_state = mock_get_state
    mock_state_manager.return_value.set_state = mock_set_state
    
    # Mock CloudFileStorage
    mock_file_storage_instance = Mock()
    
    async def mock_list_files(*args, **kwargs):
        return ["old_file.json"]  # No integration ID in path
    
    async def mock_get_file_metadata(*args, **kwargs):
        return FileMetadata(
            timeCreated=old_time,
            updated=old_time,
            size=1024,
            contentType="application/json"
        )
    
    async def mock_download_file(*args, **kwargs):
        return None
    
    async def mock_upload_file(*args, **kwargs):
        return None
    
    async def mock_delete_file(*args, **kwargs):
        return None
    
    mock_file_storage_instance.list_files = Mock(side_effect=mock_list_files)
    mock_file_storage_instance.get_file_metadata = Mock(side_effect=mock_get_file_metadata)
    mock_file_storage_instance.download_file = Mock(side_effect=mock_download_file)
    mock_file_storage_instance.upload_file = Mock(side_effect=mock_upload_file)
    mock_file_storage_instance.delete_file = Mock(side_effect=mock_delete_file)
    
    mock_file_storage.return_value = mock_file_storage_instance
    
    # Mock file content
    with patch('builtins.open', mock_open(read_data='{"device_id": "bird001", "data": "test"}')):
        result = await action_process_new_files(mock_integration, action_config)
    
        assert result["status"] == "success"
        # Note: Archiving logic is now handled in the process_ornitela_file action, not here
        assert result["files_archived"] == 0


@pytest.mark.asyncio
@patch('app.actions.handlers.CloudFileStorage')
@patch('app.actions.handlers.IntegrationStateManager')
async def test_action_process_new_files_with_deletion(mock_state_manager, mock_file_storage, mock_integration, action_config, mock_aiohttp):
    """Test processing with file deletion"""
    # Mock state with archived files that should be deleted
    very_old_time = datetime.now(timezone.utc) - timedelta(days=95)  # Older than delete_after_archive_days (90)
    
    async def mock_get_state(*args, **kwargs):
        return {
            "processed_files": ["very_old_file.json"],  # No integration ID in path
            "archived_files": ["very_old_file.json"]   # No integration ID in path
        }
    
    async def mock_set_state(*args, **kwargs):
        return None
    
    mock_state_manager.return_value.get_state = mock_get_state
    mock_state_manager.return_value.set_state = mock_set_state
    
    # Mock CloudFileStorage
    mock_file_storage_instance = Mock()
    
    async def mock_list_files(*args, **kwargs):
        return ["very_old_file.json"]  # No integration ID in path
    
    async def mock_get_file_metadata(*args, **kwargs):
        return FileMetadata(
            timeCreated=very_old_time,
            updated=very_old_time,
            size=1024,
            contentType="application/json"
        )
    
    async def mock_download_file(*args, **kwargs):
        return None
    
    async def mock_upload_file(*args, **kwargs):
        return None
    
    async def mock_delete_file(*args, **kwargs):
        return None
    
    mock_file_storage_instance.list_files = Mock(side_effect=mock_list_files)
    mock_file_storage_instance.get_file_metadata = Mock(side_effect=mock_get_file_metadata)
    mock_file_storage_instance.download_file = Mock(side_effect=mock_download_file)
    mock_file_storage_instance.upload_file = Mock(side_effect=mock_upload_file)
    mock_file_storage_instance.delete_file = Mock(side_effect=mock_delete_file)
    
    mock_file_storage.return_value = mock_file_storage_instance
    
    result = await action_process_new_files(mock_integration, action_config)
    
    assert result["status"] == "success"
    # Note: Deletion logic is now handled in the process_ornitela_file action, not here
    assert result["files_deleted"] == 0


@pytest.mark.asyncio
@patch('app.actions.handlers.CloudFileStorage')
async def test_action_process_new_files_credentials_error(mock_file_storage, mock_integration, action_config):
    """Test error handling when CloudFileStorage fails"""
    mock_file_storage.side_effect = Exception("Cloud storage connection failed")
    
    result = await action_process_new_files(mock_integration, action_config)
    
    assert result["status"] == "error"
    assert "Cloud storage connection failed" in result["error"]


@pytest.mark.asyncio
async def test_process_csv_file_streaming():
    """Test streaming CSV file processing"""
    # Mock file storage
    mock_file_storage = Mock()
    
    # Mock CSV content with GPS and sensor data
    csv_content = [
        "device_id,device_name,UTC_datetime,UTC_date,UTC_time,datatype,satcount,U_bat_mV,bat_soc_pct,solar_I_mA,hdop,Latitude,Longitude,MSL_altitude_m,Reserved,speed_km/h,direction_deg,int_temperature_C,mag_x,mag_y,mag_z,acc_x,acc_y,acc_z,UTC_timestamp,milliseconds,light,altimeter_m,depth_m,conductivity_mS/cm,ext_temperature_C\n",
        "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 09:10:11,2025-01-18,09:10:11,GPSS,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 09:10:11.0,0,,,,,\n",
        "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 09:10:12,2025-01-18,09:10:12,SEN_ALL_20Hz_START,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 09:10:12.0,0,,,,,\n",
        "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 09:10:13,2025-01-18,09:10:13,SEN_ALL_20Hz,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 09:10:13.0,0,,,,,\n",
        "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 09:10:14,2025-01-18,09:10:14,SEN_ALL_20Hz_END,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 09:10:14.0,0,,,,,\n"
    ]
    
    async def mock_stream_file(*args, **kwargs):
        for chunk in csv_content:
            yield chunk.encode('utf-8')
    
    mock_file_storage.stream_file = mock_stream_file
    
    result = await _process_csv_file_streaming(mock_file_storage, "test-integration", "test_data.csv")
    
    # Should have 1 observation (GPS + sensor readings grouped together)
    assert len(result) == 1
    observation = result[0]
    
    # Check GPS location data
    assert observation["device_id"] == "226976"
    assert observation["device_name"] == "GF_BAR_2022_ADU_W_IMA_Gauele"
    assert observation["timestamp"] == "2025-01-18 09:10:11"
    assert observation["location"]["lat"] == 44.394531250000000
    assert observation["location"]["lon"] == 5.370184421539307
    assert observation["device_status"]["battery_voltage"] == 3702.0
    assert observation["device_status"]["battery_soc"] == 8.0
    assert observation["device_status"]["satellite_count"] == 3
    assert observation["additional"]["datatype"] == "GPSS"
    
    # Check sensor readings
    assert "sensor_readings" in observation
    assert observation["sensor_count"] == 2  # SEN_ALL_20Hz_START and SEN_ALL_20Hz
    assert len(observation["sensor_readings"]) == 2
    
    # Check sensor reading structure
    sensor_reading = observation["sensor_readings"][0]
    assert sensor_reading["datatype"] == "SEN_ALL_20Hz_START"
    assert sensor_reading["timestamp"] == "2025-01-18 09:10:12"
    
    sensor_reading2 = observation["sensor_readings"][1]
    assert sensor_reading2["datatype"] == "SEN_ALL_20Hz"
    assert sensor_reading2["timestamp"] == "2025-01-18 09:10:13"


@pytest.mark.asyncio
async def test_process_csv_file_streaming_gps_only():
    """Test streaming CSV file processing with GPS data only (no sensors)"""
    # Mock file storage
    mock_file_storage = Mock()
    
    # Mock CSV content with GPS data only (no sensor sequences)
    csv_content = [
        "device_id,device_name,UTC_datetime,UTC_date,UTC_time,datatype,satcount,U_bat_mV,bat_soc_pct,solar_I_mA,hdop,Latitude,Longitude,MSL_altitude_m,Reserved,speed_km/h,direction_deg,int_temperature_C,mag_x,mag_y,mag_z,acc_x,acc_y,acc_z,UTC_timestamp,milliseconds,light,altimeter_m,depth_m,conductivity_mS/cm,ext_temperature_C\n",
        "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 09:10:11,2025-01-18,09:10:11,GPSS,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 09:10:11.0,0,,,,,\n",
        "226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 10:10:11,2025-01-18,10:10:11,GPSS,3,3702,8,,,44.395652770996094,5.367559432983398,,,,,,,,,,,247,2025-01-18 10:10:11.0,0,,,,,\n"
    ]
    
    async def mock_stream_file(*args, **kwargs):
        for chunk in csv_content:
            yield chunk.encode('utf-8')
    
    mock_file_storage.stream_file = mock_stream_file
    
    result = await _process_csv_file_streaming(mock_file_storage, "test-integration", "test_data.csv")
    
    # Should have 2 observations (2 GPS records)
    assert len(result) == 2
    
    # Check first observation
    observation1 = result[0]
    assert observation1["device_id"] == "226976"
    assert observation1["timestamp"] == "2025-01-18 09:10:11"
    assert observation1["location"]["lat"] == 44.394531250000000
    assert observation1["location"]["lon"] == 5.370184421539307
    assert observation1["sensor_count"] == 0  # No sensor readings
    assert len(observation1["sensor_readings"]) == 0
    
    # Check second observation
    observation2 = result[1]
    assert observation2["device_id"] == "226976"
    assert observation2["timestamp"] == "2025-01-18 10:10:11"
    assert observation2["location"]["lat"] == 44.395652770996094
    assert observation2["location"]["lon"] == 5.367559432983398
    assert observation2["sensor_count"] == 0  # No sensor readings
    assert len(observation2["sensor_readings"]) == 0


@pytest.mark.asyncio
async def test_generate_gundi_observations():
    """Test the generate_gundi_observations function"""
    from app.actions.handlers import generate_gundi_observations
    from datetime import datetime, timedelta, timezone
    
    # Mock grouped observation data
    recent_time = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    grouped_observations = [
        {
            "file": "test_data.csv",
            "observation_id": "226976_2025-01-18_09:10:11",
            "timestamp": recent_time,
            "device_id": "226976",
            "device_name": "GF_BAR_2022_ADU_W_IMA_Gauele",
            "location": {
                "lat": 44.394531250000000,
                "lon": 5.370184421539307,
                "altitude": None
            },
            "movement": {
                "speed": None,
                "direction": None
            },
            "device_status": {
                "battery_voltage": 3702.0,
                "battery_soc": 8.0,
                "satellite_count": 3
            },
                "sensor_readings": [
                    {
                        "timestamp": (datetime.now(timezone.utc) - timedelta(days=1, minutes=1)).strftime("%Y-%m-%d %H:%M:%S"),
                        "datatype": "SEN_ALL_20Hz_START",
                        "environmental": {
                            "temperature": 25.5,
                            "light": 1000.0
                        },
                        "additional": {"milliseconds": 100},
                        "movement": {"speed": None, "direction": None},
                        "device_status": {"battery_voltage": 3702.0},
                        "sensors": {},
                    },
                    {
                        "timestamp": (datetime.now(timezone.utc) - timedelta(days=1, minutes=2)).strftime("%Y-%m-%d %H:%M:%S"),
                        "datatype": "SEN_ALL_20Hz",
                        "environmental": {
                            "temperature": 25.6,
                            "light": 1001.0
                        },
                        "additional": {"milliseconds": 200},
                        "movement": {"speed": None, "direction": None},
                        "device_status": {"battery_voltage": 3702.0},
                        "sensors": {},
                    }
                ],
            "sensor_count": 2,
            "additional": {
                "datatype": "GPSS"
            }
        }
    ]
    
    # Generate individual observations (convert generator to list for testing)
    result = list(generate_gundi_observations(grouped_observations, historical_limit_days=30))
    
    # Should have 3 observations (1 GPS + 2 sensor readings)
    assert len(result) == 3
    
    # Check GPS-only observation (first)
    gps_observation = result[0]
    assert "recorded_at" in gps_observation
    assert gps_observation["location"]["lat"] == 44.394531250000000
    assert gps_observation["source"] == "226976"
    assert gps_observation["source_name"] == "GF_BAR_2022_ADU_W_IMA_Gauele"
    assert gps_observation["type"] == "tracking-device"
    
    # Check first sensor observation
    sensor_observation1 = result[1]
    assert "recorded_at" in sensor_observation1  # Should have recorded_at field
    assert sensor_observation1["location"]["lat"] == 44.394531250000000  # GPS location applied
    assert sensor_observation1["source"] == "226976"
    assert sensor_observation1["source_name"] == "GF_BAR_2022_ADU_W_IMA_Gauele"
    assert sensor_observation1["type"] == "tracking-device"
    
    # Check second sensor observation
    sensor_observation2 = result[2]
    assert "recorded_at" in sensor_observation2  # Should have recorded_at field
    assert sensor_observation2["location"]["lat"] == 44.394531250000000  # GPS location applied
    assert sensor_observation2["source"] == "226976"
    assert sensor_observation2["source_name"] == "GF_BAR_2022_ADU_W_IMA_Gauele"
    assert sensor_observation2["type"] == "tracking-device"


@pytest.mark.asyncio
async def test_generate_gundi_observations_individual_sensor_readings():
    """Test that individual observations are created for each sensor reading"""
    from app.actions.handlers import generate_gundi_observations
    from datetime import datetime, timedelta, timezone
    
    # Mock grouped observation data with multiple readings for the same timestamp
    recent_time = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    grouped_observations = [
        {
            "file": "test_data.csv",
            "observation_id": "226976_2025-01-18_09:10:11",
            "timestamp": recent_time,
            "device_id": "226976",
            "device_name": "GF_BAR_2022_ADU_W_IMA_Gauele",
            "location": {
                "lat": 44.394531250000000,
                "lon": 5.370184421539307,
                "altitude": None
            },
            "movement": {
                "speed": None,
                "direction": None
            },
            "device_status": {
                "battery_voltage": 3702.0,
                "battery_soc": 8.0,
                "satellite_count": 3
            },
                "sensor_readings": [
                    {
                        "timestamp": (datetime.now(timezone.utc) - timedelta(days=1, minutes=1)).strftime("%Y-%m-%d %H:%M:%S"),
                        "datatype": "SEN_ALL_20Hz_START",
                        "environmental": {
                            "temperature": 25.5,
                            "light": 1000.0
                        },
                        "additional": {
                            "milliseconds": 100
                        },
                        "movement": {"speed": None, "direction": None},
                        "device_status": {"battery_voltage": 3702.0},
                        "sensors": {},
                    },
                    {
                        "timestamp": (datetime.now(timezone.utc) - timedelta(days=1, minutes=1)).strftime("%Y-%m-%d %H:%M:%S"),  # Same timestamp as above
                        "datatype": "SEN_ALL_20Hz",
                        "environmental": {
                            "temperature": 25.6,
                            "light": 1001.0
                        },
                        "additional": {
                            "milliseconds": 200
                        },
                        "movement": {"speed": None, "direction": None},
                        "device_status": {"battery_voltage": 3702.0},
                        "sensors": {},
                    },
                    {
                        "timestamp": (datetime.now(timezone.utc) - timedelta(days=1, minutes=2)).strftime("%Y-%m-%d %H:%M:%S"),  # Different timestamp
                        "datatype": "SEN_ALL_20Hz_END",
                        "environmental": {
                            "temperature": 25.7,
                            "light": 1002.0
                        },
                        "additional": {
                            "milliseconds": 50
                        },
                        "movement": {"speed": None, "direction": None},
                        "device_status": {"battery_voltage": 3702.0},
                        "sensors": {},
                    }
                ],
            "sensor_count": 3,
            "additional": {
                "datatype": "GPSS"
            }
        }
    ]
    
    # Generate individual observations (convert generator to list for testing)
    result = list(generate_gundi_observations(grouped_observations, historical_limit_days=30))
    
    # Should have 4 observations (1 GPS + 3 individual sensor readings)
    assert len(result) == 4
    
    # Check GPS-only observation (first)
    gps_observation = result[0]
    assert "recorded_at" in gps_observation
    assert gps_observation["source"] == "226976"
    assert gps_observation["type"] == "tracking-device"
    
    # Check first sensor observation (SEN_ALL_20Hz_START)
    sensor_observation1 = result[1]
    assert "recorded_at" in sensor_observation1
    assert sensor_observation1["source"] == "226976"
    assert sensor_observation1["type"] == "tracking-device"
    
    # Check second sensor observation (SEN_ALL_20Hz)
    sensor_observation2 = result[2]
    assert "recorded_at" in sensor_observation2
    assert sensor_observation2["source"] == "226976"
    assert sensor_observation2["type"] == "tracking-device"
    
    # Check third sensor observation (SEN_ALL_20Hz_END)
    sensor_observation3 = result[3]
    assert "recorded_at" in sensor_observation3
    assert sensor_observation3["source"] == "226976"
    assert sensor_observation3["type"] == "tracking-device"


@pytest.mark.asyncio
async def test_generate_gundi_observations_memory_efficiency():
    """Test that the generator is memory efficient by yielding observations one at a time"""
    from app.actions.handlers import generate_gundi_observations
    from datetime import datetime, timedelta, timezone
    
    # Create a large dataset to test memory efficiency
    large_grouped_observations = []
    base_time = datetime.now(timezone.utc) - timedelta(days=1)
    for i in range(100):  # 100 GPS records
        timestamp = (base_time + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
        observation = {
            "file": f"test_data_{i}.csv",
            "observation_id": f"226976_{timestamp.replace(' ', '_').replace(':', '-')}",
            "timestamp": timestamp,
            "device_id": "226976",
            "device_name": "GF_BAR_2022_ADU_W_IMA_Gauele",
            "location": {"lat": 44.394531250000000, "lon": 5.370184421539307},
            "movement": {"speed": None, "direction": None},
            "device_status": {"battery_voltage": 3702.0, "battery_soc": 8.0, "satellite_count": 3},
                "sensor_readings": [
                    {
                        "timestamp": (base_time + timedelta(minutes=i, seconds=1)).strftime("%Y-%m-%d %H:%M:%S"),
                        "datatype": "SEN_ALL_20Hz_START",
                        "additional": {"milliseconds": 100},
                        "movement": {"speed": None, "direction": None},
                        "device_status": {"battery_voltage": 3702.0},
                        "sensors": {},
                        "environmental": {}
                    },
                    {
                        "timestamp": (base_time + timedelta(minutes=i, seconds=2)).strftime("%Y-%m-%d %H:%M:%S"),
                        "datatype": "SEN_ALL_20Hz",
                        "additional": {"milliseconds": 200},
                        "movement": {"speed": None, "direction": None},
                        "device_status": {"battery_voltage": 3702.0},
                        "sensors": {},
                        "environmental": {}
                    },
                    {
                        "timestamp": (base_time + timedelta(minutes=i, seconds=3)).strftime("%Y-%m-%d %H:%M:%S"),
                        "datatype": "SEN_ALL_20Hz_END",
                        "additional": {"milliseconds": 300},
                        "movement": {"speed": None, "direction": None},
                        "device_status": {"battery_voltage": 3702.0},
                        "sensors": {},
                        "environmental": {}
                    }
                ],
            "sensor_count": 3,
            "additional": {"datatype": "GPSS"}
        }
        large_grouped_observations.append(observation)
    
    # Test that we can iterate through the generator without loading everything into memory
    count = 0
    for observation in generate_gundi_observations(large_grouped_observations, historical_limit_days=30):
        count += 1
        # Verify each observation has the expected structure
        assert "recorded_at" in observation
        assert "source" in observation  # Changed from device_id
        assert "location" in observation
        
        # Stop after processing a few to demonstrate memory efficiency
        if count >= 10:
            break
    
    # Should have processed 10 observations (generator yields one at a time)
    assert count == 10


@pytest.mark.asyncio
async def test_generate_gundi_observations_historical_limit():
    """Test that historical_limit_days filters out old observations"""
    from app.actions.handlers import generate_gundi_observations
    from datetime import datetime, timedelta, timezone
    
    # Create test data with old and recent observations
    current_time = datetime.now(timezone.utc)
    old_time = current_time - timedelta(days=35)  # 35 days ago
    recent_time = current_time - timedelta(days=5)  # 5 days ago
    
    grouped_observations = [
        {
            "file": "old_data.csv",
            "timestamp": old_time.strftime("%Y-%m-%d %H:%M:%S"),
            "device_id": "226976",
            "device_name": "Old Device",
            "location": {"lat": 44.0, "lon": 5.0, "altitude": None},
            "movement": {"speed": None, "direction": None},
            "device_status": {"battery_voltage": 3702.0},
            "sensor_readings": [
                {
                    "timestamp": old_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "additional": {"milliseconds": 100},
                    "movement": {"speed": None, "direction": None},
                    "device_status": {"battery_voltage": 3702.0},
                    "sensors": {},
                    "environmental": {}
                }
            ],
            "sensors": {},
            "additional": {"datatype": "GPS"}
        },
        {
            "file": "recent_data.csv",
            "timestamp": recent_time.strftime("%Y-%m-%d %H:%M:%S"),
            "device_id": "226977",
            "device_name": "Recent Device",
            "location": {"lat": 45.0, "lon": 6.0, "altitude": None},
            "movement": {"speed": None, "direction": None},
            "device_status": {"battery_voltage": 3800.0},
            "sensor_readings": [
                {
                    "timestamp": recent_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "additional": {"milliseconds": 200},
                    "movement": {"speed": None, "direction": None},
                    "device_status": {"battery_voltage": 3800.0},
                    "sensors": {},
                    "environmental": {}
                }
            ],
            "sensors": {},
            "additional": {"datatype": "GPS"}
        }
    ]
    
    # Test with 30-day limit - should only include recent observations
    result_30_days = list(generate_gundi_observations(grouped_observations, historical_limit_days=30))
    
    # Should have 2 observations (1 GPS + 1 sensor) from recent data only
    assert len(result_30_days) == 2
    assert all(obs["source"] == "226977" for obs in result_30_days)  # Only recent device
    
    # Test with 40-day limit - should include both old and recent observations
    result_40_days = list(generate_gundi_observations(grouped_observations, historical_limit_days=40))
    
    # Should have 4 observations (2 GPS + 2 sensor) from both devices
    assert len(result_40_days) == 4
    sources = [obs["source"] for obs in result_40_days]
    assert "226976" in sources  # Old device
    assert "226977" in sources  # Recent device


@pytest.mark.asyncio
async def test_process_csv_file_streaming_large_file():
    """Test streaming CSV file processing with a large file"""
    # Mock file storage
    mock_file_storage = Mock()
    
    # Mock large CSV content (simulate 1000 GPS records with sensor data)
    csv_content = ["device_id,device_name,UTC_datetime,UTC_date,UTC_time,datatype,satcount,U_bat_mV,bat_soc_pct,solar_I_mA,hdop,Latitude,Longitude,MSL_altitude_m,Reserved,speed_km/h,direction_deg,int_temperature_C,mag_x,mag_y,mag_z,acc_x,acc_y,acc_z,UTC_timestamp,milliseconds,light,altimeter_m,depth_m,conductivity_mS/cm,ext_temperature_C\n"]
    for i in range(1000):
        # GPS record
        csv_content.append(f"226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 {i%24:02d}:{i%60:02d}:00,2025-01-18,{i%24:02d}:{i%60:02d}:00,GPSS,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 {i%24:02d}:{i%60:02d}:00.0,0,,,,,\n")
        # Sensor start
        csv_content.append(f"226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 {i%24:02d}:{i%60:02d}:01,2025-01-18,{i%24:02d}:{i%60:02d}:01,SEN_ALL_20Hz_START,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 {i%24:02d}:{i%60:02d}:01.0,0,,,,,\n")
        # Sensor data
        csv_content.append(f"226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 {i%24:02d}:{i%60:02d}:02,2025-01-18,{i%24:02d}:{i%60:02d}:02,SEN_ALL_20Hz,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 {i%24:02d}:{i%60:02d}:02.0,0,,,,,\n")
        # Sensor end
        csv_content.append(f"226976,GF_BAR_2022_ADU_W_IMA_Gauele,2025-01-18 {i%24:02d}:{i%60:02d}:03,2025-01-18,{i%24:02d}:{i%60:02d}:03,SEN_ALL_20Hz_END,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,2025-01-18 {i%24:02d}:{i%60:02d}:03.0,0,,,,,\n")
    
    async def mock_stream_file(*args, **kwargs):
        for chunk in csv_content:
            yield chunk.encode('utf-8')
    
    mock_file_storage.stream_file = mock_stream_file
    
    result = await _process_csv_file_streaming(mock_file_storage, "test-integration", "large_data.csv")
    
    # Should have 1000 observations (GPS + sensor data grouped)
    assert len(result) == 1000
    assert all(record["device_id"] == "226976" for record in result)
    assert all(record["device_name"] == "GF_BAR_2022_ADU_W_IMA_Gauele" for record in result)
    assert all(record["file"] == "large_data.csv" for record in result)
    
    # Check that each observation has sensor readings
    for record in result:
        assert "sensor_readings" in record
        assert record["sensor_count"] == 2  # START and SEN_ALL_20Hz


@pytest.mark.asyncio
@patch('app.services.activity_logger.publish_event')
@patch('app.actions.handlers.log_action_activity')
@patch('app.actions.handlers.send_observations_to_gundi')
@patch('app.actions.handlers.CloudFileStorage')
@patch('app.actions.handlers.FileProcessingLockManager')
async def test_action_process_ornitela_file(mock_lock_manager, mock_file_storage, mock_send_observations, mock_log_activity, mock_publish_event, action_config, mock_aiohttp):
    """Test the process_ornitela_file action handler"""
    from app.actions.handlers import action_process_ornitela_file
    from app.actions.configurations import ProcessOrnitelaFileActionConfiguration
    
    # Mock integration
    mock_integration = Mock()
    mock_integration.id = "test-integration-id"
    mock_integration.name = "Test Integration"
    
    # Create config for single file processing
    file_config = ProcessOrnitelaFileActionConfiguration(
        bucket_path=action_config.bucket_path,
        file_name="test_file.csv",
        historical_limit_days=30
    )
    
    # Mock CloudFileStorage
    mock_file_storage_instance = Mock()
    
    async def mock_stream_file(*args, **kwargs):
        # Mock CSV content with recent date
        recent_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        csv_content = [
            "device_id,device_name,UTC_datetime,UTC_date,UTC_time,datatype,satcount,U_bat_mV,bat_soc_pct,solar_I_mA,hdop,Latitude,Longitude,MSL_altitude_m,Reserved,speed_km/h,direction_deg,int_temperature_C,mag_x,mag_y,mag_z,acc_x,acc_y,acc_z,UTC_timestamp,milliseconds,light,altimeter_m,depth_m,conductivity_mS/cm,ext_temperature_C\n",
            f"226976,GF_BAR_2022_ADU_W_IMA_Gauele,{recent_date},{recent_date.split()[0]},{recent_date.split()[1]},GPSS,3,3702,8,,,44.394531250000000,5.370184421539307,,,,,,,,,,,247,{recent_date}.0,0,,,,,\n"
        ]
        for chunk in csv_content:
            yield chunk.encode('utf-8')
    
    async def mock_get_file_metadata(*args, **kwargs):
        return FileMetadata(
            timeCreated=datetime.now(timezone.utc) - timedelta(hours=1),
            updated=datetime.now(timezone.utc) - timedelta(hours=1),
            size=1024,
            contentType="text/csv"
        )
    
    async def mock_download_file(*args, **kwargs):
        return None
    
    async def mock_upload_file(*args, **kwargs):
        return None
    
    async def mock_delete_file(*args, **kwargs):
        return None
    
    mock_file_storage_instance.stream_file = mock_stream_file
    mock_file_storage_instance.get_file_metadata = mock_get_file_metadata
    mock_file_storage_instance.download_file = mock_download_file
    mock_file_storage_instance.upload_file = mock_upload_file
    mock_file_storage_instance.delete_file = mock_delete_file
    mock_file_storage.return_value = mock_file_storage_instance
    
    # Mock send_observations_to_gundi to prevent HTTP calls
    async def mock_send_observations_func(*args, **kwargs):
        print(f"DEBUG: send_observations_to_gundi called with args={args}, kwargs={kwargs}")
        return {"status": "success"}
    
    mock_send_observations.side_effect = mock_send_observations_func
    
    # Mock FileProcessingLockManager to prevent Redis calls
    mock_lock_instance = Mock()
    async def mock_acquire_lock(*args, **kwargs):
        return True  # Always allow lock acquisition
    async def mock_release_lock(*args, **kwargs):
        return True  # Always succeed at releasing
    mock_lock_instance.acquire_lock = mock_acquire_lock
    mock_lock_instance.release_lock = mock_release_lock
    mock_lock_manager.return_value = mock_lock_instance
    
    # Mock publish_event to prevent HTTP calls
    async def mock_publish_event_func(*args, **kwargs):
        return None
    
    mock_publish_event.side_effect = mock_publish_event_func
    
    # Test the action
    result = await action_process_ornitela_file(mock_integration, file_config)
    
    # Debug output
    print(f"DEBUG: result = {result}")
    
    # Verify results
    assert result["status"] == "success"
    assert result["file_name"] == "test_file.csv"
    assert result["telemetry_records"] > 0
    assert result["observations_sent"] > 0
    
    # Verify send_observations_to_gundi was called
    mock_send_observations.assert_called()


@pytest.mark.asyncio
@patch('app.services.activity_logger.publish_event')
@patch('app.actions.handlers.log_action_activity')
@patch('app.services.gundi.send_observations_to_gundi')
@patch('app.actions.handlers.CloudFileStorage')
@patch('app.actions.handlers.FileProcessingLockManager')
async def test_action_process_ornitela_file_skip_non_csv(mock_lock_manager, mock_file_storage, mock_send_observations, mock_log_activity, mock_publish_event, action_config, mock_aiohttp):
    """Test that non-CSV files are skipped"""
    from app.actions.handlers import action_process_ornitela_file
    from app.actions.configurations import ProcessOrnitelaFileActionConfiguration
    
    # Mock integration
    mock_integration = Mock()
    mock_integration.id = "test-integration-id"
    mock_integration.name = "Test Integration"
    
    # Create config for non-CSV file
    file_config = ProcessOrnitelaFileActionConfiguration(
        bucket_path=action_config.bucket_path,
        file_name="test_file.json",
        historical_limit_days=30
    )
    
    # Mock CloudFileStorage
    mock_file_storage.return_value = Mock()
    
    # Mock FileProcessingLockManager to prevent Redis calls
    mock_lock_instance = Mock()
    async def mock_acquire_lock(*args, **kwargs):
        return True  # Always allow lock acquisition
    async def mock_release_lock(*args, **kwargs):
        return True  # Always succeed at releasing
    mock_lock_instance.acquire_lock = mock_acquire_lock
    mock_lock_instance.release_lock = mock_release_lock
    mock_lock_manager.return_value = mock_lock_instance
    
    # Mock publish_event to prevent HTTP calls
    async def mock_publish_event_func(*args, **kwargs):
        return None
    
    mock_publish_event.side_effect = mock_publish_event_func
    
    # Test the action
    result = await action_process_ornitela_file(mock_integration, file_config)
    
    # Verify file was skipped
    assert result["status"] == "skipped"
    assert result["reason"] == "Not a CSV file"
    assert result["file_name"] == "test_file.json"
    
    # Verify send_observations_to_gundi was NOT called
    mock_send_observations.assert_not_called()


@pytest.mark.asyncio
async def test_process_csv_file_streaming_encoding_handling():
    """Test CSV file processing with different encodings"""
    from app.actions.handlers import _process_csv_file_streaming, _detect_encoding
    
    # Test encoding detection function
    utf8_chunk = "test data".encode('utf-8')
    
    # Test with problematic byte (0xc3) - this should be detected as UTF-8
    problematic_chunk = b'\xc3\xa9'  # é in UTF-8
    detected = _detect_encoding(problematic_chunk)
    assert detected == 'utf-8'  # Should detect UTF-8 for valid UTF-8 sequences
    
    # Test with invalid UTF-8 that would work in Latin-1
    latin1_chunk = b'\xc3\xa9'  # This is valid UTF-8, so it will be detected as UTF-8
    detected_latin1 = _detect_encoding(latin1_chunk)
    assert detected_latin1 == 'utf-8'
    
    # Mock file storage with content that has encoding issues
    mock_file_storage = Mock()
    
    # Create CSV content with a character that causes issues (simulating the 0xc3 byte issue)
    csv_content = "device_id,device_name,UTC_datetime,datatype\n226976,Test Device,2025-01-18 09:10:11,GPS\n"
    # Add some problematic bytes that would cause UnicodeDecodeError with UTF-8
    problematic_content = csv_content.encode('utf-8') + b'\xc3\xa9'  # Add é character
    
    async def mock_stream_file(*args, **kwargs):
        # Yield the content in chunks
        chunk_size = 20
        for i in range(0, len(problematic_content), chunk_size):
            yield problematic_content[i:i+chunk_size]
    
    mock_file_storage.stream_file = mock_stream_file
    
    result = await _process_csv_file_streaming(mock_file_storage, "test-integration", "problematic_data.csv")
    
    # Should have processed the data successfully despite encoding issues
    assert len(result) == 1
    assert result[0]["device_id"] == "226976"
    assert result[0]["device_name"] == "Test Device"
