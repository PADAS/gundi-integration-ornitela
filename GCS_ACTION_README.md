# Google Cloud Storage Telemetry Data Processing Action

This action runner processes telemetry data files uploaded to a Google Cloud Storage bucket for bird tracking devices.

## Configuration

The action uses the `PullActionConfiguration` class with the following parameters:

- **bucket_path** (optional): Path within the bucket where telemetry files are stored (default: "")
- **archive_days** (optional): Number of days after processing before files are archived (default: 30)
- **delete_after_archive_days** (optional): Number of days after archiving before files are deleted (default: 90)

## How it Works

1. **File Discovery**: Lists all files in the specified GCS bucket path
2. **New File Processing**: Identifies files that haven't been processed yet
3. **Data Processing**: Downloads and processes telemetry data from new files
4. **State Management**: Tracks processed files using Redis state management
5. **File Archiving**: Moves processed files to an `archive/` folder after the specified number of days
6. **File Cleanup**: Deletes archived files after the specified retention period

## File Processing

The action supports both JSON and CSV format telemetry data with optimized processing:

- **CSV Files**: Uses streaming processing for memory efficiency with large files. The `_process_csv_file_streaming` function processes CSV files line-by-line without loading the entire file into memory.
- **JSON Files**: Uses the standard `_process_telemetry_file` function for smaller JSON files.

### CSV Processing Features

- **Memory Efficient**: Streams large CSV files without loading them entirely into memory
- **Data Grouping**: Groups GPS locations with their associated sensor readings into single observations
- **File Type Support**: Handles both SMS and GPRS files with the same column structure
- **Sensor Sequence Processing**: Recognizes and processes sensor data sequences (START, DATA, END)
- **Error Resilience**: Continues processing even if individual CSV rows have parsing errors
- **Progress Logging**: Logs progress every 1000 observations for large files

### Data Transformation Logic

The CSV processor intelligently groups related data:

1. **GPS Records** (`datatype = "GPSS"`): Creates new observations with location data
2. **Sensor Sequences**: Groups sensor readings with their GPS location:
   - `SEN_ALL_20Hz_START`: Begins a sensor reading sequence
   - `SEN_ALL_20Hz`: Individual sensor readings within the sequence
   - `SEN_ALL_20Hz_END`: Ends the sensor reading sequence

**Result**: Each observation contains:
- GPS location and device status from the GPSS record
- Array of sensor readings collected during that location period
- Unique observation ID combining device and timestamp

### Expected CSV Format

The CSV processing is optimized for bird tracking device data with the following structure:

**Core Fields:**
- `device_id`: Unique device identifier
- `device_name`: Device name/description
- `UTC_datetime`: ISO format timestamp
- `Latitude`: Decimal latitude coordinate
- `Longitude`: Decimal longitude coordinate
- `MSL_altitude_m`: Altitude in meters

**Device Status:**
- `U_bat_mV`: Battery voltage in millivolts
- `bat_soc_pct`: Battery state of charge percentage
- `solar_I_mA`: Solar panel current in milliamps
- `satcount`: Satellite count for GPS
- `hdop`: Horizontal dilution of precision

**Environmental Data:**
- `int_temperature_C`: Internal temperature in Celsius
- `ext_temperature_C`: External temperature in Celsius
- `light`: Light sensor reading
- `altimeter_m`: Altimeter reading in meters
- `depth_m`: Depth reading in meters
- `conductivity_mS/cm`: Water conductivity

**Movement Data:**
- `speed_km/h`: Speed in kilometers per hour
- `direction_deg`: Direction in degrees

**Sensor Data:**
- `mag_x`, `mag_y`, `mag_z`: Magnetometer readings
- `acc_x`, `acc_y`, `acc_z`: Accelerometer readings

**Output Structure:**
Each observation contains:
```json
{
  "file": "telemetry_data.csv",
  "observation_id": "226976_2025-01-18_09:10:11",
  "timestamp": "2025-01-18 09:10:11",
  "device_id": "226976",
  "device_name": "GF_BAR_2022_ADU_W_IMA_Gaulé",
  "location": {
    "lat": 44.394531250000000,
    "lon": 5.370184421539307,
    "altitude": null
  },
  "movement": {
    "speed": null,
    "direction": null
  },
  "device_status": {
    "battery_voltage": 3702.0,
    "battery_soc": 8.0,
    "satellite_count": 3
  },
  "sensor_readings": [
    {
      "timestamp": "2025-01-18 09:10:12",
      "datatype": "SEN_ALL_20Hz_START",
      "environmental": {...},
      "sensors": {...}
    }
  ],
  "sensor_count": 2,
  "additional": {...}
}
```

**Key Features:**
- **Grouped Data**: GPS location + associated sensor readings in single observation
- **Unique IDs**: Each observation has a unique identifier
- **Sensor Arrays**: Multiple sensor readings grouped with their GPS location
- **Structured Format**: Logical grouping of location, movement, device status, and sensor data

## File Storage Structure

The GCS action runner stores files directly under the configured bucket path without integration ID subdirectories:

```
your-gcs-bucket/
└── telemetry-data/           # bucket_path from configuration
    ├── device001_SMS_20240101.csv
    ├── device001_GPRS_20240101.csv
    ├── device002_SMS_20240102.csv
    └── archive/              # Archived files
        ├── device001_SMS_20240101.csv
        └── device002_SMS_20240102.csv
```

**Key Points:**
- Files are stored directly under the `bucket_path` (e.g., `telemetry-data/`)
- No integration ID subdirectories are created
- Archive files are moved to `archive/` subdirectory
- File names preserve their original format (Device ID + SMS/GPRS + timestamp)

## State Management

The action maintains state in Redis to track:
- Files that have been processed
- Files that have been archived
- Last run timestamp and statistics

## Error Handling

- Individual file processing errors are logged but don't stop the overall process
- GCS connection and authentication errors are handled gracefully
- All errors are logged with appropriate detail levels

## Usage Example

```python
# Configuration example
config = PullActionConfiguration(
    bucket_path="raw-data/2024",
    archive_days=30,
    delete_after_archive_days=90
)
```

## Testing

Run the tests with:
```bash
pytest app/actions/tests/test_gcs_handler.py -v
```

## Dependencies

- `google-cloud-storage>=2.10.0`
- `google-auth>=2.23.0`

Make sure to install these dependencies:
```bash
pip install google-cloud-storage google-auth
```
