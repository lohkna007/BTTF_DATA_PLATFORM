# scripts/weather_data_collection.py
"""
This script reads the cities data exported from the restored PostgreSQL backup
and fetches historical weather data for each city using the Open‑Meteo Historical Weather API.
The resulting weather data is then saved to a CSV file.

Note:
  • Historical data are available with a delay (typically 5 days). Adjust the target date accordingly.
  • In production on AWS, local CSV file I/O would be replaced with S3 interactions.
"""

import os
import requests
import logging
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv

# Load environment variables.
load_dotenv(os.path.join(os.path.dirname(__file__), '../config/config.env'))

# Setup logging.
log_file = os.path.join(os.path.dirname(__file__), '../logs/app.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Historical Weather API endpoint.
HISTORICAL_API_URL = "https://archive-api.open-meteo.com/v1/archive"

def fetch_historical_weather(lat, lon, date_str):
    """
    Fetch hourly historical weather data for a given location and date.
    
    Args:
        lat (float): Latitude.
        lon (float): Longitude.
        date_str (str): Date (YYYY-MM-DD) for which data is requested.
        
    Returns:
        dict or None: JSON response data or None on error.
    """
    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': date_str,
        'end_date': date_str,
        'hourly': 'temperature_2m,relativehumidity_2m',
        'timezone': 'UTC'
    }
    try:
        response = requests.get(HISTORICAL_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        logging.info("Fetched weather data for (lat: %s, lon: %s) on %s", lat, lon, date_str)
        return data
    except Exception as e:
        logging.error("Error fetching weather data for (lat: %s, lon: %s) on %s: %s", lat, lon, date_str, e)
    return None

def process_weather_data(data, target_hour):
    """
    Process the hourly weather data to extract the record closest to the target hour.
    
    Args:
        data (dict): JSON response from the API.
        target_hour (int): Desired hour (0-23) to select.
        
    Returns:
        dict or None: Processed record with time, temperature, and humidity.
    """
    if not data or 'hourly' not in data:
        logging.error("No hourly data found in the API response.")
        return None

    hourly = data['hourly']
    times = hourly.get('time', [])
    temperatures = hourly.get('temperature_2m', [])
    humidities = hourly.get('relativehumidity_2m', [])

    if not times or not temperatures:
        logging.error("Incomplete hourly data in the API response.")
        return None

    record = None
    for t, temp, hum in zip(times, temperatures, humidities):
        try:
            dt = datetime.strptime(t, "%Y-%m-%dT%H:%M")
        except Exception as e:
            logging.error("Error parsing time '%s': %s", t, e)
            continue
        if dt.hour == target_hour:
            record = {'time': t, 'temperature_celsius': temp, 'relative_humidity': hum}
            break

    if record is None:
        logging.warning("No exact match for target hour %s; using first record.", target_hour)
        record = {'time': times[0], 'temperature_celsius': temperatures[0], 'relative_humidity': humidities[0] if humidities else None}

    return record

def main():
    """
    Reads the cities CSV file, fetches historical weather data for each city,
    and saves the aggregated weather data to a CSV file.
    """
    # The historical date should be set to a day when data are available (e.g., 5 days ago).
    target_date = "2025-03-24"
    target_hour = 12  # e.g., noon

    # Path to the cities CSV file (exported from the backup restore).
    cities_csv_path = os.path.join(os.path.dirname(__file__), "../data/cities.csv")
    
    if not os.path.exists(cities_csv_path):
        logging.error("Cities CSV file not found at %s", cities_csv_path)
        print(f"Cities CSV file not found at {cities_csv_path}")
        return

    try:
        cities_df = pd.read_csv(cities_csv_path)
    except Exception as e:
        logging.error("Error reading cities CSV: %s", e)
        print("Error reading cities CSV.")
        return

    weather_records = []

    for _, row in cities_df.iterrows():
        city = row.get("name")
        lat = row.get("latitude")
        lon = row.get("longitude")
        if pd.isna(lat) or pd.isna(lon):
            logging.warning("Skipping city '%s' due to missing coordinates.", city)
            continue

        logging.info("Fetching weather for %s (lat: %s, lon: %s)", city, lat, lon)
        data = fetch_historical_weather(lat, lon, target_date)
        if data:
            record = process_weather_data(data, target_hour)
            if record:
                record['city'] = city
                record['latitude'] = lat
                record['longitude'] = lon
                record['date'] = target_date
                weather_records.append(record)
                logging.info("Weather record for %s: %s", city, record)
            else:
                logging.error("Failed to process weather data for %s", city)
        else:
            logging.error("Failed to fetch weather data for %s", city)

    # Save the aggregated weather records to CSV in the data folder.
    output_csv = os.path.join(os.path.dirname(__file__), "../data/cities_weather_data.csv")
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    try:
        df_weather = pd.DataFrame(weather_records)
        df_weather.to_csv(output_csv, index=False)
        logging.info("Successfully saved aggregated weather data to %s", output_csv)
        print("Weather data collection complete. Data saved to", output_csv)
    except Exception as e:
        logging.error("Error saving aggregated weather data to CSV: %s", e)
        print("Error saving aggregated weather data to CSV.")

if __name__ == "__main__":
    main()
