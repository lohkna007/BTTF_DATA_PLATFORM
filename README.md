# BTTF Logistics Data Platform PoC

This repository contains a Proof-of-Concept (PoC) for the BTTF Logistics Data Platform. The solution demonstrates how to:

- Restore a PostgreSQL backup (.bkp) directly from an S3 URL
- Extract table data and export it to CSV files
- Collect weather data from the Open-Meteo API
- Process and join shipments data with weather data to compute aggregated metrics

## Project Structure

bttf_data_platform/ ├── README.md ├── requirements.txt ├── config/ │ └── config.env # Environment variables (API keys, DB configs) ├── logs/
│ └── app.log # Log file for the application ├── models/ │ └── schema.sql # SQL DDL definitions for the data model └── scripts/ ├── restore_bkp.py # Script to restore a .bkp file from S3 and export CSV files ├── weather_data_collection.py # Script for weather data ingestion and export to CSV └── data_processing.py # Script for joining datasets and computing aggregated metrics


## Prerequisites

- **Python 3.7+**: Ensure Python is installed on your MacBook Air.
- **PostgreSQL**: Install PostgreSQL and ensure it is running locally.  
  *On macOS, you can install it via Homebrew:*  
  ```bash
  brew install postgresql@14
  brew services start postgresql@14
- VSCode: Recommended IDE for code development.

