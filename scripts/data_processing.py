# scripts/data_processing.py

import os
import pandas as pd
import logging

# Setup logging.
log_file = os.path.join(os.path.dirname(__file__), '../logs/app.log')
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_data():
    """
    Loads shipments and weather data from CSV files.
    In production, data would be read from S3 or a data warehouse.
    
    Returns:
        tuple: DataFrames for shipments and weather data.
    """
    try:
        shipments_csv = os.path.join(os.path.dirname(__file__), '../data/shipments.csv')
        # Use the cities weather data file generated from the weather collection script.
        weather_csv = os.path.join(os.path.dirname(__file__), '../data/cities_weather_data.csv')
        
        shipments_df = pd.read_csv(shipments_csv)
        weather_df = pd.read_csv(weather_csv)
        
        logging.info("Loaded shipments data: %d records", len(shipments_df))
        logging.info("Loaded weather data: %d records", len(weather_df))
        return shipments_df, weather_df
    except Exception as e:
        logging.error("Error loading data: %s", e)
        return None, None

def process_data(shipments_df, weather_df):
    """
    Joins shipments data with weather data and computes average fuel consumption metrics.
    
    Assumptions & Approach:
      - We assume shipments data contains a 'start_location' column which corresponds to the 'city'
        column in the weather data.
      - Instead of using fixed temperature ranges, we use data-driven bins. This is achieved by
        dividing the 'temperature_celsius' values into quantiles (quartiles in this example). This 
        approach automatically adjusts the bin boundaries based on the data distribution.
    
    Returns:
        DataFrame: Aggregated metrics by quantile-based temperature range.
    """
    try:
        # Merge shipments and weather data on the city name (shipments.start_location == weather.city)
        logging.info("Starting merge process: shipments (%d) vs weather (%d)",
                     len(shipments_df), len(weather_df))
        merged_df = pd.merge(
            shipments_df,
            weather_df,
            left_on='start_location',
            right_on='city',
            how='left'
        )
        logging.info("Merged DataFrame shape: %s", merged_df.shape)
        logging.info("Sample merged records:\n%s", merged_df.head().to_string())

        # Check if temperature data is available.
        if 'temperature_celsius' not in merged_df.columns:
            logging.error("Temperature data ('temperature_celsius') is missing in merged data.")
            return None

        # Use data-driven bins (quantiles) for the temperature.
        # Here, we divide the temperature data into 4 quantile-based bins.
        # This approach automatically sets the boundaries based on the data distribution.
        try:
            merged_df['temp_range'], bins = pd.qcut(
                merged_df['temperature_celsius'], q=4, retbins=True, duplicates='drop'
            )
            logging.info("Quantile-based bins for temperature: %s", bins)
            logging.info("Temperature bucket counts:\n%s", merged_df['temp_range'].value_counts())
        except Exception as e:
            logging.error("Error creating quantile-based bins: %s", e)
            return None

        # Compute fuel consumption (liters per km) for each shipment.
        if 'consumed_fuel' not in merged_df.columns or 'shipment_distance' not in merged_df.columns:
            logging.error("Required columns for fuel consumption ('consumed_fuel' and 'shipment_distance') are missing.")
            return None
        
        merged_df['fuel_consumption'] = merged_df['consumed_fuel'] / merged_df['shipment_distance']
        logging.info("Calculated fuel consumption for merged data.")

        # Group by the data-driven temperature bins and calculate average fuel consumption.
        agg_df = merged_df.groupby('temp_range')['fuel_consumption'].mean().reset_index()
        agg_df.rename(columns={'fuel_consumption': 'avg_fuel_consumption'}, inplace=True)
        logging.info("Aggregated metrics by temperature range:\n%s", agg_df.to_string(index=False))
        
        return agg_df
    except Exception as e:
        logging.error("Error processing data: %s", e)
        return None

def save_aggregated_data(agg_df, output_file):
    """
    Saves the aggregated metrics to a CSV file.
    
    Args:
        agg_df (DataFrame): Aggregated metrics.
        output_file (str): Output file path.
    """
    try:
        agg_df.to_csv(output_file, index=False)
        logging.info("Aggregated metrics saved to %s", output_file)
    except Exception as e:
        logging.error("Error saving aggregated data: %s", e)

def main():
    """
    Main function to run the data processing pipeline.
    Loads shipments and weather data, processes the data, and saves aggregated metrics.
    """
    shipments_df, weather_df = load_data()
    if shipments_df is None or weather_df is None:
        print("Failed to load data. Check logs for details.")
        return

    agg_df = process_data(shipments_df, weather_df)
    if agg_df is not None:
        output_csv = os.path.join(os.path.dirname(__file__), '../data/aggregated_metrics.csv')
        save_aggregated_data(agg_df, output_csv)
        print(f"Data processing complete. Aggregated metrics saved to {output_csv}")
    else:
        print("Data processing failed. Check logs for details.")

if __name__ == "__main__":
    main()
