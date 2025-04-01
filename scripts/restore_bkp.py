# scripts/restore_bkp.py

import os
import subprocess
import psycopg2
import requests
import tempfile

def download_bkp_from_s3(s3_url, output_file):
    """
    Downloads a .bkp file from an S3 URL and saves it to the specified output path.
    
    Args:
        s3_url (str): The S3 URL from which to download the backup file.
        output_file (str): The local file path where the backup will be saved.
    """
    try:
        # Stream the content to handle large files efficiently.
        with requests.get(s3_url, stream=True) as r:
            r.raise_for_status()
            with open(output_file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"✅ Successfully downloaded backup from S3 to {output_file}")
    except Exception as e:
        print(f"❌ Error downloading backup from S3: {e}")
        raise

def restore_bkp_to_csv(bkp_path, db_name="shipments_db", db_user="gauravlohkna", host="localhost", port="5432"):
    """
    Restores a .bkp PostgreSQL dump file and exports tables to CSV.
    
    The CSV files are stored in the local repository's data/ folder.
    
    Args:
        bkp_path (str): Path to the .bkp file.
        db_name (str): Database name.
        db_user (str): PostgreSQL username.
        host (str): PostgreSQL host.
        port (str): PostgreSQL port.
    
    Returns:
        dict: A dictionary of table names and their corresponding CSV file paths.
    """
    # Ensure PostgreSQL is running locally.
    try:
        subprocess.run(["pg_isready", "-h", host, "-p", port], check=True)
    except subprocess.CalledProcessError:
        print("❌ PostgreSQL is not running. Start it with: brew services start postgresql@14")
        return

    # Create the database if it doesn't exist.
    subprocess.run(["createdb", "-h", host, "-U", db_user, db_name], stderr=subprocess.DEVNULL)

    # Restore the .bkp file.
    restore_command = [
        "pg_restore", "--verbose", "--clean", "--no-acl", "--no-owner",
        "-h", host, "-U", db_user, "-d", db_name, bkp_path
    ]
    subprocess.run(restore_command, check=True)
    print(f"✅ Successfully restored {bkp_path} to {db_name}.")

    # Define the output directory as a folder named "data" within the repo.
    output_dir = os.path.join(os.path.dirname(__file__), "../data")
    os.makedirs(output_dir, exist_ok=True)

    # Connect to PostgreSQL and fetch table names.
    conn = psycopg2.connect(dbname=db_name, user=db_user, host=host, port=port)
    cursor = conn.cursor()
    cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'shipments';")
    tables = [table[0] for table in cursor.fetchall()]
    
    # Export each table to a CSV file in the data folder.
    csv_files = {}
    for table in tables:
        csv_path = os.path.join(output_dir, f"{table}.csv")
        query = f"COPY shipments.{table} TO STDOUT WITH CSV HEADER"
        with open(csv_path, "w") as f:
            cursor.copy_expert(query, f)
        csv_files[table] = csv_path
        print(f"📁 Exported {table} to {csv_path}")

    cursor.close()
    conn.close()
    
    return csv_files

if __name__ == "__main__":
    # S3 link for the .bkp file.
    s3_url = "https://crayon-aicoe-case-studies.s3.eu-central-1.amazonaws.com/dpe/data-engineer/shipments_schema.bkp"
    
    # Download the backup file to a temporary location.
    temp_dir = tempfile.gettempdir()
    local_bkp_path = os.path.join(temp_dir, "shipments_schema.bkp")
    download_bkp_from_s3(s3_url, local_bkp_path)
    
    # Restore the downloaded backup and export tables to CSV in the local "data" folder.
    csv_files = restore_bkp_to_csv(local_bkp_path)
    print("\n✅ CSV Export Complete:", csv_files)
