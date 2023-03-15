import mysql.connector
import os
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

# Connect to the database (or create it if it doesn't exist)
conn = mysql.connector.connect(
    host="localhost",
    user="ffl",
    password="ffladdresslookup",
    database="FFL_Address"
)
cursor = conn.cursor()

# Create the table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS store_data (
    LIC_REGN VARCHAR(255),
    LIC_DIST VARCHAR(255),
    LIC_CNTY VARCHAR(255),
    LIC_TYPE VARCHAR(255),
    LIC_XPRDTE VARCHAR(255),
    LIC_SEQN VARCHAR(255) PRIMARY KEY,
    LICENSE_NAME VARCHAR(255),
    BUSINESS_NAME VARCHAR(255),
    PREMISE_STREET VARCHAR(255),
    PREMISE_CITY VARCHAR(255),
    PREMISE_STATE VARCHAR(255),
    PREMISE_ZIP_CODE VARCHAR(255),
    MAIL_STREET VARCHAR(255),
    MAIL_CITY VARCHAR(255),
    MAIL_STATE VARCHAR(255),
    MAIL_ZIP_CODE VARCHAR(255),
    VOICE_PHONE VARCHAR(255),
    LATITUDE DOUBLE,
    LONGITUDE DOUBLE
)

""")

# Read the provided text file
def read_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        data = [line.strip().split('\t') for line in lines[1:]]  # Skip the header line
    return data

# Get geolocation data for an address
def geocode_address(address, retries=3, timeout=5):
    geolocator = Nominatim(user_agent="geoapiExercises")

    for attempt in range(retries):
        try:
            location = geolocator.geocode(address, timeout=timeout)
            if location:
                return location.latitude, location.longitude
        except (GeocoderTimedOut, GeocoderUnavailable):
            if attempt < retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            else:
                raise

    return None, None

# Update the database with the data from the file
import mysql.connector

def update_database(data):
    last_column_index = 17  # Replace this with the correct index for the last column in your data
    for row in data:
        # Geocode the address
        lat, lng = geocode_address(", ".join(row[8:12]))

        try:
            cursor.execute("""
                INSERT INTO store_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                LIC_NAME=?,
                LIC_ISSU_DATE=?,
                LIC_EXP_DATE=?,
                MAIL_LINE1=?,
                MAIL_LINE2=?,
                MAIL_CITY=?,
                MAIL_STATE=?,
                MAIL_ZIP=?,
                PREMISE_LINE1=?,
                PREMISE_LINE2=?,
                PREMISE_CITY=?,
                PREMISE_STATE=?,
                PREMISE_ZIP=?,
                LAT=?,
                LNG=?,
                BUSINESS_PHONE=?,
                VOICE_PHONE=?
            """, (*row, lat, lng, *row[0:17], row[last_column_index]))

            # Commit the changes
            mariadb_connection.commit()

            # Print the successful entry
            print(f"Successfully added entry with LIC_SEQN: {row[5]}")

        except mysql.connector.Error as e:
            print(f"Error: {e}")
            mariadb_connection.rollback()


# Call this function every month to update the database
def update_monthly():
    file_path = "monthly_data.txt"  # Replace with the actual file path
    data = read_file(file_path)
    update_database(data)

# Update the database for the first time
update_monthly()

# Close the connection
conn.close()

