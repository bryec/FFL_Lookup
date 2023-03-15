import sqlite3
import os
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

# Connect to the database (or create it if it doesn't exist)
conn = sqlite3.connect("address_database.db")
cursor = conn.cursor()

# Create the table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS stores (
    LIC_REGN INTEGER,
    LIC_DIST INTEGER,
    LIC_CNTY INTEGER,
    LIC_TYPE TEXT,
    LIC_XPRDTE TEXT,
    LIC_SEQN INTEGER,
    LICENSE_NAME TEXT,
    BUSINESS_NAME TEXT,
    PREMISE_STREET TEXT,
    PREMISE_CITY TEXT,
    PREMISE_STATE TEXT,
    PREMISE_ZIP_CODE TEXT,
    MAIL_STREET TEXT,
    MAIL_CITY TEXT,
    MAIL_STATE TEXT,
    MAIL_ZIP_CODE TEXT,
    VOICE_PHONE TEXT,
    LATITUDE REAL,
    LONGITUDE REAL,
    PRIMARY KEY (LIC_REGN, LIC_SEQN)
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
def update_database(data):
    for row in data:
        # Check if the entry already exists
        cursor.execute("SELECT * FROM stores WHERE LIC_REGN=? AND LIC_SEQN=?", (row[0], row[5]))
        existing_entry = cursor.fetchone()

        # Perform geolocation lookup only if the entry is new or has a different address
        if not existing_entry or existing_entry[8:12] != tuple(row[8:12]):
            lat, lng = geocode_address(", ".join(row[8:12]))
        else:
            lat, lng = existing_entry[-2], existing_entry[-1]

        # Update or insert the entry
        cursor.execute("""
        INSERT OR REPLACE INTO stores (
            LIC_REGN, LIC_DIST, LIC_CNTY, LIC_TYPE, LIC_XPRDTE, LIC_SEQN, LICENSE_NAME, BUSINESS_NAME,
            PREMISE_STREET, PREMISE_CITY, PREMISE_STATE, PREMISE_ZIP_CODE,
            MAIL_STREET, MAIL_CITY, MAIL_STATE, MAIL_ZIP_CODE, VOICE_PHONE, LATITUDE, LONGITUDE
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row + [lat, lng])

    conn.commit()

# Call this function every month to update the database
def update_monthly():
    file_path = "monthly_data.txt"  # Replace with the actual file path
    data = read_file(file_path)
    update_database(data)

# Update the database for the first time
update_monthly()

# Close the connection
conn.close()

