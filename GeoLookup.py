import mysql.connector
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable

# Geocoder setup
geolocator = Nominatim(user_agent="your_user_agent")

# Database connection
db = mysql.connector.connect(
    host="localhost",
    user="ffl",
    password="ffladdresslookup",
    database="FFL_Address"
)
cursor = db.cursor()

def geocode_address(address):
    try:
        location = geolocator.geocode(address)
        return (location.latitude, location.longitude) if location else (None, None)
    except GeocoderUnavailable:
        return (None, None)

def update_geocode_data():
    cursor.execute("SELECT * FROM store_data WHERE LATITUDE IS NULL OR LONGITUDE IS NULL")
    rows = cursor.fetchall()

    for row in rows:
        lat, lng = geocode_address(", ".join(row[8:12]))

        if lat and lng:
            cursor.execute("""
                UPDATE store_data
                SET LATITUDE=%s, LONGITUDE=%s
                WHERE LIC_SEQN=%s
                """, (lat, lng, row[0]))

            db.commit()
            print(f"Updated geolocation for {row[1]}")
        else:
            print(f"Failed to geocode {row[1]}")

if __name__ == "__main__":
    update_geocode_data()
