from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable, GeocoderTimedOut

geolocator = Nominatim(user_agent="my_ffl_lookup")

def geocode_address(address):
    try:
        location = geolocator.geocode(address)
        return (location.latitude, location.longitude) if location else (None, None)
    except (GeocoderUnavailable, GeocoderTimedOut) as e:
        print(f"Error: {e}")
        return (None, None)

if __name__ == "__main__":
    test_address = "4 CALLE COLON, AGUADA, PR, 00602"
    lat, lng = geocode_address(test_address)
    print(f"Latitude: {lat}, Longitude: {lng}")

