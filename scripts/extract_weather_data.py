import requests 
import os
# import json 
import sys
from datetime import date, timedelta
import pandas as pd 

# no need for this, runs in a container 
# loadENV = load_dotenv()
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')

if not WEATHER_API_KEY:
    print('Could not load ENV file. Exiting...')
    sys.exit(1) 




# ! because of python versioning in docker container, we 
# ! to use f' {""} ' pattern within f strings. 


today = date.today()
today_minus_1 = date.today() - timedelta(days=1)
today_minus_30 = date.today() - timedelta(days=30)


us_cities = [
    {"name": "Montgomery", "lat": 32.3667, "lon": -86.3},
    {"name": "Juneau", "lat": 58.3019, "lon": -134.4197},
    {"name": "Phoenix", "lat": 33.4483, "lon": -112.0733},
    {"name": "Little Rock", "lat": 34.7464, "lon": -92.2894},
    {"name": "Sacramento", "lat": 38.5817, "lon": -121.4933},
    {"name": "Denver", "lat": 39.7392, "lon": -104.9842},
    {"name": "Hartford", "lat": 41.7636, "lon": -72.6856},
    {"name": "Dover", "lat": 39.1581, "lon": -75.5247},
    {"name": "Tallahassee", "lat": 30.4381, "lon": -84.2808},
    {"name": "Atlanta", "lat": 33.7489, "lon": -84.3881},
    {"name": "Honolulu", "lat": 21.3069, "lon": -157.8583},
    {"name": "Boise", "lat": 43.6136, "lon": -116.2025},
    {"name": "Springfield", "lat": 42.1014, "lon": -72.5903},
    {"name": "Indianapolis", "lat": 39.7683, "lon": -86.1581},
    {"name": "Des Moines", "lat": 41.6006, "lon": -93.6089},
    {"name": "Topeka", "lat": 39.0483, "lon": -95.6778},
    {"name": "Baton Rouge", "lat": 30.4506, "lon": -91.1544},
    {"name": "Annapolis", "lat": 38.9783, "lon": -76.4925},
    {"name": "Boston", "lat": 42.3583, "lon": -71.0603},
    {"name": "Lansing", "lat": 42.7325, "lon": -84.5556},
    {"name": "Saint Paul", "lat": 44.9444, "lon": -93.0931},
    {"name": "Jacksonville", "lat": 30.3319, "lon": -81.6558},
    {"name": "Jefferson City", "lat": 38.5767, "lon": -92.1733},
    {"name": "Helena", "lat": 46.5928, "lon": -112.0353},
    {"name": "Lincoln", "lat": 40.8, "lon": -96.6667},
    {"name": "Carson City", "lat": 39.1639, "lon": -119.7664},
    {"name": "Trenton", "lat": 40.2169, "lon": -74.7433},
    {"name": "Albany", "lat": 42.6525, "lon": -73.7567},
    {"name": "Raleigh", "lat": 35.7719, "lon": -78.6389},
    {"name": "Bismarck", "lat": 46.8083, "lon": -100.7833},
    {"name": "Columbus", "lat": 39.9611, "lon": -82.9989},
    {"name": "Oklahoma City", "lat": 35.4675, "lon": -97.5161},
    {"name": "Harrisburg", "lat": 40.2736, "lon": -76.8847},
    {"name": "Providence", "lat": 41.8239, "lon": -71.4133},
    {"name": "Columbia", "lat": 34.0006, "lon": -81.035},
    {"name": "Nashville", "lat": 36.1658, "lon": -86.7844},
    {"name": "Austin", "lat": 30.2669, "lon": -97.7428},
    {"name": "Salt Lake City", "lat": 40.7608, "lon": -111.8903},
    {"name": "Montpelier", "lat": 44.26, "lon": -72.5758},
    {"name": "Richmond", "lat": 37.5536, "lon": -77.4606},
    {"name": "Olympia", "lat": 47.0381, "lon": -122.8994},
    {"name": "Charleston", "lat": 32.7764, "lon": -79.9311},
    {"name": "Madison", "lat": 43.0731, "lon": -89.4011},
    {"name": "Cheyenne", "lat": 41.14, "lon": -104.8197},
    {"name": "Frankfort", "lat": 38.2009, "lon": -84.8733},  # Kentucky
    {"name": "Augusta", "lat": 44.3106, "lon": -69.7795},    # Maine
    {"name": "Concord", "lat": 43.2081, "lon": -71.5376},    # New Hampshire
    {"name": "Santa Fe", "lat": 35.6870, "lon": -105.9378},  # New Mexico
    {"name": "Salem", "lat": 44.9429, "lon": -123.0351},     # Oregon
    {"name": "Pierre", "lat": 44.3683, "lon": -100.3510}
]

# for pandas prototyping 
# dataframeObj = {
#     'city_name': [],
#     'region': [],
#     'lat': [],
#     'lon': [],
#     'date_str': [], 
#     'maxtemp_f': [],
#     'mintemp_f': [],
#     'maxwind_mph': [],
#     'totalprecip_in': [],
#     'avghumidity': []
# }


for city in us_cities:
    
    try:
        weather_url = f'https://api.weatherapi.com/v1/history.json?key={WEATHER_API_KEY}&q={city["lat"]},{city["lon"]}&dt={today_minus_30}&end_dt={today_minus_1}'
        response = requests.get(weather_url)
        json = response.text

        with open(f'/opt/airflow/data/json/{today}_{city["name"]}.json', 'w') as f:
            f.write(json)

            

    except Exception as e: 
        print('Error with fetching data, please try again')
        print(e)
        sys.exit(1)