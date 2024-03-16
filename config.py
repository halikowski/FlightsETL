import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

# list of destination countries, customizable
destination_countries = ['Spain', 'Italy', 'Greece', 'Croatia', 'United Kingdom', 'Norway',
                         'Germany', 'Netherlands', 'Portugal', 'Malta']
# Departure airport for flight search (IATA city/airport code) - customizable
ORIGIN_CITY_IATA = 'WRO'

# Date-time values for flight search
NOW = datetime.now()
TOMORROW = (NOW + timedelta(days=1)).strftime("%d/%m/%Y")
IN_6MONTHS = (NOW + timedelta(days=180)).strftime("%d/%m/%Y")

# API data and output values to display
API_KEY = os.getenv('API_KEY')
flight_search_endpoint = "https://api.tequila.kiwi.com/v2/search"
destination_code_endpoint = "https://api.tequila.kiwi.com/locations/query"
headers = {
            "apikey": API_KEY
        }
parameters = {
            "fly_from": ORIGIN_CITY_IATA,
            'fly_to': '',
            "date_from": TOMORROW,
            "date_to": IN_6MONTHS,
            "nights_in_dst_from": 1,
            "nights_in_dst_to": 14,
            "ret_from_diff_city": "false",
            "ret_to_diff_city": "false",
            "adults": 1,
            "children": 0,
            "max_stopovers": 0,
            "limit": 10000,
            "curr": "PLN",
            'sort': 'date'
        }

return_flight_fields = ['utc_departure', 'utc_arrival', 'fare_category', 'airline']
main_fields = ['flyFrom', 'cityFrom', 'flyTo', 'cityTo', 'utc_departure', 'utc_arrival', 'price', 'nightsInDest',
               'distance']