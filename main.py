from datetime import datetime, timedelta
import requests
import config
import pandas as pd


def get_iata(destination):
        destination_parameters = {
            "term": destination,
            "location_types": "country",
        }
        response = requests.get(url=config.destination_code_endpoint, params=destination_parameters, headers=config.headers)
        destination_data = response.json()
        country_iata = destination_data["locations"][0]["code"]
        return country_iata

destination_codes = [get_iata(destination=country) for country in config.destination_countries]


def get_flights_info(destination):
    flight_parameters = config.parameters
    flight_parameters['flyTo'] = destination
    response = requests.get(url=config.flight_search_endpoint, params=flight_parameters, headers=config.headers)
    search_data = response.json()
    data_list = search_data.get('data',[])
    main_data = [{key: record[key] for key in config.main_fields} for record in data_list]
    return_data = [{key.replace('utc_', 'return_utc_'): record['route'][1][key] for key in config.return_flight_fields} for record in data_list]
    route_data = [{**main_dict, **return_dict} for main_dict, return_dict in zip(main_data, return_data)]
    destination_df = pd.DataFrame(route_data)
    return destination_df

flights_data_dfs = [get_flights_info(destination=country) for country in destination_codes]

raw_flights_data = pd.concat(flights_data_dfs)
print(raw_flights_data.head())
print(raw_flights_data.shape)