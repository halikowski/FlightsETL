import requests
import pandas as pd
from bs4 import BeautifulSoup
import logging
import os
import config

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_iata(destination):
    """ Receives IATA code for each destination provided, for further flight search (e.g. 'Spain' -> 'ES') """
    try:
        destination_parameters = {
            "term": destination,
            "location_types": "country",
        }
        response = requests.get(url=config.destination_code_endpoint, params=destination_parameters,
                                headers=config.headers)
        response.raise_for_status()
        destination_data = response.json()
        country_iata = destination_data["locations"][0]["code"]
        logging.info(f'Successfully obtained IATA code for {destination}')
        return country_iata

    except requests.RequestException as e:
        logging.error(f'Error during request: {e}')
        return None

    except (KeyError, IndexError) as e:
        logging.error(f'Error during extracting data from the response: {e}')
        return None


def get_flights_info(destination):
    """ Executes an API call to get flights information for provided destination,
    according to the parameters set below """
    try:
        flight_parameters = config.parameters
        flight_parameters['fly_to'] = destination
        response = requests.get(url=config.flight_search_endpoint, params=flight_parameters, headers=config.headers)
        response.raise_for_status()
        search_data = response.json()
        data_list = search_data.get('data', [])
        main_data = [{key: record[key] for key in config.main_fields} for record in data_list]
        return_data = [{key.replace('utc_', 'return_utc_'): record['route'][1][key]
                        for key in config.return_flight_fields} for record in data_list]
        route_data = [{**main_dict, **return_dict} for main_dict, return_dict in zip(main_data, return_data)]
        destination_df = pd.DataFrame(route_data)
        logging.info(f'Successfully imported flights data for {destination}')
        return destination_df

    except requests.RequestException as e:
        logging.error(f'Error during request: {e}')
        return None

    except (KeyError, IndexError) as e:
        logging.error(f'Error during extracting data from the response: {e}')
        return None


def cleaning_na_duplicates(df):
    na_values_count = df.isna().sum().sum()
    if na_values_count > 0:
        df.dropna(inplace=True)
        logging.warning(f'{na_values_count} rows with NaN values have been dropped')
    duplicates_count = df.duplicated().sum().sum()
    if duplicates_count > 0:
        df.drop_duplicates(inplace=True)
        logging.warning(f'{duplicates_count} duplicated rows have been dropped')


def avg_destination_price(df):
    """ Returns a dataframe that contains average flight price and other aggregations
     for each destination in provided input """
    avg_df = df.groupby('Dest_City', as_index=False).agg({'price_PLN': lambda x: round(x.mean(), 2)})
    avg_df.rename(columns={'price_PLN': 'avg_price_PLN'}, inplace=True)
    avg_df['avg_cost_per_km'] = avg_df['avg_price_PLN']/df['distance_km']
    return avg_df


def get_airline_data(code):
    """ Takes airline's IATA code (2- or 3-digit) and receives airline name & origin country.
        Subsequently, returns a dictionary containing mentioned airline data. """
    if code.lower() == 'e4':  # Exception for 'E4' code - the right code for this airline is 'E4*'
        airline_data = {'airline_id': code,
                        'airline_name': 'Enter Air Spolka z.o.o.',
                        'airline_country': 'Poland'}
        return airline_data
    else:
        try:
            url = f'https://www.iata.org/en/publications/directories/code-search/?airline.search={code}'
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            data = soup.find('table', class_='datatable').find_all('td')
            airline_data = {'airline_id': code,
                            'airline_name': data[3].text.strip(),
                            'airline_country': data[4].text.strip()}
            logging.info(f'Successfully imported flights data for {code}')
            return airline_data

        except requests.RequestException as e:
            logging.error(f'Error during request: {e}')
            return None

        except (KeyError, IndexError) as e:
            logging.error(f'Error during extracting data from the response: {e}')
            return None


def load(df, filename):
    """ Saves the data frame into .csv file of given name """
    df.to_csv(filename, sep=',')
    if os.path.exists(filename):
        logging.info(f'Successfully created file {filename}')
    else:
        logging.error(f'Failed to create file {filename}')


def extract(destinations):  # destinations = destination_countries
    # Creating a list of destinations' IATA codes
    destination_codes = [get_iata(destination=country) for country in destinations]
    # Creating a list of Data Frames containing flight data for each destination
    flights_data_dfs = [get_flights_info(destination=country) for country in destination_codes]
    raw_flights_data = pd.concat(flights_data_dfs)
    if raw_flights_data.shape[1] != len(config.main_fields) + len(config.return_flight_fields):
        logging.error("Number of columns in raw_flights_data does not match expected fields.")
    return raw_flights_data


def transform(raw_df):
    cleaning_na_duplicates(raw_df)
    clean_df = raw_df.rename(columns={
        'flyFrom': 'Dep_Airport',
        'cityFrom': 'Dep_City',
        'flyTo': 'Dest_Airport',
        'cityTo': 'Dest_City',
        'price': 'price_PLN',
        'nightsInDest': 'nights_in_dest',
        'distance': 'distance_km',
        'airline': 'airline_id'
    })
    clean_df = clean_df.reset_index()

    date_columns = ['utc_departure', 'utc_arrival', 'return_utc_departure', 'return_utc_arrival']
    for col in date_columns:
        clean_df[col] = pd.to_datetime(clean_df[col])
        clean_df[col] = clean_df[col].dt.tz_localize(None)

    # Columns rearrangement
    clean_df = clean_df[
        ['Dep_Airport', 'Dep_City', 'Dest_Airport', 'Dest_City', 'utc_departure', 'utc_arrival', 'return_utc_departure',
         'return_utc_arrival', 'nights_in_dest', 'fare_category', 'price_PLN', 'airline_id', 'distance_km']]

    avg_df = avg_destination_price(clean_df)
    airline_codes = clean_df['airline_id'].unique()
    airline_dfs = [get_airline_data(code=code) for code in airline_codes]
    airlines = pd.DataFrame(airline_dfs)

    return clean_df, avg_df, airlines
