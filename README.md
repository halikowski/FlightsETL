An ETL pipeline project for flights data from Wrocław,Poland (WRO / EPWR) airport to many chosen European countries.
Both departure airport and destination countries list are fully customizable in config.py file.

The script exctracts data from the Kiwi.com/Tequila API. Fetched data contains departure airport/city, destination airport/city, flight price, distance, airline, dates and much more.
After cleaning and transforming, three different Data Frames are given:
- clean_flights_df - main data frame containing mentioned flight informations
- avg_df - data frame with destinations and aggregated data (mean price, price per km)
- airlines_df - data frame with airlines data like airline IATA code, airline full name and origin country
All of them are later loaded/exported as .csv files

Moreover, Apache Airflow implementation is added, allowing for scheduling and automating this pipeline.

Repository contains example output files.

Installation guide:

pip install -r requirements.txt
