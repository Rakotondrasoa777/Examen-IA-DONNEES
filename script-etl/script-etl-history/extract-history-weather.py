import os
import requests
import pandas as pd
from datetime import datetime
import logging

#Pour extraire les donnes meteoroligique 5 ans en arriere
def extract_meteo_history(city: str, api_key: str, date: str) -> bool:
    try:
        #conversion de la date en annee pour determiner la date d'extraction
        end_year = datetime.strptime(date, "%Y-%m-%d").year
        start_year = end_year - 4 #On remonte 4 ans en arriere 

        all_records = []

        # Boucle sur chaque année dans la plage demandée
        for year in range(start_year, end_year + 1):
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
            url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{start_date}/{end_date}?unitGroup=metric&key={api_key}&include=days&contentType=json"

            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            # Traitement de chaque jour de données météo
            for day in data.get("days", []):
                record = {
                    'ville': city,
                    'date_extraction': datetime.now().strftime("%Y-%m-%d"),
                    'date_observation': day['datetime'],
                    'temperature': day.get('temp'),
                    'temp_min': day.get('tempmin'),
                    'temp_max': day.get('tempmax'),
                    'humidite': day.get('humidity'),
                    'pression': day.get('pressure'),
                    'vent_vitesse': day.get('windspeed'),
                    'vent_direction': day.get('winddir'),
                    'precipitation': day.get('precip'),
                    'description': day.get('conditions'),
                    'latitude': data.get('latitude'),
                    'longitude': data.get('longitude')
                }
                all_records.append(record)

        os.makedirs(f"/home/tojo/airflow/data/history", exist_ok=True)

        pd.DataFrame(all_records).to_csv(
            f"/home/tojo/airflow/data/history/historical_meteo_{city}.csv",
            index=False
        )

        return True

    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur API Visual Crossing pour {city}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue Visual Crossing pour {city}: {str(e)}")

    return False

#extraire manuellement les donnees
if __name__ == "__main__":
    CITIES = ['Sydney']
    api_key = os.environ.get('WEATHER_API_KEY_HISTORY')

    if not api_key:
        raise ValueError("La clé d'API n'est pas définie dans la variable d'environnement WEATHER_API_KEY_HISTORY")

    for city in CITIES:
        success = extract_meteo_history(city, api_key, '2025-07-01')
        if success:
            print(f"Extraction réussie pour {city}")
        else:
            print(f"Échec de l'extraction pour {city}")