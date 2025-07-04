import pandas as pd

#transformation des donnees historiques
historique = pd.read_csv("/home/tojo/airflow/data/processed/history_meteo_global.csv")
dim_ville = pd.read_csv("/home/tojo/airflow/data/star_schema/dim_ville.csv")

historique = historique.merge(dim_ville, on="ville", how="left")

historique = historique.drop(columns=["ville", "latitude", "longitude"], errors="ignore")

colonnes_finales = ['date_observation', 'temperature', 'temp_min', 'temp_max', 'humidite',
                    'pression', 'vent_vitesse', 'vent_direction', 'precipitation',
                    'description', 'ville_id']
historique = historique[colonnes_finales]

historique.to_csv("/home/tojo/airflow/data/star_schema/fact_weather_history.csv", index=False)


