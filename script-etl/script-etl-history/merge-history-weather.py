import pandas as pd
import os

#fusion des donnees historique recueilli par ville
def merge_files_history() -> str:
    """Ajoute les nouvelles données au fichier global existant"""
    input_dir = f"/home/tojo/airflow/data/history"
    output_file = "/home/tojo/airflow/data/processed/history_meteo_global.csv"
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    if os.path.exists(output_file):
        global_df = pd.read_csv(output_file)
    else:
        global_df = pd.DataFrame()
        print(f"{output_file} created!")
    
    new_data = []
    for file in os.listdir(input_dir):
        if file.startswith('historical_meteo_') and file.endswith('.csv'):
            new_data.append(pd.read_csv(f"{input_dir}/{file}"))
    
    if not new_data:
        raise ValueError(f"Aucune nouvelle donnée à fusionner")
    
    updated_df = pd.concat([global_df] + new_data, ignore_index=True)
    updated_df = updated_df.drop_duplicates(
        subset=['ville', 'date_observation'],  
        keep='last'                          
    )
    
    updated_df.to_csv(output_file, index=False)
    return output_file

# execution manuelle 
if __name__ == "__main__":
    merge_files_history()
    print("Fusion terminer!")