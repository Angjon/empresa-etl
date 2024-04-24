import json
import polars as pl

# Download function
def download_file(url, filename):
    import requests

    response = requests.get(url)
    if response.status_code == 200:
        with open(f"{filename}", "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully as '{filename}'")
    else:
        print(f"Failed to download file from '{url}'")

# Retrive json data function
def get_json_file(doc):
    with open(f'airflow/dags/config/{doc}', 'r') as file:
        data_list = json.load(file)
        return data_list
    
# Create folder if does not exist function
def _make_folder(path) -> None:
    import os
    
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        pass

# Support function to handle dtypes in the silver layer
def set_dtypes(dtype_aux):
    dtypes = {}
    for key,value in dtype_aux.items():
        dtypes.update({key:eval(value)})
    return dtypes