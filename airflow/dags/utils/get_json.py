import json

def get_json_file(doc):
    with open(f'airflow/dags/config/{doc}', 'r') as file:
        data_list = json.load(file)
        return data_list