def extract_data(save_path):
    import os
    import zipfile
    from utils.util import _make_folder
    from airflow.models import Variable
    
    RAW_PATH = Variable.get('raw_path_var')
    BRONZE_PATH = Variable.get('bronze_path_var')
    DOMAIN_PATH = Variable.get('domain_path_var')

    #Files in raw folder
    files_esta = os.listdir(RAW_PATH)
    files_sup = os.listdir(f'{RAW_PATH}/support')

    # Create folder if it does not exists
    _make_folder(f'{BRONZE_PATH}/extract')
    _make_folder(f'{BRONZE_PATH}/support')


    #extract each file and dump into extract folder
    for file in files_esta:
        if file.endswith('.zip'):
            file_name = file.split('.')[0]
            file_path = os.path.join(RAW_PATH,file)
            with zipfile.ZipFile(file_path,'r') as zip_ref:
                zip_ref.extractall(f'{BRONZE_PATH}/extract/{file_name}')
            print('Done')

    #extract each file and dump into extract folder
    for file in files_sup:
        if file.endswith('.zip'):
            file_name = file.split('.')[0]
            file_path = os.path.join(f'{RAW_PATH}/support',file)
            with zipfile.ZipFile(file_path,'r') as zip_ref:
                zip_ref.extractall(f'{BRONZE_PATH}/support/{file_name}')
            print('Done')