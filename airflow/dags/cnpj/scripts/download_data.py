def download_data(save_path):
    from bs4 import BeautifulSoup
    import requests
    import os
    import concurrent.futures
    from utils.util import _make_folder
    from airflow.models import Variable
    from utils.util import download_file

    # Getting variable from airflow UI
    RAW_PATH = Variable.get('raw_path_var')
    PATH_SUP = "support"

    url = "https://dados.rfb.gov.br/CNPJ/"

    content = requests.get(url)

    soup = BeautifulSoup(content.text, "html")

    # Lists for estabelecimentos anchors and download url's
    estabelecimentos = [anchor['href'] for anchor in soup.find_all("a") if "estabelecimentos" in anchor.text.lower()]

    urls = [f"https://dados.rfb.gov.br/CNPJ/{url}" for url in estabelecimentos]

    # Lists for support files anchors and download url's
    

    # List of words to find in anchors
    support = ["motivos", "municipios", "paises"]

    support_anchor = [anchor["href"] for anchor in soup.find_all("a") if any(sup in anchor.text.lower() for sup in support)]

    urls_support = [f"https://dados.rfb.gov.br/CNPJ/{url_sup}" for url_sup in support_anchor]

    ##DEBUG

    _make_folder(RAW_PATH)
    _make_folder(RAW_PATH + "/" + PATH_SUP)

    # Name and directory for estabelecimentos files
    filenames_estabele = [RAW_PATH + "/" + estabe.lower() for estabe in estabelecimentos]

    # Name and directory for support file

    filenames_support = [f"{RAW_PATH}/{PATH_SUP}/" + sup + ".zip" for sup in support]

    # Define number of threads
    num_threads_estabe = min(len(urls), 10)
    num_threads_supp = min(len(urls), 3)

    # #Download files using multiple threads
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=num_threads_estabe
    ) as executor:
        executor.map(download_file, urls, filenames_estabele)

    #Download support files
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=num_threads_supp
    ) as executor:
        executor.map(download_file, urls_support, filenames_support)
