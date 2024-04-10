import requests
from bs4 import BeautifulSoup
import os
import concurrent.futures

#Get href values for download func

url = 'https://dados.rfb.gov.br/CNPJ/'

#Lists for estabelecimentos anchors and download url's
urls = []
estabelecimentos = []

#Lists for support files anchors and download url's
urls_support = []
support_anchor = []

#List of words to find in anchors
support = ['motivos','municipios','paises']

content = requests.get(url)

soup = BeautifulSoup(content.text,'html')


#Download function
def download_file(url, filename):
    response = requests.get(url)
    if response.status_code == 200:
        with open(f'{filename}', 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully as '{filename}'")
    else:
        print(f"Failed to download file from '{url}'")


#Get estabelecimento href value
for ancor in soup.find_all('a'):
    if 'estabelecimentos' in ancor.text.lower():
        estabelecimentos.append(ancor['href'])
    else:
        pass

#get urls to download files simultenously
for i in estabelecimentos:
    url = f'https://dados.rfb.gov.br/CNPJ/{i}'
    urls.append(url)

#Get support files anchors 
for anchor in soup.find_all('a'):
    for sup in support:
        if sup in anchor.text.lower():
            support_anchor.append(anchor['href'])
        else:
            pass

#Get support files urls
for i in support_anchor:
    url = f'https://dados.rfb.gov.br/CNPJ/{i}'
    urls_support.append(url)

#Name and directory for estabelecimentos files
filenames_estabele = ['data/raw/'+estabe.lower() for estabe in estabelecimentos]

#Name and directory for support file
filenames_support = ['data/raw/support/' + sup + '.zip' for sup in support]

#Define number of threads
num_threads_estabe = min(len(urls),10)
num_threads_supp = min(len(urls),3)

# Download files using multiple threads
with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads_estabe) as executor:
    executor.map(download_file, urls, filenames_estabele)    


with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads_supp) as executor:
    executor.map(download_file, urls_support, filenames_support)


