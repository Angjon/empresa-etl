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