import pandas as pd
import requests
import gzip
import shutil
import tempfile
import uuid
import json
from base64 import b64decode
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile
import json


def read_multi_file_in_gzip(AccessToken, data_service_url, temp_pass):
    """
    If you receive a ZIP file via an API with the content type application/gzip,
    containing various files, you can use this script to extract the data.
    To ensure error-free extraction, it’s recommended to extract the file on your hard disk by creating a temporary folder with a random name. Once the process is complete, the folder can be safely deleted.
    Keep in mind that you might need to make some modifications to the file.
    Feel free to share your issues with me!

    :param AccessToken: AccessToken
    :param data_service_url: url_service
    :param temp_pass: main_temp_dir
    :return:dictionary of file dataframes
    """
    headers = {
        'Authorization': f'Bearer {AccessToken}'
    }

    response = requests.get(data_service_url, headers=headers, allow_redirects=True)

    data = response.content
    contents = json.loads(data)['File']
    bio = BytesIO(b64decode(contents))

    temp_dir = tempfile.mkdtemp(dir=temp_pass, prefix=f"tmp_{uuid.uuid4()}_")
    path = Path(temp_dir)

    with ZipFile(bio) as zip_file:
        zip_file.extractall(path)
        zip_file.close()

    data_read_dict = {}
    for p in path.iterdir():
        with gzip.open(p, 'rb') as zip_file:

            zz = zip_file.readlines()
            decoded_string = zz[0].decode("utf-8")
            try:
                df = pd.read_json(decoded_string, dtype=str)

            except:
                import json

                data_dict = json.loads(decoded_string)
                a = 10
                df = pd.DataFrame(data_dict.values(), dtype=str, index=data_dict.keys())
                df = df.transpose()

            data_read_dict[p.as_posix()] = df

    shutil.rmtree(temp_dir)
    return data_read_dict


if __name__ == "__main__":
    AccessToken = ''
    data_service_url = 'sample.com/api'
    temp_pass = 'temp_data'
    read_multi_file_in_gzip(AccessToken, data_service_url, temp_pass)
