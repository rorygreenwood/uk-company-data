import datetime
import json
import os
import zipfile

import requests
from filesplit.split import Split


def date_check(file_date: datetime.date, cursor):
    cursor.execute("select * from BasicCompanyData_filetracker where ch_upload_date = %s", (file_date,))
    res = cursor.fetchall()
    if len(res) > 0:
        print('exists')
        return True
    else:
        print('does not exist')
        return False


def unzip_ch_file(file_name):
    filepath = f'file_downloader/files/{file_name}'
    output_directory = 'file_downloader/files'
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(output_directory)
    os.remove(f'file_downloader/files/{file_name}')
    return file_name.replace('.zip', '.csv')


def fragment_ch_file(file_name):
    split = Split(file_name, 'file_downloader/files/fragments/')
    split.bylinecount(linecount=50000, includeheader=True)
    os.remove('file_downloader/files/fragments/manifest')


def pipeline_messenger(title, text, hexcolour):
    url = "https://tdworldwide.webhook.office.com/webhookb2/d5d1f4d1-2858-48a6-8156-5abf78a31f9b@7fe14ab6-8f5d-4139-84bf-cd8aed0ee6b9/IncomingWebhook/76b5bd9cd81946338da47e0349ba909d/c5995f3f-7ce7-4f13-8dba-0b4a7fc2c546"
    payload = json.dumps({
        "@type": "MessageCard",
        "themeColor": hexcolour,
        "title": title,
        "text": text,
        "markdown": True
    })
    headers = {
        'Content-Type': 'application/json'
    }
    requests.request("POST", url, headers=headers, data=payload)
