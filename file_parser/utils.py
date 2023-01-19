import logging
import os
import zipfile

from filesplit.split import Split


def unzip_ch_file(file_name):
    filepath = f'../file_downloader/files/{file_name}'
    output_directory = '../file_downloader/files'
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(output_directory)
    os.remove('../file_downloader/files/BasicCompanyDataAsOneFile-2023-01-01.zip')
    return file_name.replace('.zip', '.csv')


def fragment_ch_file(file_name):
    split = Split(file_name, '../file_downloader/files/fragments/')
    split.bylinecount(linecount=50000, includeheader=True)
    os.remove('../file_downloader/files/fragments/manifest')
