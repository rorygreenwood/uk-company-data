import logging
import bs4
import requests
import requests as r
import datetime

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')


# https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2023-04-01.zip
# https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2023-01-01.zip
def collect_companieshouse_file(firstdateofmonth, filename):
    logger.info(f'set date as {firstdateofmonth}')
    print('calling collect_companieshouse_file')
    logger.info(f'set filename as {filename}')
    baseurl = 'https://download.companieshouse.gov.uk/' + filename
    logger.info(f'sending request using url: {baseurl}')
    req = r.get(baseurl, stream=True)
    logger.info('downloading file')
    with open('file_downloader/files/' + filename, 'wb') as fd:
        chunkcount = 0
        for chunk in req.iter_content(chunk_size=100000):
            chunkcount += 1
            fd.write(chunk)
    return filename, firstdateofmonth


def search_and_collect_ch_file(firstdateofmonth: datetime.date):
    """
    checks for presence of datestring on db and then page, downloads if new filestring, and then processes
    :param firstdateofmonth:
    :return: filename, firstdateofmonth
    """
    print('search called')
    initial_req_url = 'http://download.companieshouse.gov.uk/en_output.html'
    r = requests.get(initial_req_url)
    # filename = 'BasicCompanyDataAsOneFile-' + str(firstdateofmonth) + '.zip'
    r_content = r.content
    rsoup = bs4.BeautifulSoup(r_content, 'html.parser')
    links = rsoup.find_all('a')
    # check links on product page for the current date string from firstdateofmonth
    print(links)
    str_month = firstdateofmonth.strftime('%Y-%m')
    print(str_month)

    for link in links:
        if str_month in link.text:
            print(f'new file found: {link.text}')
            print(link)
            filename = link['href']
            print(f'filename: {filename}')
            file_link = link['href']
            print(f'filelink: {file_link}')
            filename, firstdateofmonth = collect_companieshouse_file(firstdateofmonth, file_link)
            break
    return filename, firstdateofmonth

