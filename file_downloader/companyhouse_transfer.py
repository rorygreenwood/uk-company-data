import logging
import bs4
import requests
import requests as r
import datetime

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s: %(message)s')


def collect_companieshouse_file(firstdateofmonth, filename):
    logger.info(f'set date as {firstdateofmonth}')
    logger.info('collecting ch_file')
    logger.info(f'set filename as {filename}')
    baseurl = 'http://download.companieshouse.gov.uk/' + filename
    logger.info(f'sending request using url: {baseurl}')
    req = r.get(baseurl, stream=True, verify=False)
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
    logger.info('search called')
    initial_req_url = 'http://download.companieshouse.gov.uk/en_output.html'
    r = requests.get(initial_req_url, verify=False)
    # filename = 'BasicCompanyDataAsOneFile-' + str(firstdateofmonth) + '.zip'
    r_content = r.content
    rsoup = bs4.BeautifulSoup(r_content, 'html.parser')
    links = rsoup.find_all('a')
    # check links on product page for the current date string from firstdateofmonth
    logger.info(links)
    str_month = firstdateofmonth.strftime('%Y-%m')
    logger.info(str_month)
    filename = False
    for link in links:
        while not filename:
            if str_month in link.text:
                logger.info(f'new file found: {link.text}')
                logger.info(link)
                filename = link['href']
                logger.info(f'filename: {filename}')
                file_link = link['href']
                logger.info(f'filelink: {file_link}')
                filename, firstdateofmonth = collect_companieshouse_file(firstdateofmonth, file_link)
                break
    return filename, firstdateofmonth
