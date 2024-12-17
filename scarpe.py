import time
from datetime import datetime, timezone
import logging
import csv
import requests
import pandas as pd
from bs4 import BeautifulSoup


base_url = f"https://www.carwow.de/ratgeber/elektroauto/lieferzeiten-elektroautos#gref"
job_name = "Web Scraping using requests/selenium"
headers = {
    'sec-ch-ua-platform': '"Windows"',
    'Referer': 'https://www.carwow.de/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
    'sec-ch-ua-mobile': '?0',
}

params = {
    'hasCsp': 'true',
    'href': 'https://www.carwow.de/ratgeber/elektroauto/lieferzeiten-elektroautos',
    'account_id': '145',
}
output_filename = (
    job_name.split("using")[0].strip().lower().replace(" ", "-") + "-sample.csv"
)
scrape_datetime = datetime.now(timezone.utc)


def retry_on_failure(func):
    def wrapper(*args, **kwargs):
        MAX_ATTEMPTS = 3
        attempts = MAX_ATTEMPTS
        while attempts > 0:
            try:
                logging.info(f"Requesting URL: {args[1]}")
                response = func(*args, **kwargs)
                if response.status_code == 200:
                    return response
                else:
                    logging.error(
                        f"Request failed. Retrying {MAX_ATTEMPTS - attempts + 1}/{MAX_ATTEMPTS} attempts."
                    )
                    attempts -= 1
                    time.sleep(10)
            except Exception as e:
                logging.error(
                    f"An error occurred - Exception: {e}. Retrying {MAX_ATTEMPTS - attempts + 1}/{MAX_ATTEMPTS} attempts."
                )
                attempts -= 1
                time.sleep(10)
        logging.warning(f"All attempts failed. Unable to make successful request. URL: {args[1]}")
        return None
    return wrapper


class Scraper:
    def __init__(self):
        self.MASTER_LIST = []
        self.CLIENT = None
        self.DEBUG = False
        if self.DEBUG:
            logging.basicConfig(
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                level=logging.DEBUG,
                datefmt="%d-%b-%y %H:%M:%S",
            )
        else:
            logging.basicConfig(
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                level=logging.INFO,
                datefmt="%d-%b-%y %H:%M:%S",
            )

        logging.info(f"STARTING SCRAPE... {job_name}")
        time.sleep(2)

    def make_session(self, headers=None):
        s = requests.Session()
        if headers is not None:
            s.headers.update(headers)

        return s

    @retry_on_failure
    def make_request(self, url,params, method="GET", data=None):
        res = None
        if method == "GET":
            res = self.CLIENT.get(url,params=params, timeout=60)
        elif (method == "POST") and (data is not None):
            res = self.CLIENT.post(url, params=params, data=data, timeout=60)
        return res

    def scrape_data(self, base_url: str):
        logging.info(f"PROCESSING PAGE: {base_url}")
        self.CLIENT = self.make_session(headers)
        param= params
        response = self.make_request(base_url,param)
        if response :
            soup = BeautifulSoup(response.text, "html.parser")
            all_data_scrape = soup.find_all("tr")
            for item in all_data_scrape:
                row_data_dic ={}
                if item == all_data_scrape[0]:
                    as_of_date = item.text[27:37].split(".")[::-1]
                    as_of_date ="/".join(as_of_date)
                else:
                    tds = item.find_all('td')
                    multi_model = tds[0].text.strip().splitlines()
                    model = " ".join(multi_model)
                    murl = tds[0].find("a")["href"]
                    data_url = f'{base_url[:21]}{murl}'
                    multi_lead_time = tds[1].text.strip().splitlines()
                    lead_time = " ".join(multi_lead_time)
                    row_data_dic["scrape_datetime"] = scrape_datetime
                    row_data_dic["data_url"] = data_url
                    row_data_dic["as_of_date"] = as_of_date
                    row_data_dic["model"] = model
                    row_data_dic["lead_time"] = lead_time
                    self.MASTER_LIST.append(row_data_dic)
        else:
            print("no response")

    def start_scraper(self):
        page_url = f"{base_url}"
        self.scrape_data(page_url)


def run(filename: str):
    scraper = Scraper()
    scraper.start_scraper()
    results = scraper.MASTER_LIST
    if len(results) < 1:
        logging.error("NO DATA SCRAPED. EXITING...")
        return

    df = pd.DataFrame(results)
    logging.info("GENERATING FINAL OUTPUT...")
    df.to_csv(
        filename,
        encoding="utf-8",
        quotechar='"',
        quoting=csv.QUOTE_ALL,
        index=False,
    )


if __name__ == "__main__":
    run(filename=output_filename)
    logging.info("ALL DONE")
