import chromedriver_autoinstaller
import humanize
import pandas as pd

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from src.constant import *
from src.config import TEST_RECIPIENTS
from src.utils.email import Email
from src.utils.logger import logger

URL = 'https://www.nyse.com/ipo-center/filings'
XPATH = '/html/body/div[1]/div[4]/div[1]/div/div/div[7]/div/table[1]/tbody/tr[{}]/td[{}]'
COLUMNS = [DATE, ISSUER, TICKER, SECTOR, BOOKRUNNER, EXCHANGE, MARKET_CAP, OUTSTANDING_SHARES, PRICE_RANGE]


def get_driver() -> webdriver.Chrome:
    chromedriver_autoinstaller.install()
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('window-size=1024x1366')  # iPad Pro
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                         'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36')
    return webdriver.Chrome(options=options)


def fetch(driver: webdriver.Chrome) -> pd.DataFrame:
    row = 1
    data = []
    while True:
        try:
            entry = []
            for col in range(len(COLUMNS)):
                xpath = XPATH.format(row, col + 1)
                entry.append(driver.find_element_by_xpath(xpath).text)
            data.append(entry)
            logger.info(f'Fetched: {entry}')
            row += 1
        except NoSuchElementException:
            break
    return pd.DataFrame(data, columns=COLUMNS)


def report(df: pd.DataFrame) -> None:
    email = Email(subject='Upcoming IPOs', recipients=TEST_RECIPIENTS)
    df[MARKET_CAP] = df[MARKET_CAP].str.replace(',', '').astype(float)
    tech = df[df.sector.isin([TECHNOLOGY, FINANCIALS, CONSUMER_SERVICES])]\
        .sort_values(by=MARKET_CAP, ascending=False)\
        .reset_index(drop=True)
    tech[MARKET_CAP] = tech[MARKET_CAP].map(humanize.intword)
    email.add_dataframe(tech, 'Upcoming Tech IPOs')
    email.send()


def run():
    driver = get_driver()
    driver.implicitly_wait(10)  # seconds
    driver.get(URL)
    df = fetch(driver)
    report(df)
