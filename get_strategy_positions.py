import datetime

import pandas as pd
from selenium import webdriver
#import undetected_chromedriver
import time
from assets import assets_dict
from selenium.webdriver import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait


def get_strategy_opened_positions(strategy_url):
    options = webdriver.ChromeOptions()
    options.add_argument("--user-data-dir=D:\\forex_copytrader\\user1")

    driver = webdriver.Chrome(options=options)
    driver.get(strategy_url)
    #driver.get("https://google.com")

    try:
        WebDriverWait(driver, 300).until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "._dr._a._d._e._bl._ch._cf._cg")))
    except:
        driver.quit()
        return pd.DataFrame()

    rows = driver.find_elements(By.CSS_SELECTOR, "._d._ez._gi._db._fa._fb._fc._fd._ew._es._er._bl._fe")

    for row in rows:
        ActionChains(driver).move_to_element(row).perform()
        time.sleep(0.5)
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "._d._ez._gi._db._fa._fb._fc._fd._ew._es._er._bl._fe")[-1]
    except:
        return pd.DataFrame()
    try:
        if rows.text.split("Positions\n")[1][0] != "0":
            rows = rows.find_element(By.CSS_SELECTOR, "._a._d._e._bt._eh._ei._ej._ek._bl").find_element(By.CSS_SELECTOR, "._bs._a._d._bl._e").find_elements(By.XPATH, '''//div[@data-test-id="table-row"]''')
    except:
        ids = []
        dates = []
        symbols = []
        sides = []
        df = pd.DataFrame.from_dict({"On Site ID": ids, "Date": dates, "Symbol": symbols, "Side": sides})
        return df

    ids = []
    dates = []
    symbols = []
    sides = []
    #print(rows.text)
    try:
        if len(rows) != 0:
            for row in rows:
                row = row.text.split("\n")
                if row[2] in assets_dict.keys():
                    ids.append(row[0])
                    date_str = row[1][:-5]
                    dates.append(datetime.datetime.strptime(date_str, "%d %b %Y %H:%M:%S"))
                    #print(row)
                    symbols.append(row[2])
                    sides.append(row[4])
    except:
        ids = []
        dates = []
        symbols = []
        sides = []
        df = pd.DataFrame.from_dict({"On Site ID": ids, "Date": dates, "Symbol": symbols, "Side": sides})
        return df

    df = pd.DataFrame.from_dict({"On Site ID": ids, "Date": dates, "Symbol": symbols, "Side": sides})
    return df

#df = get_strategy_opened_positions("https://ct-eu.icmarkets.com/copy/strategy/26217")
#print(df)
#https://ct-eu.icmarkets.com/copy/strategy/40847
#df = get_strategy_opened_positions("https://ct-eu.icmarkets.com/copy/strategy/40847")
#print(df)
#for d in df.iterrows():
#    print(datetime.datetime.timestamp(d[1]["Date"]))