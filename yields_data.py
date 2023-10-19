import os
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException
from typing import List, Union, Dict
import http
import aiohttp
import asyncio


def latest_download_file(path):
    os.chdir(path)
    files = sorted(os.listdir(os.getcwd()), key=os.path.getmtime)
    if len(files) == 0:
        return "Empty Directory"
    newest = files[-1]

    return newest


def download_daily_treasury_par_yield_curve_rates(raw_path: str, year: int):
    url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value={year}"

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--ignore-certificate-errors")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    prefs = {
        "profile.default_content_settings.popups": 0,
        "download.default_directory": raw_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
    }
    options.add_experimental_option("prefs", prefs)

    try:
        with webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()), options=options
        ) as driver:
            driver.get(url)
            full_url = driver.current_url + "#portfolio"
            driver.get(full_url)

            download_button_xpath = "/html/body/div[1]/div/div[3]/div/section/div/div/div[2]/div/div/div[1]/div/a"
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, download_button_xpath))
            )
            driver.find_element(By.XPATH, download_button_xpath).click()

            data_as_of_date_xpath = "/html/body/div[1]/div/div[3]/div/section/div/div/div[2]/div/div/div[4]/div[1]/div[2]/div/p[1]"
            as_of_date_raw = driver.find_element(By.XPATH, data_as_of_date_xpath).text
            format_str = "%A %b %d, %Y"
            date_obj = datetime.strptime(as_of_date_raw, format_str)
            formatted_date_str = date_obj.strftime("%m-%d-%Y")

            time.sleep(2)

            output_file_name = latest_download_file(raw_path)
            renamed_output_file = f"{formatted_date_str}_daily_treasury_rates"
            df_temp = pd.read_csv(f"{raw_path}\{output_file_name}")
            df_temp.to_excel(f"{renamed_output_file}.xlsx", index=False)

            os.remove(f"{raw_path}\{output_file_name}")

            driver.quit()

    except WebDriverException:
        print("Web Driver Failed to Start")
        os.system("taskkill /im chromedriver.exe")


def get_treasurygov_header(year: int, cj: http.cookiejar = None):
    cookie_str = ""
    if cj:
        cookies = {
            cookie.name: cookie.value
            for cookie in cj
            if "home.treasury.gov" in cookie.domain
        }
        cookie_str = "; ".join([f"{key}={value}" for key, value in cookies.items()])

    headers = {
        "authority": "home.treasury.gov",
        "method": "GET",
        "path": f"/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value={year}",
        "scheme": "https",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Cookie": cookie_str,
        "Dnt": "1",
        "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    }

    if (cookie_str == ""):
        del headers["Cookie"]

    return headers


def download_multi_year_treasury_par_yield_curve_rate(
    years: List[int], raw_path: str, cj: http.cookiejar = None
):
    async def fetch(
        session: aiohttp.ClientSession, url: str, curr_year: int
    ) -> pd.DataFrame:
        try:
            headers = get_treasurygov_header(curr_year, cj)
            curr_file_name = f"{curr_year}_daily_treasury_rates"
            full_file_path = os.path.join(raw_path, "temp", f"{curr_file_name}.csv")
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    with open(full_file_path, "wb") as f:
                        chunk_size = 8192
                        while True:
                            chunk = await response.content.read(chunk_size)
                            if not chunk:
                                break
                            f.write(chunk)

                    renamed = await convert_csv_to_excel(full_file_path)
                    return pd.read_excel(renamed)
                else:
                    raise Exception(f"Bad Status: {response.status}")
        except Exception as e:
            print(e)
            return pd.DataFrame()

    async def convert_csv_to_excel(full_file_path: str or None) -> str:
        if not full_file_path:
            return

        copy = full_file_path
        rdir_path = copy.split("\\")
        rdir_path.remove("temp")
        renamed = str.join("\\", rdir_path)
        renamed = f"{renamed.split('.')[0]}.xlsx"

        df_temp = pd.read_csv(full_file_path)
        df_temp.to_excel(f"{renamed.split('.')[0]}.xlsx", index=False)
        os.remove(full_file_path)

        return renamed

    async def get_promises(session: aiohttp.ClientSession):
        tasks = []
        for year in years:
            curr_url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{year}/all?type=daily_treasury_yield_curve&amp;field_tdr_date_value={year}&amp;page&amp;_format=csv"
            task = fetch(session, curr_url, year)
            tasks.append(task)

        return await asyncio.gather(*tasks)

    async def run_fetch_all() -> List[pd.DataFrame]:
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session)
            return all_data

    os.mkdir(f"{raw_path}/temp")
    dfs = asyncio.run(run_fetch_all())
    os.rmdir(f"{raw_path}/temp")

    yield_df = pd.concat(dfs, ignore_index=True)
    years_str = str.join("_", [str(x) for x in years])
    yield_df.to_excel(f"{years_str}_daily_treasury_rates.xlsx", index=False)

    return yield_df


if __name__ == "__main__":
    start = time.time()

    raw_path = r"C:\Users\chris\ETF_Fund_Flows\data"
    df = download_multi_year_treasury_par_yield_curve_rate([2023, 2022, 2021, 2020, 2019], raw_path)
    print(df)

    end = time.time()
    print(f"Time Elapsed: {end - start} s")
