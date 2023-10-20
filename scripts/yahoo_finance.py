import pandas as pd
import requests
import asyncio
import http
import aiohttp
import webbrowser
import os
import browser_cookie3
import time
from datetime import date, datetime
from typing import Tuple, List


def is_downloadable(url):
    h = requests.head(url, allow_redirects=True)
    header = h.headers
    content_type = header.get("content-type")
    if "text" in content_type.lower():
        return False
    if "html" in content_type.lower():
        return False
    return True


def get_yahoofinance_download_auth(
    ticker: str, from_sec: int, to_sec: int, cj: http.cookiejar = None
) -> Tuple[dict, str]:
    cookie_str = ""
    if cj:
        cookies = {
            cookie.name: cookie.value for cookie in cj if "yahoo" in cookie.domain
        }
        cookies["thamba"] = 2
        cookies["gpp"] = "DBAA"
        cookies["gpp_sid"] = "-1"
        cookie_str = "; ".join([f"{key}={value}" for key, value in cookies.items()])

    headers = {
        "authority": "query1.finance.yahoo.com",
        "method": "GET",
        "path": f"/v7/finance/download/{ticker}?period1={from_sec}&amp;period2={to_sec}&amp;interval=1d&amp;events=history&amp;includeAdjustedClose=true",
        "scheme": "https",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Cookie": cookie_str,
        "Dnt": "1",
        "Sec-Ch-Ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    }
    if not cj:
        del headers["Cookie"]

    crumb_url = "https://query2.finance.yahoo.com/v1/test/getcrumb"
    res = requests.get(crumb_url, headers=headers)
    crumb = res.text

    return headers, crumb


def download_historical_data_yahoofinance(
    ticker: str,
    from_date: date,
    to_date: date,
    raw_path: str,
    cj: http.cookiejar = None,
):
    from_sec = round(from_date.timestamp())
    to_sec = round(to_date.timestamp())
    base_url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={from_sec}&period2={to_sec}&interval=1d&events=history&includeAdjustedClose=true"

    webbrowser.open(base_url)
    headers, crumb = get_yahoofinance_download_auth(ticker, from_sec, to_sec, cj)
    os.system("taskkill /im chrome.exe /f")

    other_options = "&formatted=false&region=US&lang=en-US"
    res_url = f"{base_url}&crumb={crumb}{other_options}"

    response = requests.get(res_url, headers=headers, allow_redirects=True)
    full_file_path = os.path.join(raw_path, f"{ticker}_yahoofin_historical_data.csv")
    if response.status_code == 200:
        with open(full_file_path, mode="wb") as file:
            chunk_size = 10 * 1024
            for chunk in response.iter_content(chunk_size=chunk_size):
                file.write(chunk)

        df = pd.read_csv(full_file_path)
        df.to_excel(f"{full_file_path.split('.')[0]}.xlsx")
        os.remove(full_file_path)

        return df


def multi_download_historical_data_yahoofinance(
    tickers: List[str],
    from_date: date,
    to_date: date,
    raw_path: str,
    cj: http.cookiejar = None,
    big_wb=False,
):
    from_sec = round(from_date.timestamp())
    to_sec = round(to_date.timestamp())

    async def fetch(
        session: aiohttp.ClientSession, url: str, curr_ticker: str
    ) -> pd.DataFrame:
        try:
            webbrowser.open(url)
            headers, crumb = get_yahoofinance_download_auth(
                curr_ticker, from_sec, to_sec, cj
            )

            other_options = "&formatted=false&region=US&lang=en-US"
            res_url = f"{url}&crumb={crumb}{other_options}"

            full_file_path = os.path.join(
                raw_path, f"{curr_ticker}_yahoofin_historical_data.csv"
            )
            async with session.get(res_url, headers=headers) as response:
                if response.status == 200:
                    with open(full_file_path, "wb") as f:
                        chunk_size = 10 * 1024
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

        df = pd.read_csv(full_file_path)
        df.to_excel(f"{full_file_path.split('.')[0]}.xlsx", index=False)
        os.remove(full_file_path)

        return f"{full_file_path.split('.')[0]}.xlsx"

    async def get_promises(session: aiohttp.ClientSession):
        tasks = []
        for ticker in tickers:
            curr_url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={from_sec}&period2={to_sec}&interval=1d&events=history&includeAdjustedClose=true"
            task = fetch(session, curr_url, ticker)
            tasks.append(task)

        return await asyncio.gather(*tasks)

    async def run_fetch_all() -> List[pd.DataFrame]:
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session)
            return all_data

    os.mkdir(f"{raw_path}/temp")
    dfs = asyncio.run(run_fetch_all())
    print(dfs)
    os.rmdir(f"{raw_path}/temp")
    os.system("taskkill /im chrome.exe /f")

    if big_wb:
        tickers_str = str.join("_", [str(x) for x in tickers])
        wb_file_name = f"{raw_path}\{tickers_str}_yahoofin_historical_data.xlsx"
        with pd.ExcelWriter(wb_file_name) as writer:
            for i, df in enumerate(dfs, 0):
                try:
                    df.drop("Unnamed: 0", axis=1, inplace=True)
                except:
                    pass
                df.to_excel(writer, sheet_name=f"{tickers[i]}", index=False)

    return True


if __name__ == "__main__":
    start = time.time()

    cj = browser_cookie3.chrome()
    tickers = ["EDV", "TLT", "ZROZ"]
    from_date = datetime(2023, 1, 1)
    to_date = datetime.today()
    raw_path = r"C:\Users\chris\ETF_Fund_Flows\data\yahoofin"

    multi_download_historical_data_yahoofinance(
        tickers, from_date, to_date, raw_path, cj, True
    )

    end = time.time()
    print(f"Time Elapsed: {end - start} s")
