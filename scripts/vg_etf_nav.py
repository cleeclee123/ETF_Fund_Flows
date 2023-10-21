import requests
import pandas as pd
import http
import aiohttp
import asyncio
import webbrowser
import os
import urllib.parse
import bs4
from lxml import etree
from datetime import datetime, timedelta, date
from typing import Tuple, List


def vg_get_headers(auth: str, path: str, referer: str, cj: http.cookiejar = None):
    cookie_str = ""
    if cj:
        webbrowser.open("https://advisors.vanguard.com/advisors-home")
        webbrowser.open("https://investor.vanguard.com/home")
        cookies = {
            cookie.name: cookie.value for cookie in cj if "vangaurd" in cookie.domain
        }
        cookie_str = "; ".join([f"{key}={value}" for key, value in cookies.items()])
        os.system("taskkill /im chrome.exe /f")

    headers = {
        "authority": auth,
        "method": "GET",
        "path": path,
        "scheme": "https",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7,application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Cookie": cookie_str,
        "Dnt": "1",
        "Referer": referer,
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
    if not cj:
        del headers["Cookie"]

    return headers


def vg_ticker_to_ticker_id(ticker: str, cj: http.cookiejar = None):
    try:
        url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{ticker}/profile"
        headers = vg_get_headers(
            "investor.vanguard.com",
            f"/investment-products/etfs/profile/api/{ticker}/profile",
            url,
            cj,
        )
        res = requests.get(url, headers=headers)
        json = res.json()
        return json["fundProfile"]["fundId"]
    except Exception as e:
        print(e)
        return None


def vg_get_pcf(ticker: str, cj: http.cookiejar = None) -> pd.DataFrame:
    ticker_id = vg_ticker_to_ticker_id(ticker, cj)
    if not ticker_id:
        return pd.DataFrame()

    try:
        url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{ticker_id}/portfolio-holding/pcf"
        headers = vg_get_headers(
            "investor.vanguard.com",
            f"/investment-products/etfs/profile/api/{ticker_id}/portfolio-holding/pcf",
            url,
            cj,
        )
        res = requests.get(url, headers=headers)
        holdings = res.json()["holding"]
        return pd.DataFrame(holdings)
    except Exception as e:
        print(e)
        return pd.DataFrame()


def vg_get_etf_inception_date(
    ticker: str, cj: http.cookiejar = None, fund_info_dataset_path: str = None
) -> str:
    if fund_info_dataset_path:
        df = pd.read_excel(fund_info_dataset_path)
        fund_row = df[(df["ticker"] == f"{ticker}")]
        inception_date = datetime.strptime(
            str(fund_row["inception"].iloc[0]).split("T")[0], "%Y-%m-%d"
        )
        return inception_date.strftime("%m-%d-%Y")

    try:
        url = f"https://investor.vanguard.com/investment-products/etfs/profile/api/{ticker}/profile"
        headers = vg_get_headers(
            "investor.vanguard.com",
            f"/investment-products/etfs/profile/api/{ticker}/profile",
            url,
            cj,
        )
        res = requests.get(url, headers=headers)
        json = res.json()
        inception_date = datetime.strptime(
            str(json["fundProfile"]["inceptionDate"]).split("T")[0], "%Y-%m-%d"
        )
        return inception_date.strftime("%m-%d-%Y")

    except Exception as e:
        print(e)
        return None


def create_12_month_periods(
    start_date_str: date, end_date_str: date
) -> List[List[str]]:
    start_date = datetime.strptime(start_date_str, "%m-%d-%Y")
    end_date = datetime.strptime(end_date_str, "%m-%d-%Y")

    periods = []
    current_period_start = start_date
    while current_period_start < end_date:
        current_period_end = current_period_start + timedelta(days=365)
        if current_period_end > end_date:
            current_period_end = end_date
        periods.append(
            [
                current_period_start.strftime("%m-%d-%Y"),
                current_period_end.strftime("%m-%d-%Y"),
            ]
        )
        current_period_start += timedelta(days=365)

    return periods


def is_valid_date(date_string, format_string="%m-%d-%Y"):
    try:
        datetime.strptime(date_string, format_string)
        return True
    except ValueError:
        return False


def vg_get_historical_nav_prices(
    ticker: str, raw_path: str = None, cj: http.cookiejar = None
):
    async def fetch(session: aiohttp.ClientSession, url: str) -> pd.DataFrame:
        try:
            referer = url.split(".com")[1]
            headers = vg_get_headers("personal.vanguard.com", referer, url, cj)
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    html_string = await response.text()
                    soup = bs4.BeautifulSoup(html_string, "html.parser")

                    def vg_table_html_filter_class(tag):
                        return (
                            tag.name == "tr"
                            and ("class" in tag.attrs)
                            and ("wr" in tag["class"] or "ar" in tag["class"])
                        )

                    tbody = soup.findAll("table")[2].findAll(vg_table_html_filter_class)
                    list = []
                    for row in tbody:
                        try:
                            cols = row.find_all("td")
                            if len(cols) >= 2:
                                date = cols[0].get_text(strip=True)
                                price = cols[1].get_text(strip=True).replace(",", "")

                                if "$" not in str(price) or not is_valid_date(
                                    str(date).replace("/", "-")
                                ):
                                    continue
                                
                                list.append({"date": date, "navPrice": price})
                        
                        except Exception as e:
                            print(e)
                            continue
                    
                    return list

                else:
                    raise Exception(f"Bad Status: {response.status}")
        except Exception as e:
            print(e)
            return []

    async def get_promises(
        session: aiohttp.ClientSession,
        fund_id: int,
        inception_date_str: str,
        today_date_str: str,
    ):
        targets = create_12_month_periods(inception_date_str, today_date_str)
        tasks = []
        for date in targets:
            begin, end = [urllib.parse.quote_plus(x) for x in date]
            curr_url = f"https://personal.vanguard.com/us/funds/tools/pricehistorysearch?radio=1&results=get&FundType=ExchangeTradedShares&FundIntExt=INT&FundId={fund_id}&fundName=0930&radiobutton2=1&beginDate={begin}&endDate={end}&year=#res"

            print(begin, end, curr_url)

            task = fetch(session, curr_url)
            tasks.append(task)

        return await asyncio.gather(*tasks)

    async def run_fetch_all(
        fund_id: int,
        inception_date_str: str,
        today_date_str: str,
    ):
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(
                session, fund_id, inception_date_str, today_date_str
            )
            return all_data

    fund_id = vg_ticker_to_ticker_id(ticker, cj)
    inception_date_str = vg_get_etf_inception_date(ticker, cj, None)
    today_date_str = datetime.today().strftime("%m-%d-%Y")
    list_of_lists = asyncio.run(
        run_fetch_all(fund_id, inception_date_str, today_date_str)
    )
    flat = [item for sublist in list_of_lists for item in sublist]

    if raw_path:
        df = pd.DataFrame(flat)
        df.to_excel(
            f"{raw_path}/{ticker}_{today_date_str}_all_nav_prices.xlsx", index=False
        )

    return flat


if __name__ == "__main__":
    ticker = "EDV"
    # df = vg_get_pcf(ticker)
    # print(df)

    # print('shares: ', df['shareQuantity'].sum())
    # print('value: ', df['adjMarketValue'].sum())

    # inception_date = vg_get_etf_inception_date(
    #     ticker, None, r"C:\Users\chris\ETF_Fund_Flows\data\other\2023-10-08_vg_fund_info.xlsx"
    # )

    raw_path = r"C:\Users\chris\ETF_Fund_Flows\data\other"
    ll = vg_get_historical_nav_prices(ticker, raw_path)
    print(ll)
