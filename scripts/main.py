import time
import browser_cookie3
import http
from multiprocessing import Process
from typing import List
from datetime import date, datetime

from fund_flows import (
    fetch_new_bearer_token,
    multi_fetch_fund_flow_data,
)
from vg_etf_nav import vg_get_historical_nav_prices
from yahoo_finance import multi_download_historical_data_yahoofinance
from yields_data import download_multi_year_treasury_par_yield_curve_rate


def run_in_parallel(*fns):
    proc = []
    for fn, args in fns:
        p = Process(target=fn, args=args)
        p.start()
        proc.append(p)
    for p in proc:
        p.join()


def fund_flow_wrapper(
    tickers: List[str],
    date_from: date,
    date_to: date,
    raw_path: str,
    cj: http.cookiejar = None,
):
    try:
        token = fetch_new_bearer_token(cj)
        bearer = token["fundApiKey"]
    except Exception as e:
        bearer = "0QE2aa6trhK3hOmkf5zXwz6Riy7UWdk4V6HYw3UdZcRZV3myoV9MOfwNLL6FKHrpTN7IF7g12GSZ6r44jAfjte0B3APAaQdWRWZtW2qhYJrAXXwkpYJDFdkCng97prr7N4JAXkCI1zB7EiXrFEY8CIQclMLgQk2XHBZJiqJSIEgtWckHK3UPLfm12X9rhME9ac7gvcF3fWDo8A66X6RHXr3g9jzKeC62th75S1t6juvWjQYDCz65i7UlRfTVWDVV"
        print(f"Fund Flow bearer token requeat failed: {str(e)}")

    data = multi_fetch_fund_flow_data(tickers, bearer, date_from, date_to, raw_path)
    return data


if __name__ == "__main__":
    start = time.time()

    cj = browser_cookie3.chrome()
    tickers = ["VGIT", "VGLT"]
    years = [2023, 2022, 2021, 2020, 2019]
    date_from = datetime(2023, 1, 1)
    date_to = datetime.today()

    raw_path_flow = r"C:\Users\chris\ETF_Fund_Flows\data\flow"
    raw_path_yahoo = r"C:\Users\chris\ETF_Fund_Flows\data\yahoofin"
    raw_path_treasury = r"C:\Users\chris\ETF_Fund_Flows\data\treasury"

    # run_in_parallel(
    #     (fund_flow_wrapper, (tickers, date_from, date_to, raw_path_flow, cj)),
    #     (
    #         multi_download_historical_data_yahoofinance,
    #         (tickers, date_from, date_to, raw_path_yahoo, cj, True),
    #     ),
    #     (
    #         download_multi_year_treasury_par_yield_curve_rate,
    #         (years, raw_path_treasury, cj),
    #     ),
    # )
    
    fund_flow_wrapper(tickers, date_from, date_to, raw_path_flow, cj)
    multi_download_historical_data_yahoofinance(tickers, date_from, date_to, raw_path_yahoo, cj, True)
    download_multi_year_treasury_par_yield_curve_rate(years, raw_path_treasury, cj)

    end = time.time()
    print(f"Time Elapsed: {end - start} s")
