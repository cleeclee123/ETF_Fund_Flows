import pandas as pd
import requests
import asyncio
import aiofiles
import http
import aiohttp
import webbrowser
import os
import browser_cookie3
import time
from datetime import date, datetime
from typing import Tuple, List, Dict


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
    path: str, cj: http.cookiejar = None, run_crumb=False
) -> Tuple[dict, str] | dict:
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
        "path": path,
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

    if run_crumb:
        crumb_url = "https://query2.finance.yahoo.com/v1/test/getcrumb"
        res = requests.get(crumb_url, headers=headers)
        crumb = res.text

        return headers, crumb

    return headers


def download_historical_data_yahoofinance(
    ticker: str,
    from_date: date,
    to_date: date,
    raw_path: str,
    cj: http.cookiejar = None,
    open_chrome=False,
):
    from_sec = round(from_date.timestamp())
    to_sec = round(to_date.timestamp())
    base_url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={from_sec}&period2={to_sec}&interval=1d&events=history&includeAdjustedClose=true"

    webbrowser.open(base_url) if open_chrome else None
    _, crumb = get_yahoofinance_download_auth("/v1/test/getcrumb", cj, True)
    headers = get_yahoofinance_download_auth(
        f"/v7/finance/download/{ticker}?period1={from_sec}&amp;period2={to_sec}&amp;interval=1d&amp;events=history&amp;includeAdjustedClose=true&crumb={crumb}&formatted=false&region=US&lang=en-US",
        cj,
    )
    os.system("taskkill /im chrome.exe /f") if open_chrome else None

    res_url = f"{base_url}&crumb={crumb}&formatted=false&region=US&lang=en-US"
    response = requests.get(res_url, headers=headers, allow_redirects=True)
    full_file_path = os.path.join(raw_path, f"{ticker}_yahoofin_historical_data.csv")
    if response.status_code == 200:
        with open(full_file_path, mode="wb") as file:
            chunk_size = 10 * 1024
            for chunk in response.iter_content(chunk_size=chunk_size):
                file.write(chunk)

        df = pd.read_csv(full_file_path)
        df.to_excel(f"{full_file_path.split('.')[0]}.xlsx", index=False)
        os.remove(full_file_path)

        return df


def multi_download_historical_data_yahoofinance(
    tickers: List[str],
    from_date: date,
    to_date: date,
    raw_path: str,
    cj: http.cookiejar = None,
    max_date=False,
    write_to_xlsx=False,
    big_wb=False,
    open_chrome=False,
) -> Dict[str, pd.DataFrame]:
    from_sec = round(from_date.timestamp())
    to_sec = (
        round(to_date.timestamp())
        if not max_date
        else round(datetime.today().timestamp())
    )

    async def fetch(
        session: aiohttp.ClientSession, url: str, curr_ticker: str, crumb: str
    ) -> pd.DataFrame:
        try:
            webbrowser.open(url) if open_chrome else None
            headers = get_yahoofinance_download_auth(
                f"/v7/finance/download/{curr_ticker}?period1={from_sec}&amp;period2={to_sec}&amp;interval=1d&amp;events=history&amp;includeAdjustedClose=true&crumb={crumb}&formatted=false&region=US&lang=en-US",
                cj,
            )
            os.system("taskkill /im chrome.exe /f") if open_chrome else None

            full_file_path = os.path.join(
                raw_path, f"{curr_ticker}_yahoofin_historical_data.csv"
            )
            res_url = f"{url}&crumb={crumb}&formatted=false&region=US&lang=en-US"
            async with session.get(res_url, headers=headers) as response:
                if response.status == 200:
                    # with open(full_file_path, "wb") as f:
                    async with aiofiles.open(full_file_path, mode="wb") as f:
                        chunk_size = 10 * 1024
                        while True:
                            chunk = await response.content.read(chunk_size)
                            if not chunk:
                                break
                            await f.write(chunk)

                    if write_to_xlsx:
                        renamed = await convert_csv_to_excel(full_file_path)
                        return pd.read_excel(renamed)

                    df = pd.read_csv(full_file_path)
                    os.remove(full_file_path)
                    return df

                else:
                    raise Exception(f"Bad Status: {response.status}")

        except Exception as e:
            print(e)
            return pd.DataFrame()

    async def convert_csv_to_excel(full_file_path: str | None) -> str:
        if not full_file_path:
            return

        df = pd.read_csv(full_file_path)
        df.to_excel(f"{full_file_path.split('.')[0]}.xlsx", index=False)
        time.sleep(1)
        os.remove(full_file_path)

        return f"{full_file_path.split('.')[0]}.xlsx"

    async def get_promises(session: aiohttp.ClientSession):
        tasks = []
        _, crumb = get_yahoofinance_download_auth("/v1/test/getcrumb", cj, True)
        for ticker in tickers:
            if max_date:
                try:
                    headers = get_yahoofinance_download_auth(
                        f"v8/finance/chart/{ticker}?formatted=true&crumb={crumb}&lang=en-US&region=US&includeAdjustedClose=true&corsDomain=finance.yahoo.com",
                        cj,
                    )
                    first_trade_date_url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker}?formatted=true&crumb={crumb}&lang=en-US&region=US&includeAdjustedClose=true&corsDomain=finance.yahoo.com"
                    first_trade_date = requests.get(
                        first_trade_date_url, headers=headers
                    ).json()["chart"]["result"][0]["meta"]["firstTradeDate"]
                    from_sec = round(first_trade_date)
                except Exception as e:
                    print("First Trade Date Error", e)

            curr_url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={from_sec}&period2={to_sec}&interval=1d&events=history&includeAdjustedClose=true"
            task = fetch(session, curr_url, ticker, crumb)
            tasks.append(task)

        return await asyncio.gather(*tasks)

    async def run_fetch_all() -> List[pd.DataFrame]:
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session)
            return all_data

    os.mkdir(f"{raw_path}/temp")
    dfs = asyncio.run(run_fetch_all())
    os.rmdir(f"{raw_path}/temp")
    os.system("taskkill /im chrome.exe /f") if open_chrome else None

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

    return dict(zip(tickers, dfs))


def get_yahoofinance_data_file_path_by_ticker(ticker: str, cj: http.cookiejar = None):
    dir = f"{os.path.abspath('')}/yahoofinance"
    files = sorted(
        os.listdir(dir),
    )
    tickers_with_data = [x for x in files if x.split("_")[0].lower() == ticker.lower()]

    if len(tickers_with_data) == 0:
        from_date = datetime.datetime(2023, 1, 1)
        to_date = datetime.datetime.today()
        download_historical_data_yahoofinance(ticker, from_date, to_date, dir, cj)

    return f"{dir}/{ticker}_yahoofin_historical_data.xlsx"


def get_option_expiration_dates_yahoofinance(
    ticker: str, cj: http.cookiejar = None
) -> List[int]:
    _, crumb = get_yahoofinance_download_auth("/v1/test/getcrumb", cj, True)
    headers = get_yahoofinance_download_auth(
        f"/v7/finance/options/{ticker}?formatted=true&crumb={crumb}&lang=en-US&region=US&date=1&straddle=true&corsDomain=finance.yahoo.com",
        cj,
    )
    url = f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}?formatted=true&crumb=wZdpBzDeWLv&lang=en-US&region=US&date=1&straddle=true&corsDomain=finance.yahoo.com"

    try:
        res = requests.get(url, headers=headers)
        json = res.json()
        return json["optionChain"]["result"][0]["expirationDates"]
    except Exception as e:
        print(e)
        return []


def safe_get(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


def empty_option_data_yahoofinance() -> dict[str, None]:
    return {
        "percentChange": None,
        "openInterest": None,
        "strike": None,
        "change": None,
        "inTheMoney": None,
        "impliedVolatility": None,
        "volume": None,
        "ask": None,
        "contractSymbol": None,
        "lastTradeDate": None,
        "expiration": None,
        "currency": None,
        "contractSize": None,
        "bid": None,
        "lastPrice": None,
    }


def get_options_chain_yahoofinance(
    ticker: str,
    raw_path: str,
    cj: http.cookiejar = None,
    big_wb=False,
) -> Dict[date, pd.DataFrame]:
    async def fetch(
        session: aiohttp.ClientSession, url: str, ex_date: int, crumb: str
    ) -> pd.DataFrame:
        try:
            headers = get_yahoofinance_download_auth(
                f"/v7/finance/options/{ticker}?formatted=true&crumb={crumb}&lang=en-US&region=US&date={ex_date}&straddle=true&corsDomain=finance.yahoo.com",
                cj,
            )
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    # straddle schema
                    """
                    type Straddle = {
                        stike: Option
                    }
                    type Option = {
                        call: OptionInfo
                        puts OptionInfo
                    }
                    type OptionInfo = {
                        "openInterest": int
                        "strike": int
                        "change": int
                        "inTheMoney": bool
                        "impliedVolatility": int
                        "volume": int
                        "ask": int
                        "contractSymbol": str
                        "lastTradeDate": int
                        "expiration": int
                        "currency": int
                        "contractSize": int
                        "bid": int
                        "lastPrice": int
                    }
                    """
                    json = await response.json()
                    data = json["optionChain"]["result"][0]["options"][0]["straddles"]

                    straddle = {}
                    for option in data:
                        strike_price = option["strike"]["raw"]

                        call_raw = safe_get(option, "call")
                        call_raw_dict = call_raw.items() if call_raw else None
                        call = (
                            {
                                key: (
                                    value.get("raw", value)
                                    if isinstance(value, dict)
                                    else value
                                )
                                for key, value in call_raw_dict
                                if call_raw_dict
                            }
                            if call_raw_dict
                            else empty_option_data_yahoofinance()
                        )

                        put_raw = safe_get(option, "put")
                        put_raw_dict = put_raw.items() if put_raw else None
                        put = (
                            {
                                key: (
                                    value.get("raw", value)
                                    if isinstance(value, dict)
                                    else value
                                )
                                for key, value in put_raw_dict
                                if put_raw_dict
                            }
                            if put_raw_dict
                            else empty_option_data_yahoofinance()
                        )

                        straddle[strike_price] = {
                            "call": call,
                            "put": put,
                        }

                    return straddle, ex_date

                else:
                    raise Exception(f"Bad Status: {response.status}")

        except Exception as e:
            print(e)
            return {}

    async def get_promises(session: aiohttp.ClientSession):
        # Option Chain Schema
        """
        type OptionChain = {
            date (int epoch): Straddle
        }
        """
        _, crumb = get_yahoofinance_download_auth("/v1/test/getcrumb", cj, True)
        exp_dates = get_option_expiration_dates_yahoofinance(ticker, cj)
        tasks = []
        for exp_date in exp_dates:
            curr_url = f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}?formatted=true&crumb={crumb}&lang=en-US&region=US&date={exp_date}&straddle=true&corsDomain=finance.yahoo.com"
            task = fetch(session, curr_url, exp_date, crumb)
            tasks.append(task)

        return await asyncio.gather(*tasks)

    """
    return List[Tuple(OptionChain, ExpDate)]
    """

    async def run_fetch_all():
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session)
            return all_data

    results = asyncio.run(run_fetch_all())
    dfs = {}
    for_big_wb = []

    try:
        for options_data, exp_date in results:
            strike_prices = list(options_data.keys())
            temp = []
            for strike in strike_prices:
                call_raw_dict = options_data[strike]["call"]
                put_raw_dict = options_data[strike]["put"]

                call_dict = {
                    f"{key}_call": value
                    for key, value in call_raw_dict.items()
                    if key != "strike"
                }
                put_dict = {
                    f"{key}_put": value
                    for key, value in put_raw_dict.items()
                    if key != "strike"
                }
                curr_strike_straddle_dict = {**call_dict, **put_dict}
                curr_strike_straddle_dict["strike"] = (
                    call_raw_dict["strike"]
                    if call_raw_dict["strike"]
                    else put_raw_dict["strike"]
                )

                print(curr_strike_straddle_dict)
                temp.append(curr_strike_straddle_dict)

                if big_wb:
                    copy_curr_strike_straddle_dict = curr_strike_straddle_dict.copy()
                    copy_curr_strike_straddle_dict["expirationDate"] = exp_date
                    for_big_wb.append(copy_curr_strike_straddle_dict)

            curr_exp_straddle_df = pd.DataFrame(temp)
            curr_exp_straddle_df.set_index("strike")
            dfs[exp_date] = curr_exp_straddle_df

        with pd.ExcelWriter(raw_path, engine="openpyxl") as writer:
            pd.DataFrame().to_excel(writer, index=False)
            for exp_date, df in dfs.items():
                cols = list(df.columns)
                last_price_index = cols.index("lastPrice_call")
                cols.remove("strike")
                cols.insert(last_price_index + 1, "strike")
                df = df[cols]
                df.to_excel(
                    writer,
                    sheet_name=f"{datetime.fromtimestamp(exp_date).strftime('%m-%d-%Y')}",
                    index=False,
                )

            if big_wb:
                big_df = pd.DataFrame(for_big_wb)
                big_df.to_excel(writer, sheet_name="all", index=False)

    except Exception as e:
        print(e)

    return dfs


def get_futures_chain(
    ticker: str, raw_path: str, cj: http.cookiejar = None, big_wb=False
):
    pass


def yahoofinance_data_fetcher():
    pass
    # tar -xvzf C:\PATH\TO\SOURCE\filename.tar.gz -C C:\PATH\TO\DESTINATION


if __name__ == "__main__":
    t0 = time.time()

    from_date = datetime(2023, 1, 1)
    to_date = datetime.today()
    tickers = ["CCRV", "COMT", "TLTW", "LQDW", "HYGW", "BRLN"]
    vol_indices = ["^MOVE", "^VIX"]
    ir_futures = ["ZT=F", "ZF=F", "ZN=F", "ZB=F", "SR3=F"]

    oil_tickers = ["USO", "UCO", "SCO"]
    nat_gas_tickers = ["BOIL", "UNG", "KOLD"]
    energy_tickers = oil_tickers + nat_gas_tickers + ["VDE", "XLE", "XOP"]
    ust_tickers = [
        "UTWO",
        "UST",
        "PST",
        "TBF",
        "TBT",
        "TBX",
        "TTT",
        "UBT",
        "GOVT",
        "TMV",
        "TYD",
        "TYO",
    ]
    long_bond_tickers = ["EDV", "ZROZ", "TLT", "TMF", "VGLT", "TLTW", "GOVZ"]
    hy_tickers = ["HYG", "JNK", "SHYG", "USHY", "HYLB", "SJNK", "SPHY", "HYGW", "ANGL"]
    senior_loan_tickers = [
        "BKLN",
        "SRLN",
        "FTSL",
        "FLBL",
        "FLRT",
        "SEIX",
        "LONZ",
        "BRLN",
    ]
    yen_tickers = ["FXY", "YCS", "YCL"]
    japan_tickers = ["EWJ", "BBJP", "DXJ", "HEWJ", "JPXN"]
    jpy_tickers = yen_tickers + japan_tickers
    meme_tickers = ["SPY", "QQQ", "VOO", "TQQQ", "IWF", "IWM"]

    clo_tickers = ["JAAA", "AAA", "CLOA", "JBBB", "CLOI", "CLOZ", "ICLO"]
    mbs_tickers = ["JSI", "MBB", "VMBS", "SPMB", "LMBS", "JMBS", "CMBS", "MTBA"]
    sec_prods_tickers = clo_tickers + mbs_tickers

    oil_tickers = ["USO", "UCO", "SCO"]
    nat_gas_tickers = ["BOIL", "UNG", "KOLD"]
    energy_tickers = oil_tickers + nat_gas_tickers + ["VDE", "XLE", "XOP"]

    sp500_tickers = [
        "SPY",
        "IVV",
        "VOO",
        "SPXL",
        "UPRO",
        "SH",
        "SPXU",
        "SPXS",
        "SPUU",
        "RSP",
    ]

    credit_tickers = ["VGIT", "VGLT", "LQD", "ICSH", "FLOT"]

    vix_tickers = ["VXX", "UVXY", "SVXY", "VIXY", "SVOL"]

    metals_tickers = ["GLD", "IAU", "SGOL", "GLTR", "SLV", "GLDM"]

    china_tickers = ["MCHI", "KWEB", "FXI", "YINN", "CWEB", "CHIQ"]
    hk_tickers = ["EWH", "FLHK"]

    india_tickers = ["INDA", "EPI", "SMIN", "PIN", "INCO", "INQQ"]

    alts_tickers = ["TUA", "TYA", "SVOL", "PFIX"]

    degen_tickers = ["SOXS"]

    em_tickers = [
        "EMXC",
        "VWO",
        "IEMG",
        "EEM",
        "EMB",
        "KEMX",
        "EMLC",
        "EBND",
        "EMB",
        "VWOB",
        "PCY",
    ]

    mbs_tickers = ["VMBS", "MBB", "SPMB", "JMBS"]

    all_tickers = (
        ust_tickers
        + long_bond_tickers
        + hy_tickers
        + senior_loan_tickers
        + jpy_tickers
        + sec_prods_tickers
        + energy_tickers
        + sp500_tickers
        + credit_tickers
        + vix_tickers
        + metals_tickers
        + china_tickers
        + hk_tickers
        + india_tickers
        + alts_tickers
        + degen_tickers
        + em_tickers
        + ["BDRY"]
        + mbs_tickers
    )

    hf_tickers = [
        "RYLD",
        "DBMF",
        "RPAR",
        "RLY",
        "MNA",
        "TDSC",
        "WTMF",
        "CAOS",
        "FVC",
        "ADME",
        "GDMA",
        "GMOM",
        "FMF",
        "MRSK",
        "PFIX",
        "PHDG",
        "PUTW",
        "UPAR",
        "ARB",
        "PPI",
        "ZHDG",
        "ROMO",
        "GHTA",
        "DBEH",
        "FORH",
    ]

    dict = multi_download_historical_data_yahoofinance(
        hf_tickers,
        from_date,
        to_date,
        r"C:\Users\chris\fund_flows\yahoofinance",
        max_date=True,
        write_to_xlsx=True,
    )

    print(dict)

    # list = get_option_expiration_dates_yahoofinance('TLT')
    # print(list)

    # ticker = "CCRV"
    # data = get_options_chain_yahoofinance(
    #     ticker,
    #     fr"C:\Users\chris\trade\curr_pos\yahoofinance\option_chains\{ticker}_option_chain.xlsx",
    #     None,
    #     True
    # )
    # print(data)

    t1 = time.time()
    print("\033[94m {}\033[00m".format(t1 - t0), " seconds")
