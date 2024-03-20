import asyncio
import http
import os
import time
import webbrowser
from datetime import date, datetime
from typing import Dict, List

import aiohttp
import brotli

import pandas as pd
import requests
import ujson as json


def fetch_new_bearer_token(
    cj: http.cookiejar = None, open_chrome=False
) -> Dict[str, str] | None:
    headers = {
        "authority": "www.etf.com",
        "method": "GET",
        "path": "/api/v1/api-details",
        "scheme": "https",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Dnt": "1",
        "Referer": "https://www.etf.com/",
        "Sec-Ch-Ua": '"Chromium";v="118", "Google Chrome";v="118", "Not=A?Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    }

    if open_chrome:
        webbrowser.open("https://www.etf.com/")
        time.sleep(3)
        os.system("taskkill /im chrome.exe /f")

    if cj:
        print("CJ IS DEFINED")
        cookies = {}
        try:
            cookies = {
                cookie.name: cookie.value for cookie in cj if "etf.com" in cookie.domain
            }
            cookies["_gat_G-NFBGF6073J"] = 1
            cookies["kw.pv_session	"] = 10
            # find dynmaic way to get this
            cookies["kw.session_ts"] = "1697501453497"
            cookies_str = "; ".join(
                [f"{key}={value}" for key, value in cookies.items()]
            )
            headers["Cookie"] = cookies_str
        except Exception as e:
            print(e)

    url = "https://www.etf.com/api/v1/api-details"
    res = requests.get(url, headers=headers)

    if res.status_code == 200 or res.status_code == 201:
        json = res.json()
        return {
            "fundApiKey": json["fundApiKey"],
            "toolsApiKey": json["toolsApiKey"],
            "oauthToken": json["oauthToken"],
        }

    print(f"Status Code: {res.status_code} - Fetch Bearer Token Failed")
    return None


def fetch_fund_flow_data(
    ticker, date_from: date, date_to: date, raw_path: str = None
) -> pd.DataFrame:
    date_from_str = date_from.strftime("%Y-%m-%d").replace("-", "")
    date_to_str = date_to.strftime("%Y-%m-%d").replace("-", "")
    headers = {
        "authority": "api-prod.etf.com",
        "method": "GET",
        "path": f"/private/apps/fundflows/{ticker}/charts?startDate={date_from_str}&endDate={date_to_str}",
        "scheme": "https",
        "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "max-age=0",
        "Content-Type": "application/json",
        "Cookie": "__qca=P0-78624358-1697326913816; _hjSessionUser_2686080=eyJpZCI6IjJhY2UwYWQzLTZiZjktNWQzMi1hMDRiLTBkZjc2Njg5OTUzNSIsImNyZWF0ZWQiOjE2OTczMjY5MTQzNzIsImV4aXN0aW5nIjp0cnVlfQ==; _pctx=%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIFYAODgdgGZ%2BAFg4AmfhwAMARin9eQ6SAC%2BQA; _pcid=%7B%22browserId%22%3A%22ls837s5u647clcer%22%7D; _pcus=eyJ1c2VyU2VnbWVudHMiOm51bGx9; cX_P=ls837s5u647clcer; cX_G=cx%3A31419pten9my114dodksigmt0w%3A2w5vkseoawmov; __pat=-14400000; _gid=GA1.2.878539956.1710930385; kw.session_ts=1710930385367; cf_clearance=_Xo3DCDqxM6L9iOS6tsVkEgtIUGyTsmbwh_ww79.bFc-1710930383-1.0.1.1-0sZuWIrXJ7rv4gcX3KcCqPF2r6FGrUGEijvjJcfD08YR8dnuGzK_4_UW8.XsBgjsfnPsWxXQ1mxsrTnq1ol9yg; _sp_ses.a8c0=*; _hjSession_2686080=eyJpZCI6ImE2ZDUyNWNkLWUxNDQtNDRhNy1iYzJkLTY2NmE1NDUyMmRlNyIsImMiOjE3MTA5MzAzODU2ODUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowfQ==; __pvi=eyJpZCI6InYtbHR6bnJ4bGtrNHZyNzJpMiIsImRvbWFpbiI6Ii5ldGYuY29tIiwidGltZSI6MTcxMDkzMDQ5MzYxN30%3D; _ga=GA1.2.45362378.1697326914; __tbc=%7Bkpex%7DHoiIrZMnfHXlKEKIuLWWixXAyQTR6JNrJys6wjwcc2lTRRif_AqPojRPKKlo0Foa; xbc=%7Bkpex%7Dwz8izwFXIkg9sLpEIXtmh2H8PPtpnov1kwV3o-yvpAhSIN9xZxi0CVldRjvsBH-PQ2q12g4O6QZEcpx8Lh9zGup4ghrS39tI-rQV5xk_RkbMXAfb_69fMtXht7dRtG9i6_rLDP2KGnzxEIZhgdwQDVB8uZdftFRej0djNeP8UV9P4VBi7J8cAqvnlREj-TH1Jm2QG-2i2LSYd_FXlLfyLVkNQzqs_-gxfVDq7IR-lKJEJA0PkVslVZWlXnKJZ0QbZWjDvlRJOAC-Qms5g3fNIza6SU3m0GktCfanTrQUILx4AsuLBYUV5bK5STOxANiU7yP0OMNcU_-yQbgv9UUYDnNwV_Z5oXQ4LGMgE4hwMNz9pTc6IebE_QETzHW0sT5BoVf9AgdpiUAxNlGnpzURMxm-1boONnpX8r_qSCHaVCfskXRVVqYOnxVPtdQjmIUvKsAbtwI_tY7JZmtpw7tZl7_5jMf8NyOm33aya9WY6rTUXPlwt3vbTki3-pDasRBl; FCNEC=%5B%5B%22AKsRol9dHWFn6ShMul76v_0h28xOXK6BdWrQQvZ1EsOCxA-gLnNwhYgJWJ3bQK79lz9oHInBb2eylud_9UeULvhLVjMydCU1-t_4SEjtc20bZSxPynFQMc48i2iGqWN-HqC7Akcz9g4VfIoTQGGp9InyZsc2PAB2VQ%3D%3D%22%5D%5D; _ga_NFBGF6073J=GS1.1.1710930385.56.1.1710930495.56.0.0; kw.pv_session=6; _sp_id.a8c0=6c8d3171-ec52-41cf-91cc-d9602e221d09.1697326914.53.1710930614.1710602081.2bfd5bb2-a34d-4deb-990d-e99ef8ae14a4",
        "Dnt": "1",
        "If-None-Match": "W/\"8b09-E20zV8i3lylx2EqkjQDu72dYFSE\"",
        "Sec-Ch-Ua": "\"Chromium\";v=\"122\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"122\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    url = f"https://api-prod.etf.com/private/apps/fundflows/{
        ticker}/charts?startDate={date_from_str}&endDate={date_to_str}"
    res = requests.get(url, headers=headers)
    if res.ok:
        if 'br' in res.headers.get('Content-Encoding', ''):
            try:
                decompressed_data = brotli.decompress(res.content)
                json_data = json.loads(decompressed_data.decode('utf-8'))
            except brotli.Error as e:
                print(f"Decompression error: {e}")
                if "PADDING_2" in str(e):
                    print("Requests has already decompressed brotli data")
                    try:
                        json_data = json.loads(res.content)
                    except Exception as e:
                        print(e)
                        return None
        else:
            try:
                json_data = res.json()
            except ValueError as e:
                print(f"Error parsing JSON: {e}")
                return None

        try:
            data = json_data["data"]["results"]["data"]
            df = pd.DataFrame(data)
            if raw_path:
                wb_name = f"{raw_path}/{ticker}_fund_flow_data.xlsx"
                df.to_excel(wb_name, index=False)
            return df
        except (KeyError, TypeError) as e:
            print(f"Error processing data: {e}")
            return None

    print(f"Status Code: {res.status_code} - Fetch Fund Flows Data Failed")
    return None


def multi_fetch_fund_flow_data(
    tickers: List[int],
    date_from: date,
    date_to: date,
    raw_path: str = None,
    return_df=False,
    run_brotli_decompression=False,
) -> Dict[str, List[Dict[str, str]] | pd.DataFrame]:
    async def fetch(
        session: aiohttp.ClientSession, url: str, curr_ticker: int
    ) -> pd.DataFrame:
        try:
            headers = {
                "authority": "api-prod.etf.com",
                "method": "GET",
                "path": url.split("/", 1)[1],
                "scheme": "https",
                "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "max-age=0",
                "Content-Type": "application/json",
                "Cookie": "__qca=P0-78624358-1697326913816; _hjSessionUser_2686080=eyJpZCI6IjJhY2UwYWQzLTZiZjktNWQzMi1hMDRiLTBkZjc2Njg5OTUzNSIsImNyZWF0ZWQiOjE2OTczMjY5MTQzNzIsImV4aXN0aW5nIjp0cnVlfQ==; _pctx=%7Bu%7DN4IgrgzgpgThIC4B2YA2qA05owMoBcBDfSREQpAeyRCwgEt8oBJAEzIFYAODgdgGZ%2BAFg4AmfhwAMARin9eQ6SAC%2BQA; _pcid=%7B%22browserId%22%3A%22ls837s5u647clcer%22%7D; _pcus=eyJ1c2VyU2VnbWVudHMiOm51bGx9; cX_P=ls837s5u647clcer; cX_G=cx%3A31419pten9my114dodksigmt0w%3A2w5vkseoawmov; __pat=-14400000; _gid=GA1.2.878539956.1710930385; kw.session_ts=1710930385367; cf_clearance=_Xo3DCDqxM6L9iOS6tsVkEgtIUGyTsmbwh_ww79.bFc-1710930383-1.0.1.1-0sZuWIrXJ7rv4gcX3KcCqPF2r6FGrUGEijvjJcfD08YR8dnuGzK_4_UW8.XsBgjsfnPsWxXQ1mxsrTnq1ol9yg; _sp_ses.a8c0=*; _hjSession_2686080=eyJpZCI6ImE2ZDUyNWNkLWUxNDQtNDRhNy1iYzJkLTY2NmE1NDUyMmRlNyIsImMiOjE3MTA5MzAzODU2ODUsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowfQ==; __pvi=eyJpZCI6InYtbHR6bnJ4bGtrNHZyNzJpMiIsImRvbWFpbiI6Ii5ldGYuY29tIiwidGltZSI6MTcxMDkzMDQ5MzYxN30%3D; _ga=GA1.2.45362378.1697326914; __tbc=%7Bkpex%7DHoiIrZMnfHXlKEKIuLWWixXAyQTR6JNrJys6wjwcc2lTRRif_AqPojRPKKlo0Foa; xbc=%7Bkpex%7Dwz8izwFXIkg9sLpEIXtmh2H8PPtpnov1kwV3o-yvpAhSIN9xZxi0CVldRjvsBH-PQ2q12g4O6QZEcpx8Lh9zGup4ghrS39tI-rQV5xk_RkbMXAfb_69fMtXht7dRtG9i6_rLDP2KGnzxEIZhgdwQDVB8uZdftFRej0djNeP8UV9P4VBi7J8cAqvnlREj-TH1Jm2QG-2i2LSYd_FXlLfyLVkNQzqs_-gxfVDq7IR-lKJEJA0PkVslVZWlXnKJZ0QbZWjDvlRJOAC-Qms5g3fNIza6SU3m0GktCfanTrQUILx4AsuLBYUV5bK5STOxANiU7yP0OMNcU_-yQbgv9UUYDnNwV_Z5oXQ4LGMgE4hwMNz9pTc6IebE_QETzHW0sT5BoVf9AgdpiUAxNlGnpzURMxm-1boONnpX8r_qSCHaVCfskXRVVqYOnxVPtdQjmIUvKsAbtwI_tY7JZmtpw7tZl7_5jMf8NyOm33aya9WY6rTUXPlwt3vbTki3-pDasRBl; FCNEC=%5B%5B%22AKsRol9dHWFn6ShMul76v_0h28xOXK6BdWrQQvZ1EsOCxA-gLnNwhYgJWJ3bQK79lz9oHInBb2eylud_9UeULvhLVjMydCU1-t_4SEjtc20bZSxPynFQMc48i2iGqWN-HqC7Akcz9g4VfIoTQGGp9InyZsc2PAB2VQ%3D%3D%22%5D%5D; _ga_NFBGF6073J=GS1.1.1710930385.56.1.1710930495.56.0.0; kw.pv_session=6; _sp_id.a8c0=6c8d3171-ec52-41cf-91cc-d9602e221d09.1697326914.53.1710930614.1710602081.2bfd5bb2-a34d-4deb-990d-e99ef8ae14a4",
                "Dnt": "1",
                "If-None-Match": "W/\"8b09-E20zV8i3lylx2EqkjQDu72dYFSE\"",
                "Sec-Ch-Ua": "\"Chromium\";v=\"122\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"122\"",
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": "\"Windows\"",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            }
            async with session.get(url, headers=headers) as response:
                if response.ok:
                    content = await response.text()
                    if 'br' in response.headers.get('Content-Encoding', ''):
                        if run_brotli_decompression:
                            decompressed_data = brotli.decompress(content)
                            json_data = json.loads(
                                decompressed_data.decode('utf-8'))
                        else:
                            json_data = json.loads(content)
                    else:
                        json_data = await response.json()

                    data = json_data["data"]["results"]["data"]
                    df = pd.DataFrame(data)
                    if raw_path:
                        wb_name = f"{
                            raw_path}/{curr_ticker}_fund_flow_data.xlsx"
                        df.to_excel(wb_name, index=False)
                    return (curr_ticker, data) if not return_df else (curr_ticker, df)
                else:
                    raise Exception(f"Bad Status: {response.status}")
        except Exception as e:
            print(f"Error with {curr_ticker}", e)
            return (curr_ticker, None)

    async def get_promises(session: aiohttp.ClientSession):
        tasks = []
        for ticker in tickers:
            date_from_str = date_from.strftime("%Y-%m-%d").replace("-", "")
            date_to_str = date_to.strftime("%Y-%m-%d").replace("-", "")
            curr_url = f"https://api-prod.etf.com/private/apps/fundflows/{
                ticker}/charts?startDate={date_from_str}&endDate={date_to_str}"
            task = fetch(session, curr_url, ticker)
            tasks.append(task)

        return await asyncio.gather(*tasks)

    async def run_fetch_all() -> List[pd.DataFrame]:
        async with aiohttp.ClientSession() as session:
            all_data = await get_promises(session)
            return all_data

    result = asyncio.run(run_fetch_all())
    clean_dict = {}
    for k, v in result:
        if v is None:
            print(f"{k} no fund flows data")
        else:
            clean_dict[k] = v

    return clean_dict


def fund_flow_data_fetcher(
    tickers: List[str],
    raw_path: str = None,
    date_from: datetime = datetime(2000, 1, 1),
    date_to: datetime = datetime.today(),
):
    raw_path = raw_path or r"C:\Users\chris\fund_flows\flows"
    return multi_fetch_fund_flow_data(
        tickers, date_from, date_to, raw_path, return_df=True
    )


if __name__ == "__main__":
    t1 = time.time()

    date_from = datetime(2000, 1, 1)
    date_to = datetime.today()
    raw_path = r"C:\Users\chris\fund_flows\flows"

    tickers_test = ["fucjy", "QQQ"]
    df_dict = multi_fetch_fund_flow_data(
        tickers=tickers_test, date_from=date_from, date_to=date_to, return_df=True, raw_path=raw_path)
    print(df_dict)

    t2 = time.time()
    print(f"{t2 - t1} seconds")
