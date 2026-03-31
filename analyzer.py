import pandas as pd
import aiohttp
import asyncio
import time
from bs4 import BeautifulSoup

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/122.0.0.0 Safari/537.36'
    ),
    'Referer': 'https://finance.naver.com',
}


async def get_naver_rank_tickers(session, investor_gubun="9000", market="KOSPI"):
    """
    Scrape top net-buyers from Naver Finance ranking asynchronously.
    """
    sosok = "01" if market == "KOSPI" else "02"
    url = (
        "https://finance.naver.com/sise/sise_deal_rank_iframe.naver"
        f"?investor_gubun={investor_gubun}&sosok={sosok}&type=buy"
    )
    headers = {**HEADERS, 'Referer': 'https://finance.naver.com/sise/sise_deal_rank.naver'}
    try:
        async with session.get(url, headers=headers, timeout=10) as res:
            text = await res.text(encoding='euc-kr')
            soup = BeautifulSoup(text, 'lxml')

            tickers = []
            for a in soup.select('a.tltle'):
                if 'code=' in a['href']:
                    code = a['href'].split('code=')[-1]
                    name = a.get_text(strip=True)
                    tickers.append({'ticker': code, 'name': name})
            return tickers
    except Exception as e:
        print(f"[Error] get_naver_rank_tickers ({market}, {investor_gubun}): {e}")
        return []


async def get_naver_historical_investor(session, ticker, n_days=20):
    """
    Scrape historical foreign + institutional net buying asynchronously.
    """
    headers = {**HEADERS, 'Referer': f'https://finance.naver.com/item/main.naver?code={ticker}'}
    data = []
    page = 1
    max_pages = 3  # 최대 3페이지 (약 30거래일)

    try:
        while len(data) < n_days and page <= max_pages:
            url = f"https://finance.naver.com/item/frgn.naver?code={ticker}&page={page}"
            async with session.get(url, headers=headers, timeout=10) as res:
                text = await res.text(encoding='euc-kr')
                soup = BeautifulSoup(text, 'lxml')

                rows = soup.select('table.type2 tr[onmouseover]')
                if not rows:
                    break

                for row in rows:
                    cols = row.select('td')
                    if len(cols) < 9:
                        continue

                    date_raw = cols[0].get_text(strip=True)    # "2026.03.19"
                    date     = date_raw.replace('.', '')        # "20260319"

                    inst_str    = cols[5].get_text(strip=True).replace(',', '')
                    foreign_str = cols[6].get_text(strip=True).replace(',', '')

                    if not inst_str or not foreign_str:
                        continue

                    try:
                        inst    = int(inst_str)
                        foreign = int(foreign_str)
                    except ValueError:
                        continue

                    data.append({
                        'date':    date,
                        'inst':    inst,
                        'foreign': foreign,
                    })

                    if len(data) >= n_days:
                        break

            page += 1

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data).set_index('date')
        return df.iloc[::-1]

    except Exception as e:
        print(f"[Error] get_naver_historical_investor ({ticker}): {e}")
        return pd.DataFrame()


async def analyze_double_buying(market="KOSPI"):
    """
    Identify stocks with simultaneous foreign + institutional net buying.
    Returns dict: { 'new': [...], 'continuous': [...], 'ended': [...] }
    """
    print(f"[+] Analyzing {market} concurrently...")

    async with aiohttp.ClientSession() as session:
        # 동시에 외국인 & 기관 랭킹 조회
        task1 = get_naver_rank_tickers(session, "9000", market)
        task2 = get_naver_rank_tickers(session, "1000", market)
        foreign_top, inst_top = await asyncio.gather(task1, task2)

        candidates = {t['ticker']: t['name'] for t in foreign_top}
        for t in inst_top:
            candidates[t['ticker']] = t['name']

        # 비동기이므로 상위 50개까지 안전하게 한 번에 조회 가능
        candidate_list = list(candidates.items())[:50]
        print(f"[+] Processing top {len(candidate_list)} candidates concurrently. Fetching history ...")

        # 50개 종목 히스토리 동시 크롤링!
        tasks = [
            get_naver_historical_investor(session, code, n_days=20)
            for code, name in candidate_list
        ]
        
        histories = await asyncio.gather(*tasks)

    new_double        = []
    continuous_double = []
    ended_double      = []

    for i, (code, name) in enumerate(candidate_list):
        df = histories[i]
        if df.empty:
            continue
            
        try:
            df['both'] = (df['foreign'] > 0) & (df['inst'] > 0)

            latest = bool(df['both'].iloc[-1])
            prev   = bool(df['both'].iloc[-2]) if len(df) > 1 else False

            latest_row = df.iloc[-1]

            if latest and not prev:
                # New double buying today
                new_double.append({
                    "ticker":  code,
                    "name":    name,
                    "market":  market,
                    "foreign": int(latest_row["foreign"]),
                    "inst":    int(latest_row["inst"]),
                })

            elif latest and prev:
                # Count consecutive days at tail
                cont_days = 0
                for v in reversed(df['both'].values):
                    if v:  cont_days += 1
                    else:  break
                continuous_double.append({
                    "ticker":  code,
                    "name":    name,
                    "market":  market,
                    "foreign": int(latest_row["foreign"]),
                    "inst":    int(latest_row["inst"]),
                    "days":    cont_days,
                    "start_date": df.index[-cont_days],
                })

            elif not latest and prev:
                # Buying ended today
                streak = 0
                for v in reversed(df['both'].values[:-1]):
                    if v:  streak += 1
                    else:  break
                if streak >= 2:
                    prev_row = df.iloc[-2]
                    ended_double.append({
                        "ticker":       code,
                        "name":         name,
                        "market":       market,
                        "foreign_prev": int(prev_row["foreign"]),
                        "inst_prev":    int(prev_row["inst"]),
                        "ended_date":   df.index[-1],
                    })

        except Exception as e:
            print(f"[Error] {name} ({code}): {e}")

    new_double.sort(       key=lambda x: x['foreign'] + x['inst'], reverse=True)
    continuous_double.sort(key=lambda x: (-x['days'], x['name']))

    result = {
        "new":        new_double[:30],
        "continuous": continuous_double[:30],
        "ended":      ended_double[:30],
    }

    print(f"[+] Done - New: {len(result['new'])}, Continuous: {len(result['continuous'])}, Ended: {len(result['ended'])}")
    return result


if __name__ == "__main__":
    result = asyncio.run(analyze_double_buying("KOSPI"))
