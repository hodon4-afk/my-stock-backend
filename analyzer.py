import pandas as pd
import requests
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


def get_naver_rank_tickers(investor_gubun="9000", market="KOSPI"):
    """
    Scrape top net-buyers from Naver Finance ranking.
    investor_gubun: 9000 = Foreigner, 1000 = Institutional
    market: KOSPI or KOSDAQ
    """
    sosok = "01" if market == "KOSPI" else "02"
    url = (
        "https://finance.naver.com/sise/sise_deal_rank_iframe.naver"
        f"?investor_gubun={investor_gubun}&sosok={sosok}&type=buy"
    )
    headers = {**HEADERS, 'Referer': 'https://finance.naver.com/sise/sise_deal_rank.naver'}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = 'euc-kr'
        soup = BeautifulSoup(res.text, 'lxml')

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


def get_naver_historical_investor(ticker, n_days=5):
    """
    Scrape historical foreign + institutional net buying from Naver Finance
    URL  : https://finance.naver.com/item/frgn.naver?code={ticker}
    cols : inst_net (col 5), foreign_net (col 6)
    Returns a DataFrame indexed by YYYYMMDD, oldest first.
    """
    url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
    headers = {**HEADERS, 'Referer': f'https://finance.naver.com/item/main.naver?code={ticker}'}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.encoding = 'euc-kr'
        soup = BeautifulSoup(res.text, 'lxml')

        rows = soup.select('table.type2 tr[onmouseover]')

        data = []
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

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data).set_index('date')
        return df.iloc[::-1]   # Naver is newest-first; reverse to chronological

    except Exception as e:
        print(f"[Error] get_naver_historical_investor ({ticker}): {e}")
        return pd.DataFrame()


def analyze_double_buying(market="KOSPI"):
    """
    Identify stocks with simultaneous foreign + institutional net buying.
    Returns dict: { 'new': [...], 'continuous': [...], 'ended': [...] }
    """
    print(f"[+] Analyzing {market} ...")

    foreign_top = get_naver_rank_tickers("9000", market)
    inst_top    = get_naver_rank_tickers("1000", market)

    candidates = {t['ticker']: t['name'] for t in foreign_top}
    for t in inst_top:
        candidates[t['ticker']] = t['name']

    # Limit candidates to top 20 to avoid Render timeout (30s)
    candidate_list = list(candidates.items())[:20]
    print(f"[+] Processing top {len(candidate_list)} candidates. Fetching history ...")

    new_double        = []
    continuous_double = []
    ended_double      = []

    for i, (code, name) in enumerate(candidate_list):
        try:
            df = get_naver_historical_investor(code, n_days=5)
            if df.empty:
                continue

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
                # Buying ended today -- only report if streak was >= 2 days
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
    # Primary: Days (descending), Secondary: Name (ascending)
    continuous_double.sort(key=lambda x: (-x['days'], x['name']))

    result = {
        "new":        new_double[:30],
        "continuous": continuous_double[:30],
        "ended":      ended_double[:30],
    }

    print(f"[+] Done - New: {len(result['new'])}, Continuous: {len(result['continuous'])}, Ended: {len(result['ended'])}")
    return result


if __name__ == "__main__":
    result = analyze_double_buying("KOSPI")
