"""
スクレイピングページ
    - https://kabu.com/investment/meigara/bunkatu.html
    - https://kabu.com/investment/meigara/gensi.html

"""

import os
import datetime
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from selenium import webdriver
from get_data.config import db_config

#  DBエンジンのインスタンスを作成
conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(conn_string)

today = datetime.datetime.now().date()

def fetch_split_data():    
    # driverの設定
    options = webdriver.ChromeOptions()
    options.add_argument('--headless') # 画面を表示させずにブラウザを起動し、プログラムだけを実行させる
    browser = webdriver.Chrome(options=options)

    browser.get('https://kabu.com/investment/meigara/bunkatu.html')
    html = browser.page_source
    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.find("tbody")
    rows = tbody.find_all("tr")  

    split_data = []

    for row in rows:
        cols = [col.text for col in row.find_all("td")]
        col_1 = cols[1] + ".T"
        col_2 = cols[2]
        col_3 = 1 / float(cols[3].split("：")[1])
        col_4 = datetime.datetime.strptime(cols[4], "%Y/%m/%d").date()

        data = [col_1, col_2, col_3, col_4]
        split_data.append(data)

    devide_df = pd.DataFrame(split_data, columns=["symbol", "company_name", "ratio", "last_date_with_rights"])
    
    browser.quit()

    return devide_df

def fetch_merge_data():    
    # driverの設定
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    browser = webdriver.Chrome(options=options)

    browser.get('https://kabu.com/investment/meigara/gensi.html')
    html = browser.page_source
    soup = BeautifulSoup(html, "html.parser")
    tbody = soup.find("tbody")
    rows = tbody.find_all("tr")  

    merge_data = []

    for row in rows:
        cols = [col.text for col in row.find_all("td")]
        col_1 = cols[1] + ".T"
        col_2 = cols[2]
        col_3 = float(cols[3].split("→")[0].strip("株"))
        col_4 = datetime.datetime.strptime(cols[4], "%Y/%m/%d").date()

        data = [col_1, col_2, col_3, col_4]
        merge_data.append(data)

    union_df = pd.DataFrame(merge_data, columns=["symbol", "company_name", "ratio", "last_date_with_rights"])
    
    browser.quit()

    return union_df

# 既存の株価データと株式分割情報を利用して修正
def adjust_stock_prices(stock_df, devide_union_df):
    devide_union_df['last_date_with_rights'] = pd.to_datetime(devide_union_df['last_date_with_rights'])
    adjusted_df = stock_df.copy()
    filtered_df = pd.DataFrame(columns=["symbol", "company_name", "ratio", "last_date_with_rights"])

    today = datetime.datetime.now().date()
    if today in devide_union_df['last_date_with_rights'].values:       
        # 昨日の日付だけを抽出
        filtered_df = devide_union_df[devide_union_df["last_date_with_rights"] == today]
        for _, row in filtered_df.iterrows():
            ticker = row["symbol"] 
            # company_name = row["company_name"]
            ratio = row["ratio"]
            # 該当銘柄の分割日以前のデータを修正
            mask = (adjusted_df['symbol'] == ticker) & (adjusted_df['date'] <= today)
            adjusted_df.loc[mask, ['open', 'high', 'low', 'close', 'adjclose']] *= ratio
            adjusted_df.loc[mask, 'volume'] /= ratio

    return adjusted_df, filtered_df


def main():
    # 修正値が反映される日（split_date）の銘柄をピックアップして、前日（last_date_with_rights）以前のデータを修正
    devide_df = fetch_split_data()
    union_df = fetch_merge_data()
    devide_union_df = pd.concat([devide_df, union_df])
    devide_union_df.to_sql("devide_union_data", engine, if_exists="append", index=False)
    # プログラムの開始時に必要なディレクトリをすべて作成
    os.makedirs('./input/devide_union', exist_ok=True)
    devide_union_df.to_csv('./input/devide_union/devide_union_df.csv', index=False)

    # # 1回目のみ実行
    # if s: # stock_pricesにデータがある、かつadjusted_stock_pricesにデータがない場合に実行
    #     adjusted_df, filtered_df = adjust_stock_prices(df_all_stock_prices, devide_union_df) # 修正する関数の適用
    #     adjusted_df.to_sql("adjusted_stock_prices", engine, if_exists="replace", index=False) # 修正後の株価をテーブルに保存
    #     filtered_df.to_sql("applied_data", engine, if_exists="append", index=False) # 修正した銘柄を保存
    # else:    
    #     # ２回目からは修正済みのデータ（adjusted_stock_prices）を引っ張ってくる
    #     adjusted_df, filtered_df = adjust_stock_prices(df_all_stock_prices, devide_union_df) # 修正する関数の適用
    #     adjusted_df.to_sql("adjusted_stock_prices", engine, if_exists="replace", index=False) # 修正後の株価をテーブルに保存
    #     filtered_df.to_sql("applied_data", engine, if_exists="append", index=False) # 修正した銘柄を保存

if __name__ == "__main__":
    main()
