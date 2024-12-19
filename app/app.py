import sys
import os
import datetime
import numpy as np
import matplotlib.pyplot as plt 
import japanize_matplotlib
import plotly.express as px
import plotly.graph_objects as go
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.function import *

# ページタイトルとページアイコン
st.set_page_config(page_title="メインページ", page_icon='icon.png', layout="wide")

# テーブル名
financial_info_table = 'financial_info'
metrics_table = 'metrics' 

# 条件入力UI
st.sidebar.header("検索条件")
raw_symbol = st.sidebar.text_input("シンボル", value="3923")
symbol = raw_symbol + '.T'
start_date = st.sidebar.date_input("開始日", (datetime.datetime.today() - datetime.timedelta(days=700)))
end_date = st.sidebar.date_input("終了日", datetime.datetime.today())

# データベースからデータを取得
if st.sidebar.button("データを取得"): # SQLでは、テーブル名をバインドパラメータとして扱うえない。→SQLの構文解析とクエリプランの生成が、実行前に行われる必要があるため
    
    # SQLクエリとパラメータの定義
    financial_info_query = text(f"""
        SELECT *
        FROM {financial_info_table}
        WHERE symbol = :symbol
        AND "asOfDate"::date BETWEEN :start_date AND :end_date
    """)
    financial_info_params = {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date
    }

    # SQLクエリとパラメータの定義
    metrics_query = text(f"""
        SELECT *
        FROM {metrics_table}
        WHERE symbol = :symbol
    """)
    metrics_params = {
        "symbol": symbol
    }

    # データの取得
    try:
        financial_data = fetch_data(financial_info_query, financial_info_params)
        metrics_data = fetch_data(metrics_query, metrics_params)

        if not financial_data.empty or metrics_data.empty:
            # financial_infoデータの取得
            financial_data["売上高総利益率"] = financial_data["GrossProfit"] / financial_data["TotalRevenue"]
            financial_data["営業利益率"] = financial_data["OperatingIncome"] / financial_data["TotalRevenue"]
            financial_data["当期純利益率"] = financial_data["NetIncomeCommonStockholders"] / financial_data["TotalRevenue"]
            financial_data["流動比率"] = financial_data["CurrentAssets"] / financial_data["CurrentLiabilities"]
            financial_data["当座比率"] = (financial_data["CurrentAssets"] - financial_data["Inventory"]) / financial_data["CurrentLiabilities"]
            financial_data["自己資本比率"] = financial_data["StockholdersEquity"] / financial_data["TotalAssets"]

            fin_3m = financial_data.query("periodType=='3M'").loc[:,["symbol","asOfDate","periodType","TotalRevenue","GrossProfit","OperatingIncome","NetIncomeCommonStockholders","NetIncome",
                                                                    "売上高総利益率","営業利益率","当期純利益率","流動比率","当座比率","自己資本比率","TotalAssets","StockholdersEquity","TotalLiabilitiesNetMinorityInterest"]].dropna()
            fin_3m = fin_3m.sort_values('asOfDate')
            fin_12m = financial_data.query("periodType=='12M'").loc[:,["symbol","asOfDate","periodType","TotalRevenue","GrossProfit","OperatingIncome","NetIncomeCommonStockholders","NetIncome",
                                                                    "売上高総利益率","営業利益率","当期純利益率","流動比率","当座比率","自己資本比率","TotalAssets","StockholdersEquity","TotalLiabilitiesNetMinorityInterest"]].dropna()
            fin_12m = fin_12m.sort_values('asOfDate')
            latest_row_3m = fin_3m[fin_3m["asOfDate"] == fin_3m["asOfDate"].max()]
            latest_row_12m = fin_12m[fin_12m["asOfDate"] == fin_12m["asOfDate"].max()]

        else:
            st.warning("指定された期間内に年次データが存在しません。期間を調整してください。")

    except Exception as e:
        st.error(f"データ取得中にエラーが発生しました: {e}")

# columns_str = ', '.join(columns) if isinstance(columns, (list, tuple)) else columns

# メイン画面
st.header("財務情報サマリー")

if 'financial_data' in locals() and 'metrics_data' in locals():
    # 1. 企業基本情報の追加
    st.subheader("企業基本情報")
    with st.container(border=True):
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.write(f"##### 企業名：{metrics_data['ticker_name'].values[0]}")
            st.write(f"##### 区分　：{metrics_data['market_product_category'].values[0]}")
            st.write(f"##### 業種　：{metrics_data['type_33'].values[0]}")
        with col_b:
            marketcap = format_to_billions_jpy(float(metrics_data['MarketCap'].values[0]))
            st.write(f"##### 時価総額：{marketcap}")
            PER_filtered_data = financial_data[financial_data['PeRatio'].notna()]
            PER_latest_date = PER_filtered_data['asOfDate'].max()
            PER = PER_filtered_data[PER_filtered_data['asOfDate'] == PER_latest_date]['PeRatio'].values[0]
            st.write(f"##### PER　：{PER:.2f}倍")
            PBR_filtered_data = financial_data[financial_data['PbRatio'].notna()]
            PBR_latest_date = PBR_filtered_data['asOfDate'].max()
            PBR = PBR_filtered_data[PBR_filtered_data['asOfDate'] == PBR_latest_date]['PbRatio'].values[0]
            st.write(f"##### PBR　：{PBR:.2f}倍")
        with col_c:
            st.write(f"##### 配当利回り：{metrics_data['dividendYield'].values[0]:.2f}%")
            st.write(f"##### ROE ：{metrics_data['ROE'].values[0]:.2f}%")
            st.write(f"##### ROA ：{metrics_data['ROA'].values[0]:.2f}%")

    st.divider() 

    col_1 = format_to_billions_jpy(latest_row_12m["TotalRevenue"].iloc[0])
    col_2 = format_to_billions_jpy(latest_row_12m["GrossProfit"].iloc[0])
    col_3 = format_to_billions_jpy(latest_row_12m["NetIncomeCommonStockholders"].iloc[0])
    col_4 = format_to_billions_jpy(latest_row_12m["NetIncome"].iloc[0])

    col_5 = latest_row_12m["売上高総利益率"].iloc[0] * 100
    col_6 = latest_row_12m["営業利益率"].iloc[0] * 100
    col_7 = latest_row_12m["当期純利益率"].iloc[0] * 100

    col_8 = latest_row_12m["流動比率"].iloc[0] * 100
    col_9 = latest_row_12m["当座比率"].iloc[0] * 100
    col_10 = latest_row_12m["自己資本比率"].iloc[0] * 100

    with st.container(border=True):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.subheader("当期売上高")
            st.write(f"#### {col_1}")
        with col2:
            st.subheader("当期売上高総利益")
            st.write(f"#### {col_2}")
        with col3:
            st.subheader("当期営業利益")
            st.write(f"#### {col_3}")
        with col4:
            st.subheader("当期純利益")
            st.write(f"#### {col_4}")
            
    st.divider()

    with st.container(border=True):
        # 収益性と安全性指標の表示
        st.write("#### 収益性")
        col5, col6, col7 = st.columns(3)
        with col5:
            st.write("##### 売上高総利益率")
            st.write(f"### {col_5:.2f}%")
        with col6:
            st.write("##### 営業利益率")
            st.write(f"### {col_6:.2f}%")
        with col7:
            st.write("##### 当期純利益率")
            st.write(f"### {col_7:.2f}%")

    with st.container(border=True):
        st.write("#### 安全性")
        col8, col9, col10 = st.columns(3)
        with col8:
            st.write("##### 流動比率")
            st.write(f"### {col_8:.2f}%")
        with col9:
            st.write("##### 当座比率")
            st.write(f"### {col_9:.2f}%")
        with col10:
            st.write("##### 自己資本比率")
            st.write(f"### {col_10:.2f}%")

    st.divider()  # コンテナを線で区切る

    col11, col12 = st.columns(2)

    # 売上/利益率の月次推移グラフ
    with col11:
        col11.write("### 月次推移 売上/利益率 単月")
        fig, ax1 = plt.subplots(figsize=(10, 6))
        ax2 = ax1.twinx()

        ax1.bar(fin_3m['asOfDate'], fin_3m['TotalRevenue'], color='skyblue', label='売上高', alpha=0.7)
        ax2.plot(fin_3m['asOfDate'], fin_3m['売上高総利益率'] * 100, color='red', marker='o', label='総利益率')
        ax2.plot(fin_3m['asOfDate'], fin_3m['営業利益率'] * 100, color='blue', marker='x', label='営業利益率')
        ax2.plot(fin_3m['asOfDate'], fin_3m['当期純利益率'] * 100, color='green', marker='s', label='純利益率')

        ax1.set_xlabel('年月日')
        ax1.set_ylabel('売上高 (JPY)')
        ax2.set_ylabel('利益率 (%)')
        ax1.tick_params(axis='x', rotation=45)
        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')

        st.pyplot(fig)
    
    # 資産・負債構成の期首期末比較
    with col12:
        col12.write("### 資産・負債構成")

        # 各年のデータを取得
        fin_12m["Year"] = pd.to_datetime(fin_12m["asOfDate"]).dt.year  # 年を抽出
        years = fin_12m["Year"].unique()
        n_years = len(years)

        fig, axes = plt.subplots(1, n_years, figsize=(10, 6))

        for i, year in enumerate(years):
            yearly_data = fin_12m[fin_12m["Year"] == year]

            # 軸の設定
            ax = axes[i] if n_years > 1 else axes

            # バーチャートのx座標（ずらして配置する）
            x = np.arange(len(yearly_data))

            # 資産のバーチャート
            ax.bar(
                x - 0.2,
                yearly_data["TotalAssets"],
                width=0.4,
                color="skyblue",
                alpha=0.8,
                label="資産 (Total Assets)"
            )

            # 負債＆資本のスタックバーチャート
            ax.bar(
                x + 0.2,
                yearly_data["TotalLiabilitiesNetMinorityInterest"],
                width=0.4,
                color="pink",
                alpha=0.8,
                label="負債 (Liabilities)"
            )
            ax.bar(
                x + 0.2,
                yearly_data["StockholdersEquity"],
                width=0.4,
                bottom=yearly_data["TotalLiabilitiesNetMinorityInterest"],
                color="lightgreen",
                alpha=0.8,
                label="資本 (Equity)"
            )

            # 軸ラベルとタイトル
            ax.set_title(f"{year}年の資産・負債＆資本")
            ax.set_ylabel("金額 (USD)")
            ax.set_xticks(x)
            ax.set_xticklabels(yearly_data["asOfDate"])
            ax.set_xlabel("日付")
            ax.legend(loc="upper left")

        plt.tight_layout()
        
        st.pyplot(fig)


    st.subheader("参照データ")
    st.success(f"{len(financial_data)}件のデータを取得しました。")
    st.dataframe(financial_data) 
    st.dataframe(metrics_data) 
else:
    st.info("左側のサイドバーで条件を入力し、「データを取得」ボタンを押してください。")