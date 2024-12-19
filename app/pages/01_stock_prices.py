import sys
import os
import datetime
import numpy as np
import streamlit as st
import plotly.graph_objs as go
from plotly.subplots import make_subplots
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.function import *

st.set_page_config(layout="wide")

#  DBからデータを取得
conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(conn_string)

stock_table = 'adjusted_stock_prices'

# 条件入力UI
st.sidebar.header("検索条件")
raw_symbol = st.sidebar.text_input("シンボル", value="1301")
symbol = raw_symbol + '.T'
start_date = st.sidebar.date_input("開始日", (datetime.datetime.today() - datetime.timedelta(days=700)))
end_date = st.sidebar.date_input("終了日", datetime.datetime.today())

# データベースからデータを取得
if st.sidebar.button("データを取得"): # SQLでは、テーブル名をバインドパラメータとして扱うえない。→SQLの構文解析とクエリプランの生成が、実行前に行われる必要があるため
    
    # SQLクエリとパラメータの定義
    stock_prices_query = text(f"""
        SELECT *
        FROM {stock_table}
        WHERE symbol = :symbol
        AND "date"::date BETWEEN :start_date AND :end_date
    """)
    stock_prices_params = {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date
    }

    # データの取得
    try:
        stock_prices_data = fetch_data(stock_prices_query, stock_prices_params)
        # データをセッション状態に保存
        st.session_state.stock_prices_data = stock_prices_data
    except Exception as e:
        st.error(f"データ取得中にエラーが発生しました: {e}")

# 株価と出来高
st.header("株価ローソク足・出来高")

# データがあるかチェックし、プロット
if 'stock_prices_data' in locals():

    # サブプロットでローソク足と出来高プロファイルを表示
    fig = make_subplots(
        rows=2, cols=2, 
        column_widths=[0.8, 0.2], 
        shared_yaxes=True,
        horizontal_spacing=0.02,
        subplot_titles=("株価ローソク足チャート", "出来高プロファイル")
    )

    fig.add_trace(
        go.Candlestick(
            x=stock_prices_data["date"],
            open=stock_prices_data["open"],
            high=stock_prices_data["high"],
            low=stock_prices_data["low"],
            close=stock_prices_data["close"],
            name="ローソク足"
        ),
        row=1, col=1
    )

    # 出来高プロファイル用のヒストグラムデータ作成
    stock_prices_data["close"] = pd.to_numeric(stock_prices_data["close"])
    stock_prices_data["volume"] = pd.to_numeric(stock_prices_data["volume"])
    hist, bin_edges = np.histogram(stock_prices_data["close"], bins=20, weights=stock_prices_data["volume"])
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

    # 出来高プロファイル（横向きバー）
    fig.add_trace(
        go.Bar(
            x=hist,
            y=bin_centers,
            orientation='h',
            marker=dict(color='lightblue'),
            name="出来高"
        ),
        row=1, col=2
    )
    

    # 出来高（縦向きバー）を下段に追加
    fig.add_trace(
        go.Bar(
            x=stock_prices_data["date"],
            y=stock_prices_data["volume"],
            name="出来高",
            marker=dict(color="lightblue")
        ),
        row=2, col=1
    )

    fig.update_layout(
        title=f"{symbol} の株価ローソク足と出来高 ({start_date} 〜 {end_date})",
        yaxis_title="株価",
        xaxis_title="期間",
        xaxis2_title="出来高",
        xaxis_rangeslider_visible=False,
        showlegend=False
    )
    # 出来高プロファイルのx軸を非表示
    fig.update_xaxes(visible=False, row=1, col=2)
    fig.update_xaxes(visible=False, row=2, col=1)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("参照データ")
    st.dataframe(stock_prices_data)

    # バックテスト機能
    st.sidebar.header("バックテスト設定")
    strategy = st.sidebar.selectbox("戦略を選択", ["移動平均線クロス", "RSI"])
    short_window = st.sidebar.number_input("短期移動平均線 (日数)", min_value=1, value=5)
    long_window = st.sidebar.number_input("長期移動平均線 (日数)", min_value=1, value=20)
    rsi_threshold = st.sidebar.slider("RSI閾値", min_value=0, max_value=100, value=(30, 70))
    initial_cash = st.sidebar.number_input("初期資金", min_value=10000, value=1000000)


else:
    st.info("左側のサイドバーで条件を入力し、「データを取得」ボタンを押してください。")

if st.sidebar.button("バックテスト実行"):
    try:
        # データの準備
        stock_prices_data = st.session_state.stock_prices_data
        stock_prices_data["date"] = pd.to_datetime(stock_prices_data["date"])
        stock_prices_data.set_index("date", inplace=True)
        
        # 移動平均線クロス戦略
        if strategy == "移動平均線クロス":
            stock_prices_data["short_ma"] = stock_prices_data["close"].rolling(window=short_window).mean()
            stock_prices_data["long_ma"] = stock_prices_data["close"].rolling(window=long_window).mean()
            stock_prices_data["signal"] = np.where(stock_prices_data["short_ma"] > stock_prices_data["long_ma"], 1, 0)
            stock_prices_data["position"] = stock_prices_data["signal"].diff()

        # RSI戦略
        elif strategy == "RSI":
            delta = stock_prices_data["close"].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            stock_prices_data["RSI"] = 100 - (100 / (1 + gain / loss))
            stock_prices_data["signal"] = np.where(stock_prices_data["RSI"] < rsi_threshold[0], 1, 
                                                np.where(stock_prices_data["RSI"] > rsi_threshold[1], -1, 0))
            stock_prices_data["position"] = stock_prices_data["signal"].diff()

        # バックテストの実行
        cash = initial_cash
        position = 0
        trade_log = []

        for i, row in stock_prices_data.iterrows():
            if row["position"] == 1:  # 買いシグナル
                position = cash / row["close"]
                cash = 0
                trade_log.append({"date": i, "action": "買い", "price": row["close"]})
            elif row["position"] == -1:  # 売りシグナル
                cash = position * row["close"]
                position = 0
                trade_log.append({"date": i, "action": "売り", "price": row["close"]})

        # 最終的な資産計算
        final_value = cash + (position * stock_prices_data["close"].iloc[-1] if position > 0 else 0)
        profit = final_value - initial_cash

        # 結果表示
        st.subheader("バックテスト結果")
        st.write(f"最終資産: {final_value:,.2f} 円")
        st.write(f"総利益: {profit:,.2f} 円")
        st.write(f"トレード回数: {len(trade_log)} 回")
        st.dataframe(pd.DataFrame(trade_log))

        # パフォーマンスチャート
        stock_prices_data["portfolio"] = cash + (position * stock_prices_data["close"] if position > 0 else 0)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=stock_prices_data.index, y=stock_prices_data["close"], name="株価"))
        fig.add_trace(go.Scatter(x=stock_prices_data.index, y=stock_prices_data["portfolio"], name="ポートフォリオ価値"))
        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"バックテスト中にエラーが発生しました: {e}")
