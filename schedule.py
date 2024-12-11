import os
import time
import warnings
import datetime
import pandas as pd
# from get_data.brands import *
from get_data.finance import *
from get_data.non_finance import *
from get_data.non_finance_2 import *
from get_data.devide_union import *
from get_data.config import db_config
from sqlalchemy import inspect # , Table, MetaData

warnings.filterwarnings("ignore", category=FutureWarning)

#  DBエンジンのインスタンスを作成
conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(conn_string)
# Inspectorを使用してテーブルの存在確認
inspector = inspect(engine)
stock_table = "stock_prices"
# metadata = MetaData(bind=engine)
# table = Table(stock_table, metadata, autoload_with=engine)

# now = datetime.datetime.now().strftime('%Y%m%d')
now = datetime.date.today().strftime('%Y%m%d')
# yesterday = (datetime.datetime.now().date() - datetime.timedelta(days=1))

# stock_pricesの取得開始期間の設定
stock_period = (datetime.date.today() - datetime.timedelta(days=5)).strftime('%Y%m%d') # '2024-11-20'

# non_financial_infoの取得開始期間の設定
non_financial_period = 10

def retry_with_timeout(func, max_retries=3, delay=5):
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise e
            print(f"リトライ {attempt + 1}/{max_retries}: {e}")
            time.sleep(delay * (attempt + 1)) # 遅延時間を徐々に増やす

def main():
    # # 上場銘柄を取得して保存
    # listing_df = fetch_listing_stocks()
    # if not listing_df.empty:
    #     listing_df.to_sql("listing_stocks", engine, if_exists="append", index=False)
    #     listing_df.to_csv('./input/listing/' + now.strftime('%Y%m%d') + '_listing_stocks.csv')
    
    # # 廃止銘柄を取得して保存
    # delisted_df = fetch_delisted_stocks()
    # if not delisted_df.empty:
    #     listing_df.to_sql("listing_stocks", engine, if_exists="append", index=False)
    #     delisted_df.to_csv('./input/delisted/' + now.strftime('%Y%m%d') + '_delisted_df.csv')


    # """財務情報の取得"""
    # 東証上場銘柄一覧を読み込む
    # JPXから銘柄コード一覧をダウンロード
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    # 保存先ディレクトリとファイル名
    save_dir = "./input/finance_data/"
    save_path = os.path.join(save_dir, "raw_jpx_codes.xls")
    os.makedirs(save_dir, exist_ok=True)

    try:
        r = requests.get(url)
        r.raise_for_status()
        with open(save_path, "wb") as jpx:
            jpx.write(r.content)
    except requests.exceptions.RequestException as e:
        print(f"銘柄コード一覧の取得に失敗しました：{e}")

    is_file = os.path.isfile('./input/finance_data/raw_jpx_codes.xls')
    if is_file:
        print('東証上場銘柄一覧の読み込み開始')
        raw_stock_lists = pd.read_excel("./input/finance_data/raw_jpx_codes.xls")
    else:
        print('指定フォルダ内にファイルがあることを確認')
        exit()

    # 企業情報の指標、財務状況を保存するデータフレームの作成
    origin_df_all_stock_prices = pd.read_csv("input/finance_data/origin/stock_prices.csv", encoding="shift-jis")
    origin_df_all_company_metrics = pd.read_csv("input/finance_data/origin/company_metrics.csv", encoding="shift-jis")
    origin_df_all_company_financial_info = pd.read_csv("input/finance_data/origin/company_financial_info.csv")

    # # 株式分割・併合した際の株価調整時に使用
    # merge_df = pd.read_csv("input/finance_data/merge_split/merge_df.csv", encoding="shift-jis")
    # split_df = pd.read_csv("input/finance_data/merge_split/split_df.csv", encoding="shift-jis")
    
    # 企業情報の指標、財務状況を保存するデータフレームの作成
    df_all_stock_prices_tmp = pd.DataFrame()
    missed_all_stock_prices_tmp = pd.DataFrame()

    df_all_company_metrics_tmp = pd.DataFrame()
    missed_all_company_metrics_tmp = pd.DataFrame()

    df_all_company_financial_info_tmp = pd.DataFrame()
    missed_all_company_financial_info_tmp = pd.DataFrame()

    # 企業の銘柄名、市場・商品区分、33業種、17業種を保持
    series_ticker_name = pd.Series()
    series_market_product_category = pd.Series()
    series_type_33 = pd.Series()
    series_type_17 = pd.Series()
    
    gfd = GetFinanceData()
    stock_lists = gfd.preprocess_stock_lists(raw_stock_lists)
    
    # tickerの企業情報の指標、財務状況を取得
    for ticker in stock_lists['コード']:
        time.sleep(5)
        # 証券コードに「.T」を追加
        jpx_filter_df = stock_lists[stock_lists['コード'] == ticker]
        ticker_num = str(ticker) + '.T'
        
        try:
            ticker_data = Ticker(ticker_num)
            print(f"{ticker_num}の処理開始")
        except ChunkedEncodingError:
            print(f"リトライ:{ticker_num}")
            time.sleep(10)
        except Exception as e:
            print(f"Other error: {e}")

        series_ticker_name = pd.concat([series_ticker_name, jpx_filter_df['銘柄名']])
        series_market_product_category = pd.concat([series_market_product_category, jpx_filter_df['市場・商品区分']])
        series_type_33 = pd.concat([series_type_33, jpx_filter_df['33業種区分']])
        series_type_17 = pd.concat([series_type_17, jpx_filter_df['17業種区分']])

        # 企業情報の取得
        try:
            df_stock_prices_tmp, missed_stock_prices_tmp = retry_with_timeout(lambda: gfd.get_stock_prices(ticker_num, ticker_data, stock_period))
            df_all_stock_prices_tmp = pd.concat([df_all_stock_prices_tmp, df_stock_prices_tmp])
            missed_all_stock_prices_tmp = pd.concat([missed_all_stock_prices_tmp, missed_stock_prices_tmp])
        except Exception as e:
            print(f"{ticker_num}の株価情報取得中にエラーが発生しました: {e}")
            continue

        try:
            df_company_metrics_tmp, missed_company_metrics_tmp = retry_with_timeout(lambda: gfd.get_company_metrics(ticker_num, ticker_data))
            df_all_company_metrics_tmp = pd.concat([df_all_company_metrics_tmp, df_company_metrics_tmp])
            missed_all_company_metrics_tmp = pd.concat([missed_all_company_metrics_tmp, missed_company_metrics_tmp])
        except Exception as e:
            print(f"{ticker_num}の財務指標取得中にエラーが発生しました: {e}")
            continue

        try:
            df_company_financial_info_tmp, missed_company_finacial_info_tmp = retry_with_timeout(lambda: gfd.get_company_finacial_info(ticker_num, ticker_data))
            df_all_company_financial_info_tmp = pd.concat([df_all_company_financial_info_tmp, df_company_financial_info_tmp])
            missed_all_company_financial_info_tmp = pd.concat([missed_all_company_financial_info_tmp, missed_company_finacial_info_tmp])
        except Exception as e:
            print(f"{ticker_num}の財務情報取得中にエラーが発生しました: {e}")
            continue

    # 不要な列の削除を行い、CSVファイルに保存
    os.makedirs('./input/finance_data/tmp', exist_ok=True)
    df_all_stock_prices_tmp['date'] = gfd.preprocess_date(df_all_stock_prices_tmp['date'])
    df_all_stock_prices_tmp = df_all_stock_prices_tmp.drop('index', axis=1)
    df_all_stock_prices_tmp.to_csv(f'./input/finance_data/tmp/{now}_stock_prices.csv', encoding='cp932', index=False, errors='ignore')
    df_all_stock_prices_tmp.to_sql("stock_prices", engine, if_exists="append", index=False)  
    
    missed_all_stock_prices_tmp.to_csv(f'./input/finance_data/tmp/{now}_missed_stock_prices.csv', index=False, errors='ignore')
    missed_all_company_metrics_tmp.to_csv(f'./input/finance_data/tmp/{now}_missed_company_metrics.csv', index=False, errors='ignore')
    missed_all_company_financial_info_tmp.to_csv(f'./input/finance_data/tmp/{now}_missed_company_financial_info.csv', index=False, errors='ignore')
    
    # 株式分割・併合があれば株価データを調整
    # 修正値が反映される日（split_date）の銘柄をピックアップして、前日（last_date_with_rights）以前のデータを修正
    os.makedirs('./input/devide_union', exist_ok=True)
    devide_df = fetch_split_data()
    union_df = fetch_merge_data()
    devide_union_df = pd.concat([devide_df, union_df])
    devide_union_df.to_sql("devide_union_data", engine, if_exists="append", index=False)
    devide_union_df.to_csv('./input/devide_union/devide_union_df.csv', index=False)

    os.makedirs('./input/finance_data/merge', exist_ok=True)
    try:
        # Inspectorを使用してテーブルの存在確認
        inspector = inspect(engine)
        # 1回目のみ実行（adjusted_stock_pricesにデータがない場合に実行）
        if 'adjusted_stock_prices' not in inspector.get_table_names(schema='public'): 
            df_all_stock_prices = pd.concat([origin_df_all_stock_prices, df_all_stock_prices_tmp]).drop_duplicates().reset_index() 
            df_all_stock_prices['date'] = gfd.preprocess_date(df_all_stock_prices['date'])
            df_all_stock_prices.to_sql("stock_prices", engine, if_exists="replace", index=False) # 修正後の株価をテーブルに保存
            adjusted_df, filtered_df = adjust_stock_prices(df_all_stock_prices, devide_union_df) # 修正する関数の適用
            # adjusted_df['date'] = gfd.preprocess_date(adjusted_df['date'])
            adjusted_df.to_sql("adjusted_stock_prices", engine, if_exists="replace", index=False) # 修正後の株価をテーブルに保存
            adjusted_df.to_csv('./input/finance_data/merge/stock_prices.csv', encoding='cp932', index=False, errors='ignore')
            filtered_df.to_sql("applied_data", engine, if_exists="append", index=False) # 修正した銘柄を保存
            filter_stock_query = "SELECT * FROM applied_data;" 
            all_filtered_df = pd.read_sql(filter_stock_query, engine)
            all_filtered_df = pd.concat([all_filtered_df, filtered_df])
            all_filtered_df.to_csv('./input/finance_data/merge/missed_stock_prices.csv', encoding='cp932', index=False, errors='ignore')
        # ２回目からは修正済みのデータ（adjusted_stock_prices）を修正
        elif 'adjusted_stock_prices' in inspector.get_table_names(schema='public'):
            stock_query = "SELECT * FROM adjusted_stock_prices;" 
            adjusted_stock_prices = pd.read_sql(stock_query, engine)
            df_all_stock_prices = pd.concat([adjusted_stock_prices, df_all_stock_prices_tmp]).drop_duplicates().reset_index() 
            df_all_stock_prices['date'] = gfd.preprocess_date(df_all_stock_prices['date'])
            df_all_stock_prices.to_sql("stock_prices", engine, if_exists="replace", index=False) # 修正後の株価をテーブルに保存
            adjusted_df, filtered_df = adjust_stock_prices(df_all_stock_prices, devide_union_df) # 修正する関数の適用
            # adjusted_df['date'] = gfd.preprocess_date(adjusted_df['date'])
            adjusted_df.to_sql("adjusted_stock_prices", engine, if_exists="replace", index=False) # 修正後の株価をテーブルに保存
            adjusted_df.to_csv('./input/finance_data/merge/stock_prices.csv', encoding='cp932', index=False, errors='ignore')
            filtered_df.to_sql("applied_data", engine, if_exists="append", index=False) # 修正した銘柄を保存
            filter_stock_query = "SELECT * FROM applied_data;" 
            all_filtered_df = pd.read_sql(filter_stock_query, engine)
            all_filtered_df = pd.concat([all_filtered_df, filtered_df])
            all_filtered_df.to_csv('./input/finance_data/merge/missed_stock_prices.csv', encoding='cp932', index=False, errors='ignore')
        else:
            print('株価データが保存されているか確認してください')
    except Exception as e:
        print("株価データ取得中にエラーが発生しました:", e)

    # 列の追加、削除を行いCSVファイルに保存
    df_all_company_metrics_tmp['ticker_name'] = series_ticker_name.values
    df_all_company_metrics_tmp['market_product_category'] = series_market_product_category.values
    df_all_company_metrics_tmp['type_33'] = series_type_33.values
    df_all_company_metrics_tmp['type_17'] = series_type_17.values
    df_all_company_metrics_tmp['exDividendDate'] = gfd.preprocess_date(df_all_company_metrics_tmp['exDividendDate'] )
    # df_all_company_metrics_tmp['exDividendDate'] = pd.to_datetime(df_all_company_metrics_tmp['exDividendDate'], errors='coerce').dt.date # datetime64に変換
    # df_all_company_metrics_tmp['exDividendDate'] = pd.to_datetime(df_all_company_metrics_tmp['exDividendDate'], errors='coerce')
    df_all_company_metrics_tmp.to_csv(f'./input/finance_data/tmp/{now}_company_metrics.csv', encoding='cp932', index=False, errors='ignore')
    df_all_company_metrics_tmp.to_sql("metrics", engine, if_exists="append", index=False)    
    df_all_company_metrics = pd.concat([origin_df_all_company_metrics, df_all_company_metrics_tmp])
    df_all_company_metrics.to_csv('./input/finance_data/merge/company_metrics.csv', encoding='cp932', index=False, errors='ignore')

    # 欠損率の計算と95%以上の欠損がある列を特定し削除した後にCSVファイルに保存
    info_list = ['symbol','asOfDate','periodType','currencyCode','BasicAverageShares','BasicEPS','CostOfRevenue','DilutedAverageShares',
            'DilutedEPS','DilutedNIAvailtoComStockholders','EBIT','EBITDA','GeneralAndAdministrativeExpense','GrossProfit',
            'InterestExpense','InterestExpenseNonOperating','InterestIncome','InterestIncomeNonOperating','MinorityInterests',
            'NetIncome','NetIncomeCommonStockholders','NetIncomeContinuousOperations','NetIncomeFromContinuingAndDiscontinuedOperation',
            'NetIncomeFromContinuingOperationNetMinorityInterest','NetIncomeIncludingNoncontrollingInterests','NetInterestIncome',
            'NetNonOperatingInterestIncomeExpense','NormalizedEBITDA','NormalizedIncome','OperatingExpense','OperatingIncome',
            'OperatingRevenue','OtherNonOperatingIncomeExpenses','OtherSpecialCharges','OtherunderPreferredStockDividend',
            'PretaxIncome','ReconciledCostOfRevenue','ReconciledDepreciation','SellingGeneralAndAdministration','SpecialIncomeCharges',
            'TaxEffectOfUnusualItems','TaxProvision','TaxRateForCalcs','TotalExpenses','TotalOperatingIncomeAsReported','TotalRevenue',
            'TotalUnusualItems','TotalUnusualItemsExcludingGoodwill','WriteOff','BeginningCashPosition','CashDividendsPaid',
            'ChangeInCashSupplementalAsReported','ChangeInInventory','ChangeInOtherCurrentAssets','ChangeInOtherCurrentLiabilities',
            'ChangeInPayable','ChangeInReceivables','ChangeInWorkingCapital','ChangesInCash','CommonStockDividendPaid','Depreciation',
            'DepreciationAndAmortization','EffectOfExchangeRateChanges','EndCashPosition','FinancingCashFlow','FreeCashFlow',
            'GainLossOnInvestmentSecurities','InterestPaidCFO','InterestReceivedCFO','InvestingCashFlow','IssuanceOfDebt',
            'LongTermDebtIssuance','LongTermDebtPayments','NetBusinessPurchaseAndSale','NetCommonStockIssuance','NetIncomeFromContinuingOperations',
            'NetInvestmentPurchaseAndSale','NetIssuancePaymentsOfDebt','NetLongTermDebtIssuance','NetOtherFinancingCharges','NetOtherInvestingChanges',
            'NetShortTermDebtIssuance','OperatingCashFlow','OtherCashAdjustmentOutsideChangeinCash','OtherNonCashItems','PurchaseOfInvestment',
            'RepaymentOfDebt','SaleOfBusiness','SaleOfInvestment','TaxesRefundPaid','AccountsPayable','AccountsReceivable',
            'AdditionalPaidInCapital','AvailableForSaleSecurities','BuildingsAndImprovements','CapitalLeaseObligations','CapitalStock',
            'CashAndCashEquivalents','CashCashEquivalentsAndShortTermInvestments','CommonStock','CommonStockEquity','ConstructionInProgress',
            'CurrentAssets','CurrentCapitalLeaseObligation','CurrentDebt','CurrentDebtAndCapitalLeaseObligation','CurrentLiabilities','FinishedGoods',
            'Goodwill','GoodwillAndOtherIntangibleAssets','GrossAccountsReceivable','GrossPPE','Inventory','InvestedCapital',
            'InvestmentinFinancialAssets','LandAndImprovements','LongTermCapitalLeaseObligation','LongTermDebt','LongTermDebtAndCapitalLeaseObligation',
            'LongTermProvisions','MachineryFurnitureEquipment','MinorityInterest','NetDebt','NetPPE','NetTangibleAssets','NonCurrentDeferredTaxesAssets',
             'NonCurrentDeferredTaxesLiabilities','NonCurrentPensionAndOtherPostretirementBenefitPlans','OrdinarySharesNumber','OtherCurrentAssets',
            'OtherCurrentLiabilities','OtherIntangibleAssets','OtherNonCurrentAssets','OtherNonCurrentLiabilities','OtherPayable','OtherProperties',
            'Payables','PensionandOtherPostRetirementBenefitPlansCurrent','Properties','RawMaterials','RetainedEarnings','ShareIssued','StockholdersEquity',
            'TangibleBookValue','TotalAssets','TotalCapitalization','TotalDebt','TotalEquityGrossMinorityInterest','TotalLiabilitiesNetMinorityInterest',
            'TotalNonCurrentAssets','TotalNonCurrentLiabilitiesNetMinorityInterest','TotalTaxPayable','TradeandOtherPayablesNonCurrent','TreasurySharesNumber',
            'TreasuryStock','WorkInProcess','WorkingCapital','CurrentProvisions','EnterpriseValue','EnterprisesValueEBITDARatio','EnterprisesValueRevenueRatio',
            'MarketCap','PbRatio','PeRatio','PsRatio','capitalAdequacyRatio','ROE','CapitalExpenditure','ChangeInPrepaidAssets','CommonStockIssuance',
            'GainLossOnSaleOfPPE','IssuanceOfCapitalStock','NetIntangiblesPurchaseAndSale','NetPPEPurchaseAndSale','PurchaseOfIntangibles','PurchaseOfPPE',
            'PrepaidAssets','DepreciationAndAmortizationInIncomeStatement','DepreciationIncomeStatement','OtherOperatingExpenses','RestructuringAndMergernAcquisition',
            'AmortizationCashFlow','PurchaseOfBusiness','SaleOfPPE','AccumulatedDepreciation','DefinedPensionBenefit','LongTermEquityInvestment','OtherShortTermInvestments',
            'CommonStockPayments','RepurchaseOfCapitalStock','NonCurrentPrepaidAssets','OtherEquityInterest','TaxesReceivable','OtherReceivables',
            'NetForeignCurrencyExchangeGainLoss','FixedAssetsRevaluationReserve']
    df_all_company_financial_info_tmp = df_all_company_financial_info_tmp[info_list]
    df_all_company_financial_info_tmp['asOfDate'] = gfd.preprocess_date(df_all_company_financial_info_tmp['asOfDate'])
    df_all_company_financial_info_tmp.to_csv(f'./input/finance_data/tmp/{now}_company_financial_info.csv', encoding='cp932', index=False, errors='ignore')
    df_all_company_financial_info_tmp.to_sql("financial_info", engine, if_exists="append", index=False)    
    df_all_company_financial_info = pd.concat([origin_df_all_company_financial_info, df_all_company_financial_info_tmp])
    df_all_company_financial_info.to_csv('./input/finance_data/merge/company_financial_info.csv', encoding='cp932', index=False, errors='ignore')


    """非財務情報の取得"""
    # 最新のdocIDのデータを取得
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days= non_financial_period)
    gd = GetDocid(start_date, end_date)
    ID_dir_tmp = "./input/non-finance_data/docID"
    df_report_tmp, edinet_df_tmp = gd.create_docid_df(ID_dir_tmp)
    os.makedirs(f'{ID_dir_tmp}/tmp/report', exist_ok=True)
    os.makedirs(f'{ID_dir_tmp}/tmp/edinet_ID', exist_ok=True)
    df_report_tmp.to_csv(f"{ID_dir_tmp}/tmp/report/{now}_EdinetReport.csv", encoding="cp932", index=False)
    edinet_df_tmp.to_csv(f"{ID_dir_tmp}/tmp/edinet_ID/{now}_edinet_df.csv",index=False)
    # # 過去のデータと結合して保存
    # origin_df_report = pd.read_csv(f'{ID_dir_tmp}/origin/EdinetReport.csv')
    # merge_df_report = pd.concat([origin_df_report, df_report_tmp])
    # merge_df_report.to_csv(f"{ID_dir_tmp}/merge/latest_EdinetReport.csv", encoding="cp932")
    # origin_edinet_df = pd.read_csv(f'{ID_dir_tmp}/origin/edinet_df.csv')
    # merge_edinet_df = pd.concat([origin_edinet_df, edinet_df_tmp])
    # merge_edinet_df.to_csv(f"{ID_dir_tmp}/merge/latest_edinet_df.csv",index=False)

    # docIDをもとに非財務情報を取得
    docid_list_tmp = edinet_df_tmp["docID"].tolist()
    gcfe = GetCsvFromEdinet(keys, docid_list_tmp)
    gcfe.get_csv_file()
    os.makedirs('./input/non-finance_data/doc/tmp_2', exist_ok=True)
    filename = f'./input/non-finance_data/doc/tmp_2/{now}_non-financial_data.csv'
    text_data_tmp = gcfe.get_text_data()
    text_data_tmp.to_csv(filename, index=False)
    # edinet_dfと結合
    non_financial_df_tmp = pd.merge(edinet_df_tmp, text_data_tmp, on='docID', how='outer')
    non_financial_df_tmp = non_financial_df_tmp.drop_duplicates()
    os.makedirs('./input/non-finance_data/text_data/tmp', exist_ok=True)
    non_financial_df_tmp.to_csv(f'./input/non-finance_data/text_data/tmp/{now}_text_data.csv', index=False)

    # ファイルが存在するか確認
    # チェックするCSVファイルのパス
    latest_csv_path = './input/non-finance_data/text_data/merge/latest_text_data.csv'
    if not os.path.exists(latest_csv_path):
        # 1回目
        origin_non_financial_df = pd.read_csv('./input/non-finance_data/text_data/origin/20241007_text_data.csv', low_memory=False)
        latest_non_financial_df = pd.concat([origin_non_financial_df, non_financial_df_tmp])
    else:
        # ２回目以降
        non_financial_df = pd.read_csv(latest_csv_path,  low_memory=False)
        latest_non_financial_df = pd.concat([non_financial_df, non_financial_df_tmp])
    
    os.makedirs('./input/non-finance_data/text_data/merge', exist_ok=True)
    latest_non_financial_df.to_csv('./input/non-finance_data/text_data/merge/latest_text_data.csv', index=False)
    latest_non_financial_df.to_sql("non_financial_data", engine, if_exists="append", index=False)    

    # 短期間のスクリプトや接続数制限が厳しい環境では推奨。Webアプリケーションのように長期間稼働するシステムでは、エンジンの管理がアプリケーション全体で行われるため、通常は不要
    engine.dispose()

if __name__ == '__main__':
    start = time.perf_counter() #計測開始
    main()
    end = time.perf_counter() #計測終了
    print('{:.2f}'.format((end-start)/60)) # 87.97(秒→分に直し、小数点以下の桁数を指定して出力)