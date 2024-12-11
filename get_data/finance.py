"""
yahooqueryの取得明細情報
    history：過去の株価データ、分配金の支払金額、株式分割情報
    B/S:バランスシート（直近４年分、直近四半期分:2024年分）
    キャッシュフロー（直近４年分、直近四半期分:2024年分）
    財務諸表（直近４年分、直近四半期分:2024年分）
    4年分のその他評価指標

データ取得対象
    プライム（内国株式）
    スタンダード（内国株式）
    グロース（内国株式）
    グロース（外国株式）
    スタンダード（外国株式）
    プライム（外国株式）
    ※外国株式:アメリカなどを設立地とし、それぞれの母国等に既に上場している世界有数の大企業の他、東証だけに上場している企業(単独上場会社)がある。

対象外
    PRO Market：株を買うことができる投資家を、株式投資の知識や経験が豊富なプロ投資家（＝特定投資家）に限定
    REIT・ベンチャーファンド・カントリーファンド・インフラファンド
    ETF・ETN
    出資証券
    コード:25935(伊藤園第1種優先株式)：優先株式とは、普通株式に比べて利益の配当等を優先的に受け取ることができる株式

input
└── finance_data
    ├── company_financial_info.csv
    ├── company_metrics.csv
    └── stock_prices.csv

# 東証上場銘柄一覧に記載された証券コードの企業の財務指標と財務情報を取得
# 出力ファイルの一覧
# - company_metrics.csv
#    証券コード,1株当りの配当金,配当利回り,過去5年間の配当利回り平均,配当性向,時価総額
# - company_financial_info.csv
#    証券コード,売上高,純利益,総資産,自己資本比率,自己資本利益率

"""

import os
import time
import numpy as np
import pandas as pd
import datetime
from yahooquery import Ticker
from requests.exceptions import ChunkedEncodingError
from sqlalchemy import create_engine
from get_data.config import db_config

#  DBエンジンのインスタンスを作成
conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(conn_string)

# # JPXから銘柄コード一覧をダウンロード
# url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
# # 保存先ディレクトリとファイル名
# save_dir = "./input/finance_data/"
# save_path = os.path.join(save_dir, "raw_jpx_codes.xls")
# os.makedirs(save_dir, exist_ok=True)
# try:
#     r = requests.get(url)
#     r.raise_for_status()
#     with open(save_path, "wb") as jpx:
#         jpx.write(r.content)
# except requests.exceptions.RequestException as e:
#     print(f"エラー：{e}")

income_statement_annual_columns = ['symbol', 'asOfDate', 'periodType', 'currencyCode',
                                'AverageDilutionEarnings', 'BasicAverageShares', 'BasicEPS',
                                'CostOfRevenue', 'DilutedAverageShares', 'DilutedEPS',
                                'DilutedNIAvailtoComStockholders', 'EBIT', 'EBITDA',
                                'EarningsFromEquityInterest', 'EarningsFromEquityInterestNetOfTax',
                                'GainOnSaleOfSecurity', 'GrossProfit', 'InterestExpense',
                                'InterestExpenseNonOperating', 'InterestIncome',
                                'InterestIncomeNonOperating', 'MinorityInterests', 'NetIncome',
                                'NetIncomeCommonStockholders', 'NetIncomeContinuousOperations',
                                'NetIncomeFromContinuingAndDiscontinuedOperation',
                                'NetIncomeFromContinuingOperationNetMinorityInterest',
                                'NetIncomeIncludingNoncontrollingInterests', 'NetInterestIncome',
                                'NetNonOperatingInterestIncomeExpense', 'NormalizedEBITDA',
                                'NormalizedIncome', 'OperatingExpense', 'OperatingIncome',
                                'OperatingRevenue', 'OtherIncomeExpense',
                                'OtherNonOperatingIncomeExpenses', 'OtherunderPreferredStockDividend',
                                'PretaxIncome', 'ReconciledCostOfRevenue', 'ReconciledDepreciation',
                                'SellingGeneralAndAdministration', 'TaxEffectOfUnusualItems',
                                'TaxProvision', 'TaxRateForCalcs', 'TotalExpenses',
                                'TotalOperatingIncomeAsReported', 'TotalOtherFinanceCost',
                                'TotalRevenue', 'TotalUnusualItems',
                                'TotalUnusualItemsExcludingGoodwill']

income_statement_quarterly_columns = ['symbol', 'asOfDate', 'periodType', 'currencyCode',
                                    'AverageDilutionEarnings', 'BasicAverageShares', 'BasicEPS',
                                    'CostOfRevenue', 'DilutedAverageShares', 'DilutedEPS',
                                    'DilutedNIAvailtoComStockholders', 'EBIT', 'EBITDA',
                                    'EarningsFromEquityInterest', 'GainOnSaleOfSecurity', 'GrossProfit',
                                    'InterestExpense', 'InterestExpenseNonOperating', 'InterestIncome',
                                    'InterestIncomeNonOperating', 'MinorityInterests', 'NetIncome',
                                    'NetIncomeCommonStockholders', 'NetIncomeContinuousOperations',
                                    'NetIncomeFromContinuingAndDiscontinuedOperation',
                                    'NetIncomeFromContinuingOperationNetMinorityInterest',
                                    'NetIncomeIncludingNoncontrollingInterests', 'NetInterestIncome',
                                    'NetNonOperatingInterestIncomeExpense', 'NormalizedEBITDA',
                                    'NormalizedIncome', 'OperatingExpense', 'OperatingIncome',
                                    'OperatingRevenue', 'OtherIncomeExpense',
                                    'OtherNonOperatingIncomeExpenses', 'PretaxIncome',
                                    'ReconciledCostOfRevenue', 'ReconciledDepreciation',
                                    'SellingGeneralAndAdministration', 'TaxEffectOfUnusualItems',
                                    'TaxProvision', 'TaxRateForCalcs', 'TotalExpenses',
                                    'TotalOperatingIncomeAsReported', 'TotalRevenue', 'TotalUnusualItems',
                                    'TotalUnusualItemsExcludingGoodwill']

cash_flow_annual_columns = ['symbol', 'asOfDate', 'periodType', 'currencyCode',
                            'BeginningCashPosition', 'CapitalExpenditure', 'CashDividendsPaid',
                            'CashFlowFromContinuingFinancingActivities',
                            'CashFlowFromContinuingInvestingActivities',
                            'CashFlowFromContinuingOperatingActivities', 'ChangeInAccountPayable',
                            'ChangeInCashSupplementalAsReported', 'ChangeInIncomeTaxPayable',
                            'ChangeInInventory', 'ChangeInOtherCurrentAssets',
                            'ChangeInOtherCurrentLiabilities', 'ChangeInOtherWorkingCapital',
                            'ChangeInPayable', 'ChangeInPayablesAndAccruedExpense',
                            'ChangeInReceivables', 'ChangeInTaxPayable', 'ChangeInWorkingCapital',
                            'ChangesInAccountReceivables', 'ChangesInCash',
                            'CommonStockDividendPaid', 'CommonStockPayments', 'DeferredIncomeTax',
                            'DeferredTax', 'Depreciation', 'DepreciationAmortizationDepletion',
                            'DepreciationAndAmortization', 'DividendReceivedCFO',
                            'EarningsLossesFromEquityInvestments', 'EffectOfExchangeRateChanges',
                            'EndCashPosition', 'FinancingCashFlow', 'FreeCashFlow',
                            'GainLossOnSaleOfPPE', 'InterestPaidCFO', 'InterestReceivedCFO',
                            'InvestingCashFlow', 'IssuanceOfDebt', 'LongTermDebtIssuance',
                            'LongTermDebtPayments', 'NetBusinessPurchaseAndSale',
                            'NetCommonStockIssuance', 'NetIncome',
                            'NetIncomeFromContinuingOperations', 'NetIntangiblesPurchaseAndSale',
                            'NetInvestmentPurchaseAndSale', 'NetIssuancePaymentsOfDebt',
                            'NetLongTermDebtIssuance', 'NetOtherFinancingCharges',
                            'NetOtherInvestingChanges', 'NetPPEPurchaseAndSale',
                            'NetShortTermDebtIssuance', 'OperatingCashFlow', 'OperatingGainsLosses',
                            'OtherNonCashItems', 'PensionAndEmployeeBenefitExpense',
                            'ProvisionandWriteOffofAssets', 'PurchaseOfBusiness',
                            'PurchaseOfIntangibles', 'PurchaseOfInvestment', 'PurchaseOfPPE',
                            'RepaymentOfDebt', 'RepurchaseOfCapitalStock', 'SaleOfInvestment',
                            'SaleOfPPE', 'TaxesRefundPaid',
                            'UnrealizedGainLossOnInvestmentSecurities']

cash_flow_quarterly_columns = ['symbol', 'asOfDate', 'periodType', 'currencyCode',
                            'BeginningCashPosition', 'CapitalExpenditure', 'CashDividendsPaid',
                            'CashFlowFromContinuingFinancingActivities',
                            'CashFlowFromContinuingInvestingActivities',
                            'CashFlowFromContinuingOperatingActivities',
                            'ChangeInCashSupplementalAsReported', 'ChangeInWorkingCapital',
                            'ChangesInCash', 'CommonStockDividendPaid', 'DeferredIncomeTax',
                            'DeferredTax', 'DepreciationAmortizationDepletion',
                            'DepreciationAndAmortization', 'DividendReceivedCFO',
                            'EarningsLossesFromEquityInvestments', 'EffectOfExchangeRateChanges',
                            'EndCashPosition', 'FinancingCashFlow', 'FreeCashFlow',
                            'InterestPaidCFO', 'InterestReceivedCFO', 'InvestingCashFlow',
                            'IssuanceOfDebt', 'LongTermDebtIssuance', 'LongTermDebtPayments',
                            'NetCommonStockIssuance', 'NetIncome',
                            'NetIncomeFromContinuingOperations', 'NetIntangiblesPurchaseAndSale',
                            'NetInvestmentPurchaseAndSale', 'NetIssuancePaymentsOfDebt',
                            'NetLongTermDebtIssuance', 'NetOtherFinancingCharges',
                            'NetOtherInvestingChanges', 'NetPPEPurchaseAndSale',
                            'NetShortTermDebtIssuance', 'OperatingCashFlow', 'OperatingGainsLosses',
                            'OtherNonCashItems', 'PurchaseOfIntangibles', 'PurchaseOfInvestment',
                            'PurchaseOfPPE', 'RepaymentOfDebt', 'SaleOfInvestment', 'SaleOfPPE',
                            'TaxesRefundPaid']

balance_sheet_annual_columns = ['symbol', 'asOfDate', 'periodType', 'currencyCode', 'AccountsPayable',
                                'AccountsReceivable', 'AccumulatedDepreciation',
                                'AdditionalPaidInCapital', 'AllowanceForDoubtfulAccountsReceivable',
                                'AvailableForSaleSecurities', 'BuildingsAndImprovements',
                                'CapitalLeaseObligations', 'CapitalStock', 'CashAndCashEquivalents',
                                'CashCashEquivalentsAndShortTermInvestments', 'CashEquivalents',
                                'CashFinancial', 'CommercialPaper', 'CommonStock', 'CommonStockEquity',
                                'ConstructionInProgress', 'CurrentAccruedExpenses', 'CurrentAssets',
                                'CurrentCapitalLeaseObligation', 'CurrentDebt',
                                'CurrentDebtAndCapitalLeaseObligation', 'CurrentLiabilities',
                                'EmployeeBenefits', 'FinishedGoods',
                                'GainsLossesNotAffectingRetainedEarnings',
                                'GoodwillAndOtherIntangibleAssets', 'GrossAccountsReceivable',
                                'GrossPPE', 'IncomeTaxPayable', 'Inventory', 'InvestedCapital',
                                'InvestmentinFinancialAssets', 'InvestmentsAndAdvances',
                                'InvestmentsinAssociatesatCost', 'InvestmentsinJointVenturesatCost',
                                'LandAndImprovements', 'LongTermCapitalLeaseObligation', 'LongTermDebt',
                                'LongTermDebtAndCapitalLeaseObligation', 'LongTermEquityInvestment',
                                'MachineryFurnitureEquipment', 'MinorityInterest', 'NetDebt', 'NetPPE',
                                'NetTangibleAssets', 'NonCurrentAccountsReceivable',
                                'NonCurrentDeferredAssets', 'NonCurrentDeferredLiabilities',
                                'NonCurrentDeferredTaxesAssets', 'NonCurrentDeferredTaxesLiabilities',
                                'NonCurrentPensionAndOtherPostretirementBenefitPlans',
                                'OrdinarySharesNumber', 'OtherCurrentAssets', 'OtherCurrentBorrowings',
                                'OtherCurrentLiabilities', 'OtherEquityAdjustments',
                                'OtherEquityInterest', 'OtherIntangibleAssets', 'OtherNonCurrentAssets',
                                'OtherNonCurrentLiabilities', 'OtherPayable', 'OtherProperties',
                                'OtherReceivables', 'OtherShortTermInvestments', 'Payables',
                                'PayablesAndAccruedExpenses', 'PreferredSecuritiesOutsideStockEquity',
                                'PrepaidAssets', 'Properties', 'RawMaterials', 'Receivables',
                                'ReceivablesAdjustmentsAllowances', 'RetainedEarnings', 'ShareIssued',
                                'StockholdersEquity', 'TangibleBookValue', 'TaxesReceivable',
                                'TotalAssets', 'TotalCapitalization', 'TotalDebt',
                                'TotalEquityGrossMinorityInterest',
                                'TotalLiabilitiesNetMinorityInterest', 'TotalNonCurrentAssets',
                                'TotalNonCurrentLiabilitiesNetMinorityInterest', 'TotalTaxPayable',
                                'TreasurySharesNumber', 'TreasuryStock', 'WorkInProcess',
                                'WorkingCapital']

balance_sheet_quarterly_columns = ['symbol', 'asOfDate', 'periodType', 'currencyCode', 'AccountsPayable',
                                'AccountsReceivable', 'AccumulatedDepreciation',
                                'AdditionalPaidInCapital', 'AvailableForSaleSecurities',
                                'BuildingsAndImprovements', 'CapitalLeaseObligations', 'CapitalStock',
                                'CashAndCashEquivalents', 'CashCashEquivalentsAndShortTermInvestments',
                                'CashEquivalents', 'CashFinancial', 'CommercialPaper', 'CommonStock',
                                'CommonStockEquity', 'ConstructionInProgress', 'CurrentAccruedExpenses',
                                'CurrentAssets', 'CurrentCapitalLeaseObligation', 'CurrentDebt',
                                'CurrentDebtAndCapitalLeaseObligation', 'CurrentLiabilities',
                                'EmployeeBenefits', 'FinishedGoods', 'GoodwillAndOtherIntangibleAssets',
                                'GrossPPE', 'IncomeTaxPayable', 'Inventory', 'InvestedCapital',
                                'InvestmentinFinancialAssets', 'InvestmentsAndAdvances',
                                'InvestmentsinAssociatesatCost', 'InvestmentsinJointVenturesatCost',
                                'LandAndImprovements', 'LongTermCapitalLeaseObligation', 'LongTermDebt',
                                'LongTermDebtAndCapitalLeaseObligation', 'LongTermEquityInvestment',
                                'MachineryFurnitureEquipment', 'MinorityInterest', 'NetDebt', 'NetPPE',
                                'NetTangibleAssets', 'NonCurrentAccountsReceivable',
                                'NonCurrentDeferredAssets', 'NonCurrentDeferredLiabilities',
                                'NonCurrentDeferredTaxesAssets', 'NonCurrentDeferredTaxesLiabilities',
                                'NonCurrentPensionAndOtherPostretirementBenefitPlans',
                                'OrdinarySharesNumber', 'OtherCurrentAssets', 'OtherCurrentBorrowings',
                                'OtherCurrentLiabilities', 'OtherEquityInterest',
                                'OtherIntangibleAssets', 'OtherNonCurrentAssets',
                                'OtherNonCurrentLiabilities', 'OtherPayable', 'OtherProperties',
                                'OtherReceivables', 'OtherShortTermInvestments', 'Payables',
                                'PayablesAndAccruedExpenses', 'Properties', 'RawMaterials',
                                'Receivables', 'ReceivablesAdjustmentsAllowances', 'RetainedEarnings',
                                'ShareIssued', 'StockholdersEquity', 'TangibleBookValue',
                                'TaxesReceivable', 'TotalAssets', 'TotalCapitalization', 'TotalDebt',
                                'TotalEquityGrossMinorityInterest',
                                'TotalLiabilitiesNetMinorityInterest', 'TotalNonCurrentAssets',
                                'TotalNonCurrentLiabilitiesNetMinorityInterest', 'TotalTaxPayable',
                                'TreasurySharesNumber', 'TreasuryStock', 'WorkInProcess',
                                'WorkingCapital']

valuation_measures_columns = ['symbol', 'asOfDate', 'periodType', 'EnterpriseValue',
                            'EnterprisesValueEBITDARatio', 'EnterprisesValueRevenueRatio',
                            'ForwardPeRatio', 'MarketCap', 'PbRatio', 'PeRatio', 'PegRatio',
                            'PsRatio']
    

# テキストデータの名称とXBRL上のcodeを、keysという辞書で渡す
class GetFinanceData:
        
    def preprocess_stock_lists(self, raw_stock_lists):
        # ETF、ETNを除外
        stock_lists = raw_stock_lists[raw_stock_lists['市場・商品区分'] != 'ETF・ETN']
        # PRO Marketを除外
        stock_lists = stock_lists[stock_lists['市場・商品区分'] != 'PRO Market']
        # REIT・ベンチャーファンド・カントリーファンド・インフラファンドを除外
        stock_lists = stock_lists[stock_lists['市場・商品区分'] != 'REIT・ベンチャーファンド・カントリーファンド・インフラファンド']
        # 出資証券を除外
        stock_lists = stock_lists[stock_lists['市場・商品区分'] != '出資証券']
        # 伊藤園の優先株を除外
        stock_lists = stock_lists[stock_lists['コード'] != 25935]

        return stock_lists

    # 企業の過去数年の株価を取得
    # 引数:証券コード、Tickerオブジェクト
    # 戻値:企業の株価指標:Dataframe 
    def get_stock_prices(self, ticker_num, ticker_data, start_day):
        df_stock_prices = pd.DataFrame()
        missed_stock_prices_data = pd.DataFrame(columns=["symbol", "ticker_data"])

        try:
            data = ticker_data.history(start=start_day, interval='1d').reset_index()
            df_stock_prices = pd.concat([df_stock_prices, data], ignore_index=True)  
        except Exception as e:
            print(f"{ticker_num} の株価取得中にエラー: {e}")
            missed_data = [{'symbol':ticker_num , 'ticker_data': ticker_data}]
            missed_data_df = pd.DataFrame(missed_data)
            missed_stock_prices_data = pd.concat([missed_stock_prices_data, missed_data_df], ignore_index=True)  
                
        return df_stock_prices, missed_stock_prices_data


    # 企業の財務データを取得
    # 引数:証券コード、Tickerオブジェクト
    # 戻値:企業の財務指標:Dataframe
    def get_company_metrics(self, ticker_num, ticker_data):
        # 企業の財務指標を保存するリスト
        company_metrics = [ticker_num]
        missed_company_metrics = pd.DataFrame(columns=['symbol', 'ticker_data', 'summary_detail_key'])

        summary_detail_keys = [
            # 'previousClose',# 前日の終値
            # 'open', # 今日の開始値
            # 'dayLow', # 本日の最安値
            # 'dayHigh', # 今日の最高値
            # 'regularMarketPreviousClose', # 通常取引市場における前日の終値
            # 'regularMarketOpen', # 通常取引市場における始値
            # 'regularMarketDayLow', # 通常取引市場における最安値
            # 'regularMarketDayHigh', # 通常取引市場における最高値
            'dividendRate', # 1株あたりの年間配当額
            'dividendYield', # 配当利回り
            'exDividendDate', # 配当落ち日
            'fiveYearAvgDividendYield', # 過去5年間の平均配当利回り
            'payoutRatio', # 配当性向(利益のうち配当に回される割合)
            'beta', # 株価変動率
            'trailingPE', # 過去12か月の実績に基づく株価収益率
            'forwardPE', # 予想される株価収益率
            # 'volume', # 今日の取引量
            # 'regularMarketVolume', # : 通常取引市場での取引量
            # 'averageVolume', # : 平均取引量
            # 'averageVolume10days', # 過去10日間の平均取引量
            # 'averageDailyVolume10Day', # 過去10日間の平均日次取引量
            'bidSize', # 買い注文の株数（0株、具体的な数が示されていない）
            'askSize', # 売り注文の株数（0株、具体的な数が示されていない）
            # 'fiftyTwoWeekLow', # 過去52週間の最安値
            # 'fiftyTwoWeekHigh', # 過去52週間の最高値
            # 'priceToSalesTrailing12Months', # 売上高に対する株価比率
            # 'fiftyDayAverage', # 過去50日間の平均株価
            # 'twoHundredDayAverage', # 過去200日間の平均株価
            'trailingAnnualDividendRate', # 過去1年間の年間配当額
            'trailingAnnualDividendYield', # 過去1年間の配当利回り
            'marketCap', # 時価総額(自己株式除く)
            ]
        for summary_detail_key in summary_detail_keys:
            try:
                company_metrics.append(ticker_data.summary_detail[ticker_num][summary_detail_key])
            except Exception as e:
                print(f'証券コード:{ticker_num},未取得の属性:{summary_detail_key}')
                company_metrics.append(np.nan)
                missed_data = [{'symbol':ticker_num , 'ticker_data': ticker_data, 'summary_detail_key': summary_detail_key}]
                missed_data_df = pd.DataFrame(missed_data)
                missed_company_metrics = pd.concat([missed_company_metrics, missed_data_df], ignore_index=True)  
                
        # 企業の財務指標（現金、負債、ROA、ROE、利益率など）の取得
        financial_data_keys = [
            # 'recommendMean', # アナリストの推奨評価の平均（3.0）。1が「買い」、5が「売り」を示し、3は「予想（ホールド）」
            'ebitda', # （EBITDA）：企業の利払い・税引き・減価償却前利益を示し、企業の営業活動による利益の指標
            'totalDebt', # （総負債）: 企業が抱えている全ての負債
            # 'QuickRatio', # （クイックレシオ）:流動資産から在庫を一瞬で時換可能な資産と流動資産の比率。1.0未満は、短期負債の支払い能力にリスクがある可能性を示す
            'currentRatio', # （流動比率）:流動資産と国債金利の比率。一般的に、1.0以上であれば短期的な支払い能力があると考えられる。
            'totalRevenue', # 企業が一定期間得た全ての売上高
            'debtToEquity', # （負債比率）:企業の負債と株主資本の比率。高い値は、企業が負債に依存していることを示唆
            'revenuePerShare', # 1株当たりの利益
            'returnOnAssets', # ROA(総資産利益率)
            'returnOnEquity', # ROE（自己資本利益率）
            'earningsGrowth', # 利益成長率(期比の利益成長率)
            'revenueGrowth', # 売上成長率(同期比の売上成長率)
            'grossMargins', # 売上総利益率(売上に対する総売上利益の割合)
            'ebitdaMargins', # 売上に対するEBITDAの割合
            'operatingMargins', # 営業利益率(売上に対する営業利益の割合)
            'profitMargins', # 純利益率(売上に対する最終的な純利益の割合)
            # 'financialCurrency' # 通貨
                            ]
        for financial_data_key in financial_data_keys:
            try:
                company_metrics.append(ticker_data.financial_data[ticker_num][financial_data_key])
            except Exception as e:
                print(f'証券コード:{ticker_num},未取得の属性:{financial_data_key}')
                company_metrics.append(np.nan)
                missed_data = [{'symbol':ticker_num , 'ticker_data': ticker_data, 'summary_detail_key': summary_detail_key}]
                missed_data_df = pd.DataFrame(missed_data)
                missed_company_metrics = pd.concat([missed_company_metrics, missed_data_df], ignore_index=True)  

        columns = ['symbol', 'dividendRate', 'dividendYield', 'exDividendDate', 'fiveYearAvgDividendYield','payoutRatio', 'beta',
                'trailingPE', 'forwardPE', 'bidSize', 'askSize', 'trailingAnnualDividendRate', 'trailingAnnualDividendYield', 'MarketCap',
                'EBITDA', 'totalDebt', 'currentRatio', 'totalRevenue', 'debtToEquity', 'revenuePerShare', 
                'ROA', 'ROE', 'earningsGrowth', 'revenueGrowth', 'grossMargins', 'ebitdaMargins', 'operatingMargins', 'profitMargins']
        df_company_metrics = pd.DataFrame(data=[company_metrics], columns=columns)

        return df_company_metrics, missed_company_metrics

    # 企業の過去の財務状況を取得
    # 引数:証券コード、Tickerオブジェクト
    # 戻値:企業の財務状況:Dataframe
    def get_company_finacial_info(self, ticker_num, ticker_data):    

        missed_company_finacial_info = pd.DataFrame(columns=['symbol', 'ticker_data'])

        #for _ in range(3):
        try:
            # 損益計算書を取得
            income_statement_annual = ticker_data.income_statement(trailing=False, frequency='a') # 年次の損益計算書を取得
            if isinstance(income_statement_annual, pd.DataFrame):
                income_statement_annual = income_statement_annual.reset_index(inplace=False)
            else:
                income_statement_annual = pd.DataFrame(np.nan, index=[0], columns=income_statement_annual_columns)
                
            income_statement_quarterly = ticker_data.income_statement(trailing=False, frequency='q') # 四半期の損益計算書を取得
            if isinstance(income_statement_quarterly, pd.DataFrame):
                income_statement_quarterly = income_statement_quarterly.reset_index(inplace=False)
            else:
                income_statement_quarterly = pd.DataFrame(np.nan, index=[0], columns=income_statement_quarterly_columns)
                
            income_statement = pd.concat([income_statement_annual, income_statement_quarterly]) 
            income_statement['symbol'] = income_statement['symbol'].astype(str)
            income_statement['asOfDate'] = income_statement['asOfDate'].astype('datetime64[ns]')

            # キャッシュフロー計算書を取得
            cash_flow_annual = ticker_data.cash_flow(trailing=False, frequency='a')
            if isinstance(cash_flow_annual, pd.DataFrame):
                cash_flow_annual = cash_flow_annual.reset_index(inplace=False)
            else:
                cash_flow_annual = pd.DataFrame(np.nan, index=[0], columns=cash_flow_annual_columns)
                
            cash_flow_quarterly = ticker_data.cash_flow(trailing=False, frequency='q')
            if isinstance(cash_flow_quarterly, pd.DataFrame):
                cash_flow_quarterly = cash_flow_quarterly.reset_index(inplace=False)
            else:
                cash_flow_quarterly = pd.DataFrame(np.nan, index=[0], columns=cash_flow_quarterly_columns)
                
            cash_flow = pd.concat([cash_flow_annual, cash_flow_quarterly])
            cash_flow['symbol'] = cash_flow['symbol'].astype(str)
            cash_flow['asOfDate'] = cash_flow['asOfDate'].astype('datetime64[ns]')
        
            # 貸借対照表を取得
            balance_sheet_annual = ticker_data.balance_sheet(frequency='a')
            if isinstance(balance_sheet_annual, pd.DataFrame):
                balance_sheet_annual = balance_sheet_annual.reset_index(inplace=False)
            else:
                balance_sheet_annual = pd.DataFrame(np.nan, index=[0], columns=balance_sheet_annual_columns)
                
            balance_sheet_quarterly = ticker_data.balance_sheet(frequency='q')
            if isinstance(balance_sheet_quarterly, pd.DataFrame):
                balance_sheet_quarterly = balance_sheet_quarterly.reset_index(inplace=False)
            else:
                balance_sheet_quarterly = pd.DataFrame(np.nan, index=[0], columns=balance_sheet_quarterly_columns)
                
            balance_sheet = pd.concat([balance_sheet_annual, balance_sheet_quarterly])
            balance_sheet['symbol'] = balance_sheet['symbol'].astype(str)
            balance_sheet['asOfDate'] = balance_sheet['asOfDate'].astype('datetime64[ns]')

            # 企業価値評価
            valuation_measures = ticker_data.valuation_measures
            if isinstance(valuation_measures, pd.DataFrame):
                valuation_measures = valuation_measures.reset_index()
            else:
                valuation_measures = pd.DataFrame(np.nan, index=[0], columns=valuation_measures_columns)
                valuation_measures['symbol'] = valuation_measures['symbol'].astype(str)
                valuation_measures['asOfDate'] = valuation_measures['asOfDate'].astype('datetime64[ns]')
            #break # 成功した時点でループを抜ける

        except Exception as e:
            print(f'{ticker_num}：エラー内容{e}')
            missed_data = [{'symbol':ticker_num , 'ticker_data': ticker_data}]
            missed_data_df = pd.DataFrame(missed_data)
            missed_company_finacial_info = pd.concat([missed_company_finacial_info, missed_data_df], ignore_index=True)  
            time.sleep(10)

            return pd.DataFrame(), missed_company_finacial_info

        # データの型を確認して処理を分岐
        if not isinstance(income_statement, pd.DataFrame):
            # 取得データがDataFrameでない場合、空のDataFrameを作成
            income_statement = pd.DataFrame(columns=['symbol', 'asOfDate', 'periodType', 'currencyCode', 'NetIncome'])
        elif income_statement.empty:
            # DataFrameだが中身が空の場合も、構造を保証
            income_statement = pd.DataFrame(columns=['symbol', 'asOfDate', 'periodType', 'currencyCode', 'NetIncome'])
        else:
            # 必要なカラムが欠けている場合に備えて補完
            for col in ['symbol', 'asOfDate', 'periodType', 'currencyCode', 'NetIncome']:
                if col not in income_statement.columns:
                    income_statement[col] = None

        if not isinstance(cash_flow, pd.DataFrame):
            cash_flow = pd.DataFrame(columns=['symbol', 'asOfDate', 'periodType', 'currencyCode', 'NetIncome'])
        elif cash_flow.empty:
            cash_flow = pd.DataFrame(columns=['symbol', 'asOfDate', 'periodType', 'currencyCode', 'NetIncome'])
        else:
            for col in ['symbol', 'asOfDate', 'periodType', 'currencyCode', 'NetIncome']:
                if col not in income_statement.columns:
                    cash_flow[col] = None
        
        if not isinstance(balance_sheet, pd.DataFrame):
            balance_sheet = pd.DataFrame(columns=['symbol', 'asOfDate', 'periodType', 'currencyCode'])
        elif balance_sheet.empty:
            balance_sheet = pd.DataFrame(columns=['symbol', 'asOfDate', 'periodType', 'currencyCode'])
        else:
            for col in ['symbol', 'asOfDate', 'periodType', 'currencyCode']:
                if col not in balance_sheet.columns:
                    balance_sheet[col] = None

        if not isinstance(valuation_measures, pd.DataFrame):
            valuation_measures = pd.DataFrame(columns=['symbol', 'asOfDate', 'periodType'])
        elif valuation_measures.empty:
            valuation_measures = pd.DataFrame(columns=['symbol', 'asOfDate', 'periodType'])
        else:
            for col in ['symbol', 'asOfDate', 'periodType']:
                if col not in valuation_measures.columns:
                    valuation_measures[col] = None
            
        desired_dtypes = {
            'symbol': 'object',
            'asOfDate': 'datetime64[ns]',
            'periodType': 'object',
            'currencyCode': 'object',
            'NetIncome': 'float64'
        }

        # データ型を統一する関数
        def unify_dtypes(df, dtypes_dict):
            for col, dtype in dtypes_dict.items():
                if col in df.columns:
                    if dtype == 'datetime64[ns]': 
                        df[col] = pd.to_datetime(df[col])
                    else:
                        df[col] = df[col].astype(dtype)
            return df

        # 各データフレームに対してデータ型を統一
        income_statement = unify_dtypes(income_statement, desired_dtypes)
        cash_flow = unify_dtypes(cash_flow, desired_dtypes)
        balance_sheet = unify_dtypes(balance_sheet, desired_dtypes)
        valuation_measures = unify_dtypes(valuation_measures, desired_dtypes)

        combined_df_1 = pd.merge(income_statement, cash_flow, on=['symbol', 'asOfDate', 'periodType', 'currencyCode', 'NetIncome'], how='outer')
        combined_df_2 = pd.merge(combined_df_1, balance_sheet, on=['symbol', 'asOfDate', 'periodType', 'currencyCode'], how='outer')
        # combined_df_2['asOfDate'] = combined_df_2['asOfDate'].astype('datetime64[ns]')        
        df_financial_info = pd.merge(combined_df_2, valuation_measures , on=['symbol', 'asOfDate', 'periodType'], how='outer')

        # 過去の自己資本比率を計算
        df_financial_info['capitalAdequacyRatio'] = df_financial_info['StockholdersEquity'] / df_financial_info['TotalAssets']
        # 過去の自己資本利益率を計算
        df_financial_info['ROE'] = df_financial_info['NetIncome'] / df_financial_info['StockholdersEquity']

        return df_financial_info, missed_company_finacial_info

    def preprocess_date(self, date_list):
        dates = pd.Series(date_list).astype(str)
        dates = dates.str.split(' ').str[0]
        new_date_list = pd.to_datetime(dates)
        return new_date_list

# 初回実行
# 引数:無し
# 戻値:無し
def main():
    # 東証上場銘柄一覧を読み込む
    is_file = os.path.isfile('./input/finance_data/raw_jpx_codes.xls')
    if is_file:
        print('東証上場銘柄一覧の読み込み開始')
        raw_stock_lists = pd.read_excel("./input/finance_data/raw_jpx_codes.xls")
    else:
        print('指定フォルダ内にファイルがあることを確認')
        exit()

    gfd = GetFinanceData()
    stock_lists = gfd.preprocess_stock_lists(raw_stock_lists)

    # 企業情報の指標、財務状況を保存するデータフレームの作成
    df_all_stock_prices = pd.DataFrame()
    missed_all_stock_prices = pd.DataFrame()

    df_all_company_metrics = pd.DataFrame()
    missed_all_company_metrics = pd.DataFrame()

    df_all_company_financial_info = pd.DataFrame()
    missed_all_company_financial_info = pd.DataFrame()

    # 企業の銘柄名、市場・商品区分、33業種、17業種を保持
    series_ticker_name = pd.Series()
    series_market_product_category = pd.Series()
    series_type_33 = pd.Series()
    series_type_17 = pd.Series()

    # tickerの企業情報の指標、財務状況を取得
    for ticker in raw_stock_lists['コード']:
        time.sleep(3)
        # 証券コードに「.T」を追加
        jpx_filter_df = stock_lists[stock_lists['コード'] == ticker]
        ticker_num = str(ticker) + '.T'
        
        try:
            ticker_data = Ticker(ticker_num)
            print(f'取得中のティッカー：{ticker_num}')
        except ChunkedEncodingError:
            print(f"リトライ中:{ticker_num}")
            time.sleep(5)
        except Exception as e:
            print(f"エラー: {e}")

        series_ticker_name = pd.concat([series_ticker_name, jpx_filter_df['銘柄名']])
        series_market_product_category = pd.concat([series_market_product_category, jpx_filter_df['市場・商品区分']])
        series_type_33 = pd.concat([series_type_33, jpx_filter_df['33業種区分']])
        series_type_17 = pd.concat([series_type_17, jpx_filter_df['17業種区分']])

        # 企業情報の取得
        df_stock_prices, missed_stock_prices = gfd.get_stock_prices(ticker_num, ticker_data, '2000-01-01')
        df_all_stock_prices = pd.concat([df_all_stock_prices, df_stock_prices])
        missed_all_stock_prices = pd.concat([missed_all_stock_prices, missed_stock_prices])

        df_company_metrics, missed_company_metrics = gfd.get_company_metrics(ticker_num, ticker_data)
        df_all_company_metrics = pd.concat([df_all_company_metrics, df_company_metrics])
        missed_all_company_metrics = pd.concat([missed_all_company_metrics, missed_company_metrics])

        df_company_financial_info, missed_company_financial_info = gfd.get_company_finacial_info(ticker_num, ticker_data)
        df_all_company_financial_info = pd.concat([df_all_company_financial_info, df_company_financial_info])
        missed_all_company_financial_info = pd.concat([missed_all_company_financial_info, missed_company_financial_info])

    os.makedirs('./input/finance_data/origin', exist_ok=True)
    # 不要な列の削除を行い、CSVファイルに保存
    # df_all_stock_prices['date'] = pd.to_datetime(df_all_stock_prices['date'], errors='coerce')
    df_all_stock_prices = df_all_stock_prices.drop('index', axis=1)
    df_all_stock_prices = df_all_stock_prices.drop_duplicates()
    df_all_stock_prices['date'] = gfd.preprocess_date(df_all_stock_prices['date'])
    df_all_stock_prices.to_csv('./input/finance_data/origin/stock_prices.csv', encoding='cp932', index=False, errors='ignore')
    df_all_stock_prices.to_sql("stock_prices", engine, if_exists="append", index=False)    
    missed_all_stock_prices.to_csv('./input/finance_data/origin/missed_stock_prices.csv', index=False)

    # 列の追加、削除を行いCSVファイルに保存
    df_all_company_metrics['ticker_name'] = series_ticker_name.values
    df_all_company_metrics['market_product_category'] = series_market_product_category.values
    df_all_company_metrics['type_33'] = series_type_33.values
    df_all_company_metrics['type_17'] = series_type_17.values
    # df_all_company_metrics['recommendMean'] = None
    # df_all_company_metrics['QuickRatio'] = None
    # df_all_company_metrics = df_all_company_metrics.drop(columns=['recommendMean', 'QuickRatio'])
    df_all_company_metrics['exDividendDate'] = pd.to_datetime(df_all_company_metrics['exDividendDate'], errors='coerce').dt.date # datetime64に変換
    df_all_company_metrics = df_all_company_metrics.drop_duplicates()
    df_all_company_metrics.to_csv('./input/finance_data/origin/company_metrics.csv', encoding='cp932', index=False, errors='ignore')
    df_all_company_metrics.to_sql("metrics", engine, if_exists="append", index=False)    
    missed_all_company_metrics.to_csv('./input/finance_data/origin/missed_company_metrics.csv', index=False)

    # 欠損率の計算と95%以上の欠損がある列を特定し削除した後にCSVファイルに保存
    missing_ratio = df_all_company_financial_info.isnull().mean()
    high_missing_columns = missing_ratio[missing_ratio >= 0.95].index
    df_all_company_financial_info = df_all_company_financial_info.drop(columns=high_missing_columns)
    df_all_company_financial_info = df_all_company_financial_info.drop_duplicates()
    df_all_company_financial_info.to_csv('./input/finance_data/origin/company_financial_info.csv', encoding='cp932', index=False, errors='ignore')
    df_all_company_financial_info.to_sql("financial_info", engine, if_exists="append", index=False)    
    missed_all_company_financial_info.to_csv('./input/finance_data/origin/missed_company_financial_info.csv', index=False)

if __name__ == "__main__":
    main()