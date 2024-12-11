"""
2. EDINETから有価証券報告書をCSVファイル形式で取得し、テキストデータを抽出¶

non-finance_data
├── doc
    ├── 20241007_non-financial_data.csv
    ├── S10034C4
    │   └── XBRL_TO_CSV
    │       └── ~.csv

docフォルダ配下
- 各種doc csvファイル
- (日時)_non-financial_data.csv
"""

import datetime
import os
from bs4 import BeautifulSoup
import zipfile
import io
import requests
import pandas as pd
import time
import glob
import re
from sqlalchemy import create_engine
from get_data.config import db_config

#  DBエンジンのインスタンスを作成
conn_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(conn_string)

now = datetime.datetime.now().strftime('%Y%m%d')

# EDINETAPIのトークンを取得
edinet_api_key = os.environ['EDINET_API_KEY']

# keyの指定
keys = {
    "事業の内容":"jpcrp_cor:DescriptionOfBusinessTextBlock", # 事業の内容
    "関係会社の状況":"jpcrp_cor:OverviewOfAffiliatedEntitiesTextBlock", # 関係会社の状況
    "従業員の状況":"jpcrp_cor:InformationAboutEmployeesTextBlock", #従業員の状況
    "経営方針":"jpcrp_cor:BusinessPolicyBusinessEnvironmentIssuesToAddressEtcTextBlock", #経営方針、経営環境および対処すべき課題等
    "事業等のリスク":"jpcrp_cor:BusinessRisksTextBlock", #事業等のリスク
    "大株主の状況":"jpcrp_cor:MajorShareholdersTextBlock" # 大株主の状況
    }


# print(f"カレントディレクトリ: {os.getcwd()}") # 現在の作業ディレクトリ: /finance/Company_Analysis
# print(f"スクリプトのディレクトリ: {os.path.dirname(os.path.abspath(__file__))}") # /finance/Company_Analysis/get_data

# テキストデータの名称とXBRL上のcodeを、keysという辞書で渡す
class GetCsvFromEdinet:

    def __init__(self, keys, docid_list):
        self.keys = keys
        self.docid_list = docid_list

    def get_csv_file(self):

        extract_path = './input/non-finance_data/doc/tmp/'
        os.makedirs(extract_path, exist_ok=True)        
        for f in os.listdir(extract_path):
            file_path = os.path.join(extract_path, f)
            if os.path.isfile(file_path):
                os.remove(file_path)

        for docid in self.docid_list:
            url = f"https://api.edinet-fsa.go.jp/api/v2/documents/{docid}"
            """
            typeの各種パラメータ
            1 提出本文書及び監査報告書を取得
            2 PDFを取得
            3 代替書面・添付文書を取得
            4 英文ファイルを取得
            5 CSVを取得
            """
            params = {"type": 5, "Subscription-Key": edinet_api_key} 
            try:
                res = requests.get(url, params=params)
                res.raise_for_status() # ステータスコードが200以外の場合に例外を発生させる
                with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                    
                    # zip 内の一部のファイルが壊れている場合の確認
                    result = z.testzip()
                    if result is not None: 
                        print(f'zip 内の一部のファイルが壊れています {docid}')

                    for file in z.namelist():
                        if file.startswith("XBRL_TO_CSV/jpcrp") and file.endswith(".csv"):
                            z.extract(file, path=f"{extract_path}/{docid}/")
                            print(f"{docid}：csv読み込み")
                time.sleep(3)
            except requests.RequestException as e:
                print(f"リクエストが失敗しました {docid}: {e}")
            except zipfile.BadZipFile as e:
                print(f"zip が完全に壊れています {docid}: {e}")

    #EDINETコードの取得
    def extract_dynamic_code(self,xbrl_path):
        pattern = r'jpcrp030000-asr-001_E(\d{5})'
        match = re.search(pattern, xbrl_path)
        if match:
            return "E" + match.group(1)
        else:
            return None


    def get_text_data(self):
        data = []
        for docid in self.docid_list:
            print(docid)
            csv_path = f'./input/non-finance_data/doc/tmp/{docid}/XBRL_TO_CSV/*.csv'
            doc_data = [docid]
            try:
                csv_file = glob.glob(csv_path)[0]
                df = pd.read_csv(csv_file, encoding="utf-16",sep="\t")
                for value in self.keys.values():
                    matching_row = df[df["要素ID"]==value]
                    if not matching_row.empty:
                        doc_data.append(matching_row["値"].values[0])
                    else:
                        doc_data.append(None)
                        
                # 「サステナビリティに関する考え方及び取組」だけ、codeが特殊なので別途渡す
                dynamic_code = self.extract_dynamic_code(csv_file)
                key_text = f"jpcrp030000-asr_{dynamic_code}-000:DisclosureOfSustainabilityRelatedFinancialInformationTextBlock"
                matching_row = df[df["要素ID"]==key_text]
                if not matching_row.empty:
                    doc_data.append(matching_row["値"].values[0])
                else:
                    doc_data.append(None)

            except IndexError:
                print(f"{docid} のCSVファイルが見つかりませんでした")
                doc_data += ["File Not Found"] * (len(self.keys)+1)
            except Exception as e:
                print(f"エラー処理 {docid}: {e}")
                doc_data += ["Error"]*(len(self.keys)+1)
                
            data.append(doc_data)
            
        text_df = pd.DataFrame(data, columns=["docID"] + list(keys.keys())+["サステナビリティ方針"])

        return text_df
    

def main():
    # 有価証券報告書のXBRLファイル等の取得のためにdocid_listを作成
    edinet_df = pd.read_csv(f'../input/non-finance_data/docID/origin/edinet_df.csv') 
    origin_docid_lists = edinet_df["docID"].tolist()
    gcfe = GetCsvFromEdinet(keys, origin_docid_lists)
    # csvの取得 ※ 壊れているzipファイルのデータは全て上場企業以外であることは確認済み
    gcfe.get_csv_file()
    # テキストデータの抽出
    now = datetime.datetime.now()
    filename = '../input/non-finance_data/doc/' + now.strftime('%Y%m%d') + '_non-financial_data.csv'
    text_data = gcfe.get_text_data()
    text_data.to_csv(filename, index=False)

    merge_df = pd.merge(edinet_df, text_data, on='docID', how='outer')
    merge_df = merge_df.drop_duplicates()
    merge_df.to_csv('./input/non-finance_data/origin/' + now.strftime('%Y%m%d') + '_text_data', index=False)
    merge_df.to_sql("non_financial_data", engine, if_exists="append", index=False)    

if __name__ == "__main__":
    main()