"""
EDINETからdocID（書類管理番号）を取得
    EDINET（Electronic Disclosure for Investors' NETwork）は、金融庁が提供するオンラインシステムで、企業の有価証券報告書などを取得することが可能
    EDINETは文書を一意のdocID（書類管理番号）で管理
    有価証券報告書を取得するためには、まず、このdocIDの取得が必要

non-finance_data    
└── docID
   ├── edinet_df.csv：EdinetReport.csvとEdinetcodeDlInfo.csvの結合ファイル
   └── raw_data
       ├── EdinetReport.csv：(会社名、書類名、docID、証券コード、ＥＤＩＮＥＴコード、決算期、提出日)のdf
       ├── Edinetcode.zip：EdinetcodeDlInfo.csvの圧縮されたzipファイル
       └── EdinetcodeDlInfo.csv："ＥＤＩＮＥＴコード","資本金", "決算日", "提出者業種"のdf

EdinetReport,edinet_dfは一時データも保存
"""

import os
import requests
import datetime
import pandas as pd
import time
import zipfile
import warnings

# EDINETAPIのトークンを取得
edinet_api_key = os.environ['EDINET_API_KEY']

now = datetime.datetime.now()

# 警告を特定のものに限定
warnings.filterwarnings('ignore', category=DeprecationWarning)

# DocIDのリストを取得
class GetDocid:
    # 1 コンストラクタ・日付リストの作成
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.day_list = self.create_day_list()

    def create_day_list(self):
        day_list = []
        period = (self.end_date - self.start_date).days
        for d in range(period + 1):
            day = self.start_date + datetime.timedelta(days=d)
            day_list.append(day)
        return day_list

    # 2 レポートリストの作成
    def create_report_list(self):
        report_list = []
        for day in self.day_list:
            url = "https://api.edinet-fsa.go.jp/api/v2/documents.json" # HTTPリクエスト（EDINETのAPIエンドポイントへのURLを指定）
            params = {"date": day, "type": 2, "Subscription-Key": edinet_api_key} # type(1:書類、2:PDF)
            try:
                res = requests.get(url, params=params)
                res.raise_for_status() # HTTPステータスコードが200番台であれば「try」実行、200番台以外であれば「except」実行
                json_data = res.json() # HTTP応答から取得したJSON形式のデータを解析
                time.sleep(2)  
            except requests.RequestException as e:
                print(f"リクエストに失敗しました: {e}")
                continue

            for result in json_data.get("results", []): # json_dataから"results"キーに関連する値を取得。"results"が存在しない場合は、エラーを発生させる代わりに空のリスト([])を返す
                if result["ordinanceCode"] == "010" and result["formCode"] == "030000": # formCode= 030000：有報, 030001:訂正有報,050000:半報, 043000:四半期報告書, 043001:訂正四半期報告書 
                    report_list.append({
                        '会社名': result["filerName"],
                        '書類名': result["docDescription"],
                        'docID': result["docID"],
                        '証券コード': result["secCode"],
                        'ＥＤＩＮＥＴコード': result["edinetCode"],
                        '決算期': result["periodEnd"],
                        '提出日': day
                    })
        return report_list

    # 3 データフレームの作成と保存
    def create_docid_df(self, ID_base_dir):
        # ファイルパスを設定
        # extract_path = f"{ID_base_dir}"
        extract_path = ID_base_dir
        zip_file_path = f"{extract_path}/Edinetcode.zip"

        # 出力ディレクトリを作成（存在しない場合のみ）
        os.makedirs(extract_path, exist_ok=True)     
        
        # edinetcode.zipを取得
        # !wget -P {extract_path} https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip
        response = requests.get('https://disclosure2dl.edinet-fsa.go.jp/searchdocument/codelist/Edinetcode.zip')
        with open(f"{extract_path}/Edinetcode.zip", 'wb') as f:
            f.write(response.content)

        # zipファイルのダウンロードと展開
        try:
            with zipfile.ZipFile(zip_file_path) as zip_f:
                zip_f.extractall(extract_path)
        except zipfile.BadZipFile:
            print("ファイルの解凍に失敗しました")
            return None

        # EdinetReport.csvの作成
        df_info = pd.read_csv(f"{extract_path}/EdinetcodeDlInfo.csv", encoding="cp932", skiprows=[0])
        df_report = pd.DataFrame(self.create_report_list())
        df_report['symbol'] = df_report['証券コード'].fillna('0').astype(str).str[:4]
        # df_report.to_csv(f"{extract_path}/origin/EdinetReport.csv", encoding="cp932")

        # edinet_df.csvの作成
        df_info = df_info[["ＥＤＩＮＥＴコード","資本金", "決算日", "提出者業種"]]
        merged_df = pd.merge(df_report, df_info, how="inner", on="ＥＤＩＮＥＴコード")
        # merged_df.to_csv(f"{ID_base_dir}/origin/edinet_df.csv",index=False)

        return df_report, merged_df
    
def main():
    # 取得したい提出日を期間指定
    end_date = datetime.date.today() # 終了日を本日に設定
    start_date = end_date - datetime.timedelta(days=3652)
    # データ保存のためのBaseディレクトリを指定
    os.makedirs('./input/non-finance_data/docID', exist_ok=True)
    ID_base_dir = "./input/non-finance_data/docID"
    
    gd = GetDocid(start_date, end_date)
    df_report, edinet_df = gd.create_docid_df(ID_base_dir)
    df_report.to_csv(f"{ID_base_dir}/origin/report/{now.strftime('%Y%m%d')}_EdinetReport.csv", encoding="cp932")
    edinet_df.to_csv(f"{ID_base_dir}/origin/edinet_ID/{now.strftime('%Y%m%d')}_edinet_df.csv",index=False)

if __name__ == '__main__':
    main()