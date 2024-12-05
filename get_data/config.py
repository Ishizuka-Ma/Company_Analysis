import os

# PostgreSQLへの接続情報
db_config = {
    'user': os.environ['USER'],      # ユーザー名
    'password': os.environ['POSTGRES_PASS'],  # パスワード
    'host': 'localhost',          # ホスト（例: 'localhost'）
    'port': '5432',               # ポート番号（通常は5432）
    'database': 'finance'   # データベース名
}