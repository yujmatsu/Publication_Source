# 【Snowflake】Frosty Friday Week 8 やってみた：Streamlit in Snowflakeで可視化アプリ作成
# Zenn:https://zenn.dev/yujmatsu/articles/20251205_frostyfriday_008

## はじめに

Snowflake の実践的なスキルを磨くためのコミュニティ課題、**「Frosty Friday」**。

今回の **Week 8** は、難易度 "Basic"（初級）。
テーマは、Snowflake の世界を大きく広げる **「Streamlit (ストリームリット)」** です。

「データを分析したけど、SQLの結果だけ渡してもビジネス側に伝わらない…」
「簡単なグラフやダッシュボードを作りたいけど、BIツールを入れるほどでもない」



そんな時に最強の武器となるのが Streamlit です。
Python だけで簡単にWebアプリが作れるこのツールが、今は **Streamlit in Snowflake (SiS)** として Snowflake 上で直接動かせるようになりました。

今回は、S3上のデータをロードし、それを SiS で可視化するアプリを作成してみましょう。

## 今週の課題：Week 8 - Basic

課題の詳細は公式サイトで確認できます。
[Week 8 – Basic – Streamlit](https://frostyfriday.org/blog/2022/08/05/week-8-basic/)


### 課題のストーリー
ある会社が持つ「支払い（Payments）」データを可視化したいという依頼です。
S3 にあるCSVデータをロードし、**「2021年の支払い額推移」** を表示するアプリを作成してください。

### 要件の要約
1.  **データロード:** S3 から `payments.csv` をテーブルにロードする。
2.  **アプリ作成:** Streamlit を使って以下の機能を持つアプリを作る。
    * タイトル: "Payments in 2021"
    * フィルター: 日付の最小値 (`min date`) と最大値 (`max date`) をスライダーで選択可能にする。
    * グラフ: X軸に日付（週次集計）、Y軸に金額を表示する**折れ線グラフ**。
3.  **セキュリティ:** （ローカルの場合）パスワードをハードコードせず `secrets.toml` を使うこと。
    * ※今回は Snowflake 上で動く **SiS** を使うため、認証情報はコードに書かずにセッションを取得します。

## 実践 Step 1：データの準備 (SQL)

まずは、Snowsight の SQL ワークシートを使って、データを準備します。
ここはいつもの流れですね。

### コンテキスト設定

```sql
USE ROLE SYSADMIN;
USE WAREHOUSE FF_WH;
USE DATABASE FROSTY_FRIDAY;

CREATE SCHEMA IF NOT EXISTS WEEK_008;
USE SCHEMA WEEK_008;
```

### ステージ作成とデータロード

S3 からデータをロードします。
CSVのデータ形式に揺らぎがあっても対応できるよう、日付は `VARCHAR`、金額は `NUMBER` で定義してテーブルを作成します。

```sql
-- 1. ステージ作成
CREATE OR REPLACE TEMPORARY STAGE WEEK8_STAGE
    URL = 's3://frostyfridaychallenges/challenge_8/';

-- 2. ファイルフォーマット作成
CREATE OR REPLACE FILE FORMAT FF_CSV_HEADER
    TYPE = CSV
    SKIP_HEADER = 1;

-- 3. テーブル作成
-- CSVの中身は id, payment_date, card_type, amount の4列想定
-- 日付フォーマットの事故を防ぐため、一旦 VARCHAR で受けます
CREATE OR REPLACE TABLE PAYMENTS (
    ID INT,
    PAYMENT_DATE VARCHAR,
    CARD_TYPE VARCHAR,
    AMOUNT_SPENT NUMBER
);

-- 4. データロード
COPY INTO PAYMENTS
FROM @WEEK8_STAGE/payments.csv
FILE_FORMAT = (FORMAT_NAME = 'FF_CSV_HEADER');

-- データの確認
SELECT * FROM PAYMENTS LIMIT 10;
```

これで `PAYMENTS` テーブルにデータが入りました。


## 実践 Step 2：Streamlit アプリの作成 (Python)

ここからが本番です。Snowflake 上で Streamlit アプリを作成します。



### アプリの作成手順
1.  Snowsight の左側メニューから **[Streamlit]** を選択します。
2.  右上の **[+ Streamlit アプリ]** をクリックします。
3.  アプリ名（例: `Week 8 Payments App`）、ウェアハウス（`FF_WH`）、親データベース（`FROSTY_FRIDAY`）、スキーマ（`WEEK_008`）を選択して **[作成]** をクリックします。

### Python コードの記述

エディタが開いたら、以下のコードを貼り付けて **[実行 (Run)]** をクリックしてください。
SiS の Python 環境（Anaconda）には `pandas` などの主要ライブラリはプリインストールされています。

```python
import streamlit as st
import pandas as pd
# Snowsight上で動かすためのセッション取得モジュール
from snowflake.snowpark.context import get_active_session

# 1. Snowflakeセッションの取得
# SiSでは、get_active_session() を呼ぶだけで接続できます。
# ※注意: アプリは基本的に「オーナー権限 (Owner's Rights)」で実行されます。
session = get_active_session()

# 2. データの取得と集計
# 課題要件: "aggregated at the weekly level" (週次集計)
# SQLで集計までやってしまうのがパフォーマンス・コスト的におすすめです。
# PAYMENT_DATE は VARCHAR なので、ここで DATE 型にキャストします。
query = """
    SELECT 
        DATE_TRUNC('WEEK', TRY_TO_DATE(PAYMENT_DATE, 'MM/DD/YYYY')) AS PAYMENT_DATE,
        SUM(AMOUNT_SPENT) AS AMOUNT
    FROM PAYMENTS
    GROUP BY 1
    ORDER BY 1
"""

@st.cache_data
def load_data():
    """
    Snowflakeに接続してデータを取得し、DataFrameを整形する関数
    """
    # Snowparkのセッションを使ってSQLを実行し、直接Pandas DataFrameに変換
    payments_df = session.sql(query).to_pandas()
    
    # 日付型変換とインデックス設定（グラフ描画用）
    # SQL側でNULLになったデータ（変換失敗など）は除外する処理を入れるとより安全です
    payments_df = payments_df.dropna(subset=['PAYMENT_DATE'])
    payments_df['PAYMENT_DATE'] = pd.to_datetime(payments_df['PAYMENT_DATE'])
    payments_df = payments_df.set_index('PAYMENT_DATE')
    return payments_df

# データをロード
payments_df = load_data()

# データが空の場合のハンドリング
if payments_df.empty:
    st.error("データが見つかりませんでした。")
    st.stop()

# 日付範囲の取得（スライダーの初期値用）
def get_min_date():
    return min(payments_df.index.to_list()).date()

def get_max_date():
    return max(payments_df.index.to_list()).date()

def app_creation():
    """
    UI構築を行うメイン関数
    """
    # タイトル設定
    st.title("Payments in 2021")

    # 最小日付フィルター (スライダー)
    min_filter = st.slider("Select Min Date", 
                           min_value=get_min_date(), 
                           max_value=get_max_date(), 
                           value=get_min_date())

    # 最大日付フィルター (スライダー)
    max_filter = st.slider("Select Max Date", 
                           min_value=get_min_date(), 
                           max_value=get_max_date(), 
                           value=get_max_date())
                           
    # ユーザーの誤操作（Min > Max）をチェック
    if min_filter > max_filter:
        st.error("エラー: 開始日（Min Date）は終了日（Max Date）より前の日付にしてください。")
        st.stop()

    # フィルタリング処理 (Pandas上でフィルタ)
    mask = (payments_df.index >= pd.to_datetime(min_filter)) \
           & (payments_df.index <= pd.to_datetime(max_filter))
    
    payments_df_filtered = payments_df.loc[mask]
    
    # 折れ線グラフの描画
    st.line_chart(payments_df_filtered)

# アプリ実行
app_creation()
```

### 動作確認
画面上にスライダーと折れ線グラフが表示され、スライダーを動かすとグラフの範囲が変われば成功です。


## 学びとコスト意識のポイント

今回は Streamlit を使った可視化を行いましたが、実運用では以下の「権限」と「コスト」の観点が非常に重要になります。

### 1. 権限モデルは「Owner's Rights」
Streamlit in Snowflake (SiS) は、原則としてアプリを作成した人（オーナー）の権限で実行されます（**Owner's Rights**）。
つまり、アプリを閲覧しているユーザー（Viewer）にデータの参照権限がなくても、**オーナーが権限を持っていればデータが見えてしまいます**。
社外秘データなどを扱う場合は、アプリの共有範囲やデータの見せ方に十分注意が必要です。



### 2. ウェアハウスの稼働時間とWebSocket
SiS アプリを開いている間、ブラウザと Snowflake の間では **WebSocket** 通信が維持されます。
この通信が生きている間、バックグラウンドで仮想ウェアハウスがアクティブになり続ける可能性があります。
WebSocket は「最後の操作から約15分」でタイムアウトしますが、それまではウェアハウスが稼働し続けるリスクがあるため、**使い終わったらタブを閉じる**か、ダッシュボード専用の小さなウェアハウスを割り当てるのがコスト管理のコツです。

### 3. キャッシュの活用 (`@st.cache_data`)
Streamlit は、スライダーを動かすたびに Python スクリプト全体が再実行されます。
`@st.cache_data` を付けることで、一度取得したデータフレームをメモリに保存し、再実行時はそれを再利用します。これにより、レスポンスが高速化し、無駄なクエリ実行（＝ウェアハウス課金）を防ぐことができます。


## 次回予告

次回は **Week 9** に挑戦予定です。
再び「タグ (Tag)」がテーマのようですが、今度は「データのタグ付け」と「マスキングポリシー」を組み合わせた応用編のようです。

## 参考資料
* [Frosty Friday Week 8](https://frostyfriday.org/blog/2022/08/05/week-8-basic/)
* [Snowflake Docs: Streamlit in Snowflake](https://docs.snowflake.com/ja/developer-guide/streamlit/about-streamlit)
* [Snowflake Docs: 所有者の権利と Streamlit in Snowflake アプリの理解](https://docs.snowflake.com/ja/developer-guide/streamlit/owners-rights)
* [Streamlit Docs: st.cache_data](https://docs.streamlit.io/library/api-reference/performance/st.cache_data)