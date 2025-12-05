import streamlit as st
import pandas as pd
# Snowsight上で動かすためのセッション取得モジュール
from snowflake.snowpark.context import get_active_session

# 【変更点1】Secretsではなく、現在のアクティブセッションを取得します
session = get_active_session()

# 【ポイント】週次集計(Week Start)を行い、日付と金額を取得するクエリ
# 課題要件: "aggregated at the weekly level"
query = """
    SELECT 
        DATE_TRUNC('WEEK', PAYMENT_DATE::DATE) AS PAYMENT_DATE,
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
    # 【変更点2】Snowparkのセッションを使ってSQLを実行し、直接Pandas DataFrameに変換します
    # これにより、cursorやiterなどの複雑な処理が不要になります
    payments_df = session.sql(query).to_pandas()
    
    # 日付型変換とインデックス設定
    payments_df['PAYMENT_DATE'] = pd.to_datetime(payments_df['PAYMENT_DATE'])
    payments_df = payments_df.set_index('PAYMENT_DATE')
    return payments_df

# データをロード
payments_df = load_data()

def get_min_date():
    return min(payments_df.index.to_list()).date()

def get_max_date():
    return max(payments_df.index.to_list()).date()

def app_creation():
    """
    UI構築を行うメイン関数
    """
    # タイトル設定: "Payments in 2021"
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

    # フィルタリング処理
    mask = (payments_df.index >= pd.to_datetime(min_filter)) \
             & (payments_df.index <= pd.to_datetime(max_filter))
    
    payments_df_filtered = payments_df.loc[mask]
    
    # 折れ線グラフの描画
    st.line_chart(payments_df_filtered)

# アプリ実行
app_creation()