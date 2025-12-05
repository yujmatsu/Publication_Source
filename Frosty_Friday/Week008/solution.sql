----
-- Week008 回答例
----
-- 問題(英文)
-- Whilst, as of the time of writing, the Snowflake-Streamlit integration isn’t here yet, FrostyFriday sees that as only more reason to get ahead of the curve and start developing those Streamlit skills.
-- While Streamlit is Python-based, and we encourage you to learn Python, this challenge is Python-optional. The skeleton script below should mean you can do this challenge without any Python knowledge.
-- For a guide on getting started, head over here.
-- So…what’s the challenge?
-- Well, a company has a nice and simple payments fact table that you can find here. They want FrostyData to help them by ingesting the data and creating the below line chart.
--
-- The script must not expose passwords, as this would be very unsafe, instead, it should use Streamlit secrets.
-- The title must be “Payments in 2021”.
-- It must have a ‘min date’ filter which specifies the earliest date a user can select, by default this should be set to the earliest date possible.
-- It must have a ‘max date’ filter which specifies the latest date a user can select, by default this should be set to the latest date possible.
-- It should have a line chart with dates on the X axis, and amount on the Y axis. The data should be aggregated at the weekly level.
--
-- 問題(和訳)
-- 執筆時点ではSnowflakeとStreamlitの統合はまだ実現していませんが、FrostyFridayはこれを、時代を先取りしてStreamlitのスキルを開発し始める良い理由だと考えています。
-- StreamlitはPythonベースであり、Pythonを学ぶことを推奨しますが、このチャレンジはPythonが必須ではありません。以下のスケルトンスクリプトを使えば、Pythonの知識がなくてもこのチャレンジを行うことができます。
-- 初め方についてのガイドはこちらをご覧ください。
-- さて…チャレンジの内容は何でしょうか？
-- ある会社には、ここで見つけられるシンプルで素敵な支払いファクトテーブルがあります。彼らはFrostyDataに、データを取り込み、以下の折れ線グラフを作成するのを手伝ってほしいと考えています。
--
-- スクリプトはパスワードを公開してはいけません。これは非常に安全ではないため、代わりにStreamlitのSecretsを使用する必要があります。
-- タイトルは「Payments in 2021」でなければなりません。
-- ユーザーが選択できる最も古い日付を指定する「min date」フィルターが必要です。デフォルトでは、可能な限り最も古い日付に設定する必要があります。
-- ユーザーが選択できる最も新しい日付を指定する「max date」フィルターが必要です。デフォルトでは、可能な限り最新の日付に設定する必要があります。
-- X軸に日付、Y軸に金額を表示する折れ線グラフが必要です。データは週単位で集計される必要があります。
--
-- ■やること
-- 1. Snowflake上でテーブルを作成し、S3からデータをロードする（SQL）。
-- 2. StreamlitのSecretsファイルを作成し、接続情報を設定する。
-- 3. Pythonスクリプトを作成し、週次集計データの取得とグラフ描画を実装する。

/*
* ------------------------------------------------------------------------------
* STEP 1: Snowflake データ準備 (SQL)
* ------------------------------------------------------------------------------
*/

-- コンテキスト設定
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;

CREATE SCHEMA IF NOT EXISTS WEEK_008;
USE SCHEMA WEEK_008;

-- 1. ステージ作成
CREATE OR REPLACE TEMPORARY STAGE WEEK8_STAGE
    URL = 's3://frostyfridaychallenges/challenge_8/';

-- 2. ファイルフォーマット作成
CREATE OR REPLACE FILE FORMAT FF_CSV_HEADER
    TYPE = CSV
    SKIP_HEADER = 1;

-- 3. テーブル作成
-- CSVの中身は id, date, card_type,amount の4列
CREATE OR REPLACE TABLE PAYMENTS (
    ID INT,
    PAYMENT_DATE TIMESTAMP,
    CARD_TYPE VARCHAR,
    AMOUNT_SPENT INT
);

-- 4. データロード
COPY INTO PAYMENTS
FROM @WEEK8_STAGE/payments.csv
FILE_FORMAT = (FORMAT_NAME = 'FF_CSV_HEADER');

-- データの確認
SELECT * FROM PAYMENTS LIMIT 10;