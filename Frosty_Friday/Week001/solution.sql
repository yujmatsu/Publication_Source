----
-- Week001 回答例
----
-- 問題(英文そのまま)
-- FrostyFriday Inc., your benevolent employer, has an S3 bucket that is filled with .csv data dumps. 
-- This data is needed for analysis. Your task is to create an external stage,
-- and load the csv files directly from that stage into a table.
-- The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_1/
-- 
-- 問題(和訳)
-- あなたの慈悲深い雇用主であるFrostyFriday Inc.は、.csvデータダンプで満たされたS3バケットを持っています。
-- このデータは分析に必要です。あなたのタスクは、外部ステージを作成し、
-- そのステージからcsvファイルを直接テーブルにロードすることです。
-- S3バケットのURIは、s3://frostyfridaychallenges/challenge_1/です。
--
-- ■やること
-- 1.外部ステージ（S3バケット）を作成する。
-- 2.そのステージにある CSV ファイルを読み込む。
-- 3.ロード先のテーブルを作成し、データを投入する。
-- 4.結果を確認する。

/*
* 準備
*/
--各コンテキストの設定
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;
USE SCHEMA FROSTY_FRIDAY.WEEK_001;

-- Week 1 用のスキーマを作成して移動
CREATE SCHEMA IF NOT EXISTS WEEK_001;
USE SCHEMA WEEK_001;

/*
* 1.外部ステージ（S3バケット）を作成する。
*
* CREATE STAGE:https://docs.snowflake.com/ja/sql-reference/sql/create-stage
*
* 通常ストレージ統合の設定が必要だが、今回の課題用バケットは パブリック公開されているため、URLを指定するだけで良い。
*/

-- 外部ステージ（S3バケット）の作成（TEMPORARYで作成）
CREATE OR REPLACE TEMPORARY STAGE STRANGE_STAGE
URL = 's3://frostyfridaychallenges/challenge_1/';

-- 作成されたか確認
SHOW STAGES;

/*
* 2.そのステージにある CSV ファイルを読み込む。
*
* LIST:https://docs.snowflake.com/ja/sql-reference/sql/list
*
*/

-- ステージ内のファイルを確認する
-- `1.csv`, `2.csv`, `3.csv` というファイルが確認できればOK！
LIST @STRANGE_STAGE;

/*
* 3.ロード先のテーブルを作成し、データを投入する。
*
* COPY INTO <テーブル>:https://docs.snowflake.com/ja/sql-reference/sql/copy-into-table
*
*/

-- 事前にファイルの確認
-- ヘッダーなし、3ファイルとも文字型の1列のみ、データ件数は11件のみが確認できればOK！
SELECT 
    $1, -- 1列目
    $2, -- 2列目
    $3  -- 3列目（もしあれば）
FROM @STRANGE_STAGE;

-- ↓↓　以下は他のやり方　↓↓
-- 他にやり方として先に件数だけを確認してから中身を見てもよいかも
SELECT count(*)
FROM @STRANGE_STAGE;

-- 件数が多かった場合はlimitを使って件数を絞って確認してみる
SELECT 
    $1, -- 1列目
    $2, -- 2列目
    $3  -- 3列目（もしあれば）
FROM @STRANGE_STAGE
limit 5;
-- ↑↑ ここまで ↑↑

-- テーブルの作成
CREATE OR REPLACE TABLE WEEK1_TABLE (
    result VARCHAR
);

-- COPY INTO
-- エラーも特に出ず、11件のロードが成功していればOK！
COPY INTO WEEK1_TABLE
FROM @STRANGE_STAGE
FILE_FORMAT = (
    TYPE = 'CSV',
    SKIP_HEADER = 0 -- ヘッダーは無さそうなので0（デフォルト）
);

-- ↓↓　以下は他のやり方　↓↓
-- VALIDATION_MODE で事前検証ができる（ロード自体はしない）
-- PATTERN　でファイル形式を絞ることでCSVファイルのみを取り込む(将来のノイズ対策)
-- エラーが無い場合は、結果が返ってこない
COPY INTO WEEK1_TABLE
FROM @STRANGE_STAGE
  PATTERN = '.*\\.csv$'
  FILE_FORMAT = (TYPE = 'CSV')
  VALIDATION_MODE = 'RETURN_ERRORS';
-- ↑↑ ここまで ↑↑

/*
* 4.結果を確認する。
*
*/

-- 投入されたデータの更新
SELECT * FROM WEEK1_TABLE;

-- 任意：練習後に片付け
-- DROP STAGE IF EXISTS STRANGE_STAGE;
-- DROP TABLE IF EXISTS WEEK1_TABLE;
-- DROP SCHEMA FROSTY_FRIDAY.WEEK_001;