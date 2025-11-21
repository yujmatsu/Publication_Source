----
-- Week002 回答例
----
-- 問題(英文そのまま)
-- A stakeholder in the HR department wants to do some change-tracking but is concerned that the stream which was created for them gives them too much info they don’t care about.
-- Load in the parquet data and transform it into a table, then create a stream that will only show us changes to the DEPT and JOB_TITLE columns. 
-- You can find the parquet data here.
-- Execute the following commands:
-- UPDATE <table_name> SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;
-- UPDATE <table_name> SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;
-- UPDATE <table_name> SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;
-- UPDATE <table_name> SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;
-- UPDATE <table_name> SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;
-- 
-- 問題(和訳)
-- HR 部門の関係者は変更の追跡をしたいと考えていますが、作成されたストリームには必要のない情報が多すぎるのではないかと懸念しています。
-- parquet データをロードしてテーブルに変換し、DEPT 列と JOB_TITLE 列の変更のみを表示するストリームを作成します。 
-- parquet データはここにあります。
-- 次のコマンドを実行します。
-- UPDATE <table_name> SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;
-- UPDATE <table_name> SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;
-- UPDATE <table_name> SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;
-- UPDATE <table_name> SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;
-- UPDATE <table_name> SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;
--
-- ■やること
-- 1.Parquet形式 のデータが入った S3 バケットがある。
-- 2.そのデータをテーブルにロードする。
-- 3.「DEPT」と「JOB_TITLE」列の変更のみを追跡するストリームを作成する。
-- 4.テーブルに対していくつかの UPDATE（更新）を実行する。
-- 5.ストリームを使って、「対象の列が変更された行だけ」 が捕捉できているか確認する。

/*
* 準備
*/
--各コンテキストの設定
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;

-- Week 1 用のスキーマを作成して移動
CREATE SCHEMA IF NOT EXISTS WEEK_002;
USE SCHEMA WEEK_002;

/*
* 1.外部ステージ（S3バケット）を作成する。
*
* CREATE STAGE:https://docs.snowflake.com/ja/sql-reference/sql/create-stage
*
* 通常ストレージ統合の設定が必要だが、今回の課題用バケットは パブリック公開されているため、URLを指定するだけで良い。
*/

-- 外部ステージ（S3バケット）の作成（TEMPORARYで作成）
CREATE OR REPLACE TEMPORARY STAGE WEEK2_STAGE
  URL = 's3://frostyfridaychallenges/challenge_2/';

-- 作成されたか確認
SHOW STAGES;

/*
* 2.そのステージにある Parquet ファイルを確認する。
*
* LIST:https://docs.snowflake.com/ja/sql-reference/sql/list
*
*/

-- ステージ内のファイルを確認する
LIST @WEEK2_STAGE;

/*
* 3.ファイルフォーマットの作成
*
* CREATE FILE FORMAT:https://docs.snowflake.com/ja/sql-reference/sql/create-file-format
*
*/

CREATE OR REPLACE FILE FORMAT FF_PARQUET TYPE = PARQUET;

/*
* 4.ロード先のテーブルを作成し、データを投入する。
*
* COPY INTO <テーブル>:https://docs.snowflake.com/ja/sql-reference/sql/copy-into-table
*
*/

-- Parquetをフィールド名付き・型付きで事前確認
-- (巨大ファイルに備えて LIMIT を付ける)
SELECT
  $1:id::int                      AS id,
  $1:first_name::string           AS first_name,
  $1:last_name::string            AS last_name,
  $1:email::string                AS email,
  $1:dept::string                 AS dept,
  $1:job_title::string            AS job_title
FROM @WEEK2_STAGE (FILE_FORMAT => 'FF_PARQUET')
LIMIT 10;

-- テーブルの作成
-- スキーマを自動推定してテーブル作成
CREATE OR REPLACE TABLE EMPLOYEES
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'@WEEK2_STAGE',
        FILE_FORMAT => 'FF_PARQUET'
      )
    )
  );

-- COPY INTO
-- エラーも特に出ず、100件のロードが成功していればOK！
COPY INTO EMPLOYEES
FROM @WEEK2_STAGE
FILE_FORMAT = (FORMAT_NAME = 'FF_PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- 投入されたデータの更新
SELECT * FROM EMPLOYEES;


/*
* 5.特定列のみ監視するストリームの作成
* ストリームの紹介:https://docs.snowflake.com/ja/user-guide/streams-intro
*/

-- 1. ベーステーブルの変更追跡を有効化
ALTER TABLE EMPLOYEES SET CHANGE_TRACKING = TRUE;

-- 2. 監視したい列だけを含むビューを作成
-- (INFER_SCHEMAで作成された列名に合わせます)
CREATE OR REPLACE VIEW V_EMP_CHANGES AS
SELECT "employee_id", "dept", "job_title"
FROM EMPLOYEES;

-- 3. ビューの上にストリームを作成
CREATE OR REPLACE STREAM S_EMP_CHANGES ON VIEW V_EMP_CHANGES;

/*
* 6.データを更新しStreamを確認する
*
*/

-- 1.次のコマンドを実行します。
UPDATE EMPLOYEES SET "country" = 'Japan' WHERE "employee_id" = 8;
UPDATE EMPLOYEES SET "last_name" = 'Forester' WHERE "employee_id" = 22;
UPDATE EMPLOYEES SET "dept" = 'Marketing' WHERE "employee_id" = 25;
UPDATE EMPLOYEES SET "title" = 'Ms' WHERE "employee_id" = 32;
UPDATE EMPLOYEES SET "job_title" = 'Senior Financial Analyst' WHERE "employee_id" = 68;

--2.ストリームの確認 
SELECT
  "employee_id", "dept", "job_title",
  METADATA$ACTION,   -- INSERT or DELETE
  METADATA$ISUPDATE  -- UPDATEによる変更か？
FROM S_EMP_CHANGES
ORDER BY "employee_id", METADATA$ACTION;

/*
* 7.ストリームの「消費」を実行する
*
*/
-- 差分データを別テーブルに退避して「消費」する
CREATE OR REPLACE TABLE EMPLOYEES_DELTA AS
SELECT * FROM S_EMP_CHANGES;

-- 直後にもう一度ストリームを見ると、空になっているはず
SELECT COUNT(*) FROM S_EMP_CHANGES; 



-- 任意：練習後に片付け
-- DROP STREAM IF EXISTS S_EMP_CHANGES;
-- DROP VIEW   IF EXISTS V_EMP_CHANGES;
-- DROP TABLE  IF EXISTS EMPLOYEES_DELTA;
-- DROP TABLE  IF EXISTS EMPLOYEES;
-- DROP SCHEMA FROSTY_FRIDAY.WEEK_001;