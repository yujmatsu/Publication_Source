----
-- Week004 回答例
----
-- 問題(英文)
-- Frosty Friday Consultants has been hired by the University of Frost’s history department; they want data on monarchs in their data warehouse for analysis. Your job is to take the JSON file located here, ingest it into the data warehouse, and parse it into a table that looks like this:
-- 
-- End Result: (See image in blog)
-- Separate columns for nicknames and consorts 1 – 3, many will be null.
-- An ID in chronological order (birth).
-- An Inter-House ID in order as they appear in the file.
-- There should be 26 rows at the end.
-- Hints:
-- Make sure you don’t lose any rows along the way.
-- Be sure to investigate all the outputs and parameters available when transforming JSON.
--
-- 問題(和訳)
-- Frosty Fridayコンサルタントは、Frost大学の歴史学部から依頼を受けました。彼らは分析のためにDWHに君主（Monarchs）のデータを求めています。
-- あなたの仕事は、指定されたJSONファイルを取得してDWHに取り込み、指定された形式のテーブルに解析することです。
-- 
-- ニックネームと配偶者（Consort）1〜3を別々の列に分けてください（多くはNULLになります）。
-- IDは、出生順（年代順）に振ってください。
-- Inter-House ID（家系内ID）は、ファイルに表示される順序通りに振ってください。
-- 最終的に26行になるはずです。
-- ヒント：
-- 処理の途中で行を失わないように注意してください。
-- JSONを変換する際に利用可能なすべての出力とパラメータを必ず調査してください。
--
-- ■やること
-- 1. S3バケットを参照するステージを作成する。
-- 2. JSON用ファイルフォーマットを作成する。
-- 3. 一度生データをVARIANT型のままテーブルに取り込む（Raw Data Table）。
-- 4. LATERAL FLATTEN を使用して階層（Era/House -> Monarchs）を展開し、最終テーブルを作成する。

/*
* 準備
*/

--各コンテキストの設定
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;

-- Week 4 用のスキーマを作成して移動
CREATE SCHEMA IF NOT EXISTS WEEK_004;
USE SCHEMA WEEK_004;


/*
* 1. ステージとファイルフォーマットの作成
*/

-- 外部ステージの作成
CREATE OR REPLACE TEMPORARY STAGE WEEK4_STAGE
  URL = 's3://frostyfridaychallenges/challenge_4/';

-- JSON用のファイルフォーマット作成
-- STRIP_OUTER_ARRAY = TRUE で外側の[]を削除してロード
CREATE OR REPLACE FILE FORMAT FF_JSON_FORMAT
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE; -- 外側の大括弧 [] を取り除くオプション


/*
* 2. 生データ（Raw Data）の取り込み
* 最終クエリで 'WEEK4_RAW_DATA' を使用するため、先に作成します。
*/

-- 生データを格納するテーブルの作成とデータロード
CREATE OR REPLACE TABLE WEEK4_RAW_DATA AS
SELECT $1 AS json_data
FROM @WEEK4_STAGE
(FILE_FORMAT => 'FF_JSON_FORMAT');


/*
* 3. データの解析と最終出力テーブルの作成
* - ROW_NUMBERで誕生順のID付与
* - 配列インデックスを利用したInter-House IDの付与
* - 配列要素（Nickname, Consort）の個別抽出
* FLATTEN:https://docs.snowflake.com/ja/sql-reference/functions/flatten
*/

CREATE OR REPLACE TABLE WEEK4_OUTPUT AS
SELECT 
    -- 誕生順にIDを振る
    ROW_NUMBER() OVER (ORDER BY m.value:Birth::DATE) AS ID,
    
    -- 家系内での順番 (配列のインデックス + 1)
    m.index + 1 AS INTER_HOUSE_ID,
    
    -- 第1階層 (Era)
    root.json_data:Era::STRING AS ERA,
    
    -- 第2階層 (House)
    h.value:House::STRING AS HOUSE,
    
    -- 第3階層 (Monarch)
    m.value:Name::STRING AS NAME,
    
    -- Nickname (配列)
    m.value:Nickname[0]::STRING AS NICKNAME_1,
    m.value:Nickname[1]::STRING AS NICKNAME_2,
    m.value:Nickname[2]::STRING AS NICKNAME_3,
    
    -- 詳細情報
    m.value:Birth::DATE AS BIRTH,
    m.value:"Place of Birth"::STRING AS PLACE_OF_BIRTH,
    m.value:"Start of Reign"::DATE AS START_OF_REIGN,
    
    -- Consort (配列)
    -- エスケープ文字(\/)を利用してキーを指定
    m.value:"Consort\/Queen Consort"[0]::STRING AS QUEEN_OR_QUEEN_CONSORT_1,
    m.value:"Consort\/Queen Consort"[1]::STRING AS QUEEN_OR_QUEEN_CONSORT_2,
    m.value:"Consort\/Queen Consort"[2]::STRING AS QUEEN_OR_QUEEN_CONSORT_3,
    
    -- その他の詳細情報
    m.value:"End of Reign"::DATE AS END_OF_REIGN,
    m.value:Duration::STRING AS DURATION,
    m.value:Death::DATE AS DEATH,
    m.value:"Age at Time of Death"::STRING AS AGE_AT_TIME_OF_DEATH_YEARS,
    m.value:"Place of Death"::STRING AS PLACE_OF_DEATH,
    m.value:"Burial Place"::STRING AS BURIAL_PLACE

FROM WEEK4_RAW_DATA root,
LATERAL FLATTEN(input => root.json_data:Houses) h,
LATERAL FLATTEN(input => h.value:Monarchs) m
ORDER BY ID;


/*
* 4. 結果の確認
*/
SELECT * FROM WEEK4_OUTPUT;


-- 任意：後片付け
-- DROP TABLE IF EXISTS WEEK4_OUTPUT;
-- DROP TABLE IF EXISTS WEEK4_RAW_DATA;
-- DROP STAGE IF EXISTS WEEK4_STAGE;
-- DROP FILE FORMAT IF EXISTS FF_JSON_FORMAT;
-- DROP SCHEMA IF EXISTS WEEK_004;