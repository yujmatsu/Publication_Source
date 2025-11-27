----
-- Week003 回答例 (修正版)
----
-- 問題(英文そのまま)
-- In Week 1 we looked at ingesting S3 data, now it’s time to take that a step further. So this week we’ve got a short list of tasks for you all to do.
-- The basics aren’t earth-shattering but might cause you to scratch your head a bit once you start building the solution.
-- Frosty Friday Inc., your benevolent employer, has an S3 bucket that was filled with .csv data dumps. These dumps aren’t very complicated and all have the same style and contents. All of these files should be placed into a single table.
-- However, it might occur that some important data is uploaded as well, these files have a different naming scheme and need to be tracked. We need to have the metadata stored for reference in a separate table. You can recognize these files because of a file inside of the S3 bucket. This file, keywords.csv, contains all of the keywords that mark a file as important.
-- Objective:
-- Create a table that lists all the files in our stage that contain any of the keywords in the keywords.csv file.
-- The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_3/
--
-- 問題(和訳)
-- Week 1ではS3データの取り込みを行いましたが、今回はさらに一歩進めてみましょう。今週は、皆さんにやってもらうタスクの短いリストがあります。
-- 基本的なことは驚くようなものではありませんが、ソリューションの構築を始めると少し頭を悩ませるかもしれません。
-- あなたの慈悲深い雇用主であるFrosty Friday社は、.csvデータのダンプで満たされたS3バケットを持っています。これらのダンプはそれほど複雑ではなく、すべて同じスタイルと内容を持っています。これらのファイルはすべて単一のテーブルに配置する必要があります。
-- しかし、いくつかの重要なデータもアップロードされることがあり、これらのファイルは異なる命名規則を持っており、追跡する必要があります。参照用にメタデータを別のテーブルに保存する必要があります。
-- S3バケット内のファイルによって、これらのファイルを認識できます。このファイル、keywords.csvには、ファイルを重要としてマークするすべてのキーワードが含まれています。
-- 目的：
-- ステージ内のファイルのうち、keywords.csvファイル内のキーワードのいずれかを含むすべてのファイルをリストするテーブルを作成してください。
-- S3バケットのURIは s3://frostyfridaychallenges/challenge_3/ です。
--
-- ■やること
-- 1. S3バケットを参照するステージを作成する。
-- 2. 「keywords.csv」の中身を一時テーブルとして読み込む（ここが重要）。
-- 3. ステージ上のファイルとキーワードテーブルを結合し、該当ファイルのみ行数をカウントする。
-- 4. 結果をテーブルに保存する。

/*
* 準備
*/
-- コンテキストの設定
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;

-- Week 3 用のスキーマを作成して移動
CREATE SCHEMA IF NOT EXISTS WEEK_003;
USE SCHEMA WEEK_003;

/*
* 1. 外部ステージ（S3バケット）を作成する
*/

-- 今回もパブリックバケットなのでURL指定のみでOK
CREATE OR REPLACE TEMPORARY STAGE WEEK3_STAGE
URL = 's3://frostyfridaychallenges/challenge_3/';

/*
* 2. キーワードファイルの読み込み準備
* キーワードが入っている 'keywords.csv' を読むためにファイルフォーマットを作成します。
*/

-- CSV用のファイルフォーマット
CREATE OR REPLACE FILE FORMAT FF_CSV
TYPE = CSV
SKIP_HEADER = 1; -- ヘッダーがある場合に備えて1行スキップ

/*
* 3. キーワードのテーブル化
* ステージ上のファイルを直接サブクエリで何度も呼ぶと失敗しやすいため、
* 一度小さな一時テーブルにキーワードをロードします。
*/

CREATE OR REPLACE TEMPORARY TABLE WEEK3_KEYWORDS AS
SELECT $1::STRING AS KEYWORD
FROM @WEEK3_STAGE/keywords.csv (FILE_FORMAT => 'FF_CSV');

-- 中身を確認（3つのキーワードが入っているはずです）
SELECT * FROM WEEK3_KEYWORDS;

/*
* 4. ファイルごとの行数集計とフィルタリング
* データ本体(@WEEK3_STAGE)に対して、ファイル名にキーワードが含まれるか
* EXISTS で判定してから集計します。
*/

CREATE OR REPLACE TABLE WEEK3_RESULTS AS
SELECT
    D.METADATA$FILENAME AS FILENAME,
    COUNT(*) AS NUMBER_OF_ROWS
FROM @WEEK3_STAGE (
    FILE_FORMAT => 'FF_CSV',
    PATTERN => '.*week3_data.*' -- データファイルのみを対象にする（keywords.csvを除外）
) AS D
WHERE EXISTS (
    SELECT 1
    FROM WEEK3_KEYWORDS AS K
    WHERE CONTAINS(D.METADATA$FILENAME, K.KEYWORD)
)
GROUP BY 
    D.METADATA$FILENAME
ORDER BY 
    D.METADATA$FILENAME;


/*
* 5. 結果の確認
*/
SELECT * FROM WEEK3_RESULTS
ORDER BY FILENAME;

-- 任意：練習後に片付け
-- DROP TABLE IF EXISTS WEEK3_RESULTS;
-- DROP TABLE IF EXISTS WEEK3_KEYWORDS;
-- DROP STAGE IF EXISTS WEEK3_STAGE;
-- DROP FILE FORMAT IF EXISTS FF_CSV;
-- DROP SCHEMA IF EXISTS WEEK_003;