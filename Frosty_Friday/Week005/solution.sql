----
-- Week005 回答例 (SQL UDF vs Python UDF)
----
-- 問題(英文)
-- This week, we’re using a feature that, at the time of writing, is pretty hot off the press :
-- Python in Snowflake.
-- To start out create a simple table with a single column with a number, the size and amount are up to you, 
-- After that we’ll start with a very basic function: multiply those numbers by 3.
-- The challenge here is not ‘build a very difficult python function’ but to build and use the function in Snowflake.
-- We can test the code with a simple select statement :
-- SELECT timesthree(start_int)
-- FROM FF_week_5
--
-- 問題(和訳)
-- 今週は、執筆時点でかなり話題の最新機能「Python in Snowflake」を使用します。
-- まず手始めに、数値が入った列を1つ持つシンプルなテーブルを作成してください。サイズやデータ量は自由です。
-- その後、非常に基本的な関数を作成します。その数値を3倍にする関数です。
-- ここでの課題は「難しいPython関数を作ること」ではなく、「Snowflake内でPython関数を構築し、使用すること」です。
-- 作成したコードは、以下のようなシンプルなSELECT文でテストできます。
-- SELECT timesthree(start_int)
-- FROM FF_week_5
--
-- ■やること
-- 1. 数値データを格納するテーブルを作成し、データを投入する。
-- 2. 従来の SQL UDF を作成する（比較用）。
-- 3. Python UDF を作成する（本題）。
-- 4. 両方の関数を実行し、結果が同じになることを確認する。

/*
* 準備
*/

--各コンテキストの設定
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;

-- Week 5 用のスキーマを作成して移動
CREATE SCHEMA IF NOT EXISTS WEEK_005;
USE SCHEMA WEEK_005;


/*
* 1. テスト用テーブルの作成とデータ投入
*/

-- 数値カラム(start_int)を持つテーブルを作成
CREATE OR REPLACE TABLE FF_week_5 (
    start_int INT
);

-- テストデータを挿入
INSERT INTO FF_week_5 (start_int) VALUES 
    (10),
    (20),
    (33),
    (100),
    (-5),
    (NULL);-- NULLの挙動も見ておきましょう

-- データ確認
SELECT * FROM FF_week_5;


/*
* 2. SQL UDF (従来のSQL関数) の作成
* 比較のために、まずは標準的なSQL関数を作成します。
*/

CREATE OR REPLACE FUNCTION timesthree_sql(input_int INT)
RETURNS INT
AS
$$
    input_int * 3
$$;


/*
* 3. Python UDF (ユーザー定義関数) の作成
* LANGUAGE PYTHON を指定し、Pythonコードでロジックを記述します。
*/

CREATE OR REPLACE FUNCTION timesthree_python(input_int INT)
RETURNS INT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'       -- 利用するPythonのバージョン
HANDLER = 'multiply_logic' -- 呼び出す関数名(def)を指定
AS
$$
def multiply_logic(x):
    # ここはPythonの世界
    if x is None:
        return None
    return x * 3
$$;


/*
* 4. 実行と結果の比較
* SQL版とPython版を同時に呼び出して結果を検証します。
*/

SELECT 
    start_int,
    timesthree_sql(start_int)    AS SQL_RESULT,    -- SQL UDFの結果
FROM FF_week_5;

SELECT 
    start_int,
    timesthree_python(start_int) AS PYTHON_RESULT  -- Python UDFの結果
FROM FF_week_5;

-- どちらも同じ結果(30, 60...)が返ってくれば成功です！
-- 処理速度として微妙に早いのはやはりSQLの方になっていると思います。（ただ、簡単な処理なのでそこまで差がないはずです）

-- 任意：後片付け
-- DROP FUNCTION IF EXISTS timesthree_sql(INT);
-- DROP FUNCTION IF EXISTS timesthree_python(INT);
-- DROP TABLE IF EXISTS FF_week_5;
-- DROP SCHEMA IF EXISTS WEEK_005;