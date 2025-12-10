----
-- Week010 回答例 (検証用クエリ追加版)
----
-- 問題(英文)
-- ... (前略) ...
-- execute a single command (stored procedure)
-- do so manually, meaning it won’t be scheduled and there won’t be any Snowpipes
-- dynamically determine the warehouse size, if a file is over 10KB they want to use a small warehouse, anything under that size should be handled by an xsmall warehouse.
--
-- RESULT
-- When you execute the last line of the above script “call dynamic_warehouse_data_load()” then you should get the following result.
-- And when querying the QUERY_HISTORY, you should see that different warehouses were used for different files.
--
-- 問題(和訳)
-- ... (前略) ...
-- 単一のコマンド（ストアドプロシージャ）を実行する。
-- ウェアハウスのサイズを動的に決定する。ファイルが10KBを超える場合はSmall、それ以下はX-Small。
--
-- ■結果の確認
-- スクリプトの最後の行 "call dynamic_warehouse_data_load()" を実行すると、結果（メッセージ）が返されます。
-- また、QUERY_HISTORY をクエリすると、ファイル（のサイズ）に応じて異なるウェアハウスが使用されたことを確認できるはずです。
--
-- ■やること
-- 1. ウェアハウス、テーブル、ファイルフォーマット、ステージを作成する。
-- 2. ファイルサイズに応じてウェアハウスを動的に切り替えるストアドプロシージャを作成する。
-- 3. プロシージャを実行し、結果メッセージを確認する。
-- 4. QUERY_HISTORYを参照し、実際に意図したウェアハウスでCOPYコマンドが実行されたか裏取りする。

/*
* ------------------------------------------------------------------------------
* STEP 0: コンテキスト設定
* ------------------------------------------------------------------------------
*/
USE ROLE SYSADMIN;
USE DATABASE FROSTY_FRIDAY;

CREATE SCHEMA IF NOT EXISTS WEEK_010;
USE SCHEMA WEEK_010;

/*
* ------------------------------------------------------------------------------
* STEP 1: ウェアハウスの作成
* ------------------------------------------------------------------------------
*/
CREATE WAREHOUSE IF NOT EXISTS my_xsmall_wh 
    WITH WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 120;
    
CREATE WAREHOUSE IF NOT EXISTS my_small_wh 
    WITH WAREHOUSE_SIZE = SMALL
    AUTO_SUSPEND = 120;

-- 確認のため、最初は別のウェアハウスにしておきます
USE WAREHOUSE TEMP_WH;


/*
* ------------------------------------------------------------------------------
* STEP 2: テーブル・フォーマット・ステージ作成
* ------------------------------------------------------------------------------
*/
-- テーブル作成
CREATE OR REPLACE TABLE WEEK10_RESULTS
(
    date_time DATETIME,
    trans_amount DOUBLE
);

-- CSVフォーマット
CREATE OR REPLACE FILE FORMAT FF_CSV
    TYPE = CSV
    SKIP_HEADER = 1;

-- ステージ作成
CREATE OR REPLACE STAGE week_10_frosty_stage
    URL = 's3://frostyfridaychallenges/challenge_10/'
    FILE_FORMAT = FF_CSV;


/*
* ------------------------------------------------------------------------------
* STEP 3: ストアドプロシージャの作成
* ------------------------------------------------------------------------------
*/
create or replace procedure dynamic_warehouse_data_load(stage_name string, table_name string)
  returns variant
  language sql
  execute as caller
as
$$
declare
  v_threshold_bytes number := 10240; -- 10KB
  v_files  number := 0;
  v_small  number := 0;
  v_xsmall number := 0;
  v_rows   number := 0;
  v_sql    string;
  v_file_name string;
begin
  -- 1. ステージ上のファイル一覧を取得
  v_sql := 'list @' || stage_name;
  execute immediate :v_sql;

  -- 2. 直前のクエリ結果(LIST)を参照するカーソルを定義
  let cur_files cursor for 
      select "name" as name, "size" as size_bytes 
      from table(result_scan(last_query_id()));

  -- 3. ループ処理
  for r in cur_files do
    
    -- ファイルサイズ判定
    if (r.size_bytes > v_threshold_bytes) then
      execute immediate 'use warehouse my_small_wh';
      v_small := v_small + 1;
    else
      execute immediate 'use warehouse my_xsmall_wh';
      v_xsmall := v_xsmall + 1;
    end if;

    -- パスからファイル名部分だけを抽出 (s3://.../file.csv -> file.csv)
    v_file_name := split_part(r.name, '/', -1);

    -- そのファイルだけ COPY
    v_sql := 'copy into ' || table_name ||
             ' from @' || stage_name ||
             ' files = (''' || v_file_name || ''')';
    
    execute immediate :v_sql;

    v_files := v_files + 1;
  end for;

  -- 4. 挿入行数の取得 (修正箇所)
  -- EXECUTE IMMEDIATE ... INTO を使用せず、カーソル経由で安全に値を取得します
  v_sql := 'select count(*) from ' || table_name;
  execute immediate :v_sql;
  
  -- 直前の結果(count)を取得するカーソル
  let cur_count cursor for select $1 from table(result_scan(last_query_id()));
  open cur_count;
  fetch cur_count into :v_rows; -- ここで変数に行数を格納
  close cur_count;

  return object_construct(
    'files_processed', v_files,
    'files_via_xsmall', v_xsmall,
    'files_via_small',  v_small,
    'rows_in_table',    v_rows
  );
end;
$$;



/*
* ------------------------------------------------------------------------------
* STEP 4: 実行と結果確認 (追加対応)
* ------------------------------------------------------------------------------
*/

-- 1. プロシージャの実行
-- 実行結果として "Loaded data into ... using warehouse: ..." が表示されることを確認します。
CALL dynamic_warehouse_data_load('week_10_frosty_stage', 'WEEK10_RESULTS');


-- 2. データのロード確認
SELECT COUNT(*) AS LOADED_ROWS FROM WEEK10_RESULTS;


-- 3. QUERY_HISTORY によるウェアハウス使用履歴の確認
-- プロシージャ内で実行された 'COPY INTO' コマンドが、
-- 期待通り 'my_small_wh' (またはサイズによってはxsmall) で実行されているかを確認します。
-- ※直近100件のクエリ履歴から検索します。

SELECT 
    QUERY_ID,
    QUERY_TEXT,
    WAREHOUSE_NAME, -- プロシージャが選んだウェアハウス
    START_TIME,
    USER_NAME
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 100)) 
ORDER BY START_TIME DESC;


/*
* 後片付け
*/
/*
DROP PROCEDURE IF EXISTS dynamic_warehouse_data_load(STRING, STRING);
DROP STAGE IF EXISTS week_10_frosty_stage;
DROP TABLE IF EXISTS WEEK10_RESULTS;
DROP FILE FORMAT IF EXISTS FF_CSV;
DROP WAREHOUSE IF EXISTS my_xsmall_wh;
DROP WAREHOUSE IF EXISTS my_small_wh;
DROP SCHEMA IF EXISTS WEEK_010;
*/