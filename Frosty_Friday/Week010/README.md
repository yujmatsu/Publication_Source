# 【Snowflake】Frosty Friday Week 10 やってみた：ストアドプロシージャでウェアハウスサイズを動的に切り替える
# Zenn:https://zenn.dev/yujmatsu/articles/20251210_frostyfriday_010

## はじめに

Snowflake の実践的なスキルを磨くためのコミュニティ課題、「Frosty Friday」。

今回の **Week 10** は、難易度 "Hard"（上級）。
記念すべき10週目のテーマは **「ストアドプロシージャ (Stored Procedure)」** です。

「大量の小さなファイルは `X-SMALL` で、巨大なファイルは `SMALL` でロードしたい」
「ファイルサイズを見て、自動でウェアハウスを切り替えるような処理はできないの？」

通常の SQL だけでは難しいこのような「動的な条件分岐」や「ループ処理」も、ストアドプロシージャを使えば実現できます。
今回は Snowflake Scripting (SQL) を使って、**ファイルサイズに応じてウェアハウスを自動選択するETL処理** を構築してみましょう。


## 今週の課題：Week 10 - Hard

課題の詳細は公式サイトで確認できます。
[Week 10 – Hard – Stored Procedures](https://frostyfriday.org/blog/2022/08/19/week-10-hard/)


### 課題のストーリー
ある会社では、S3 にアップロードされた CSV ファイルを毎日ロードしています。
しかし、ファイルのサイズはまちまちです。コスト効率を最適化するために、以下のロジックでロードを行いたいと考えています。

### 要件の要約
1.  **動的なウェアハウス選択:**
    * ファイルサイズが **10KB 超** の場合 → `my_small_wh` (Small) を使用
    * ファイルサイズが **10KB 以下** の場合 → `my_xsmall_wh` (X-Small) を使用
2.  **実装方法:**
    * 単一のストアドプロシージャ (`dynamic_warehouse_data_load`) として実装する。
    * Snowpipe は使わず、手動実行 (CALL) を想定する。
3.  **確認:**
    * プロシージャの戻り値として、処理したファイル数や行数を返す。
    * `QUERY_HISTORY` を確認し、実際にウェアハウスが切り替わっていることを裏取りする。


## 知識：Snowflake Scripting とは？

Snowflake Scripting は、Snowflake 内で手続き型ロジック（変数、IF文、ループ、例外処理など）を書くための言語です。
SQL を拡張したような構文で、PL/SQL に似ています。

今回の課題を解くための重要ポイントは以下の3つです。

1.  **`RESULT_SCAN(LAST_QUERY_ID())`**:
    * 直前に実行したクエリ（今回は `LIST` コマンド）の結果をテーブルのように参照するテクニック。
2.  **`CURSOR` (カーソル) と `FOR` ループ**:
    * クエリ結果を1行ずつ取り出して処理するための仕組み。
3.  **`EXECUTE IMMEDIATE`**:
    * 文字列として組み立てた SQL（動的 SQL）を実行するコマンド。これで `USE WAREHOUSE ...` や `COPY INTO ...` を実行します。


## 実践：ハンズオン

それでは、Snowsight でやっていきましょう。

### Step 0: コンテキストの設定

```sql
USE ROLE SYSADMIN;
USE DATABASE FROSTY_FRIDAY;

CREATE SCHEMA IF NOT EXISTS WEEK_010;
USE SCHEMA WEEK_010;
```

### Step 1: ウェアハウスの準備

サイズ違いの2つのウェアハウスを作成します。
（※検証後は削除または停止するのを忘れないようにしましょう）

```sql
-- 小さいファイル用
CREATE WAREHOUSE IF NOT EXISTS my_xsmall_wh 
    WITH WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60;
    
-- 大きいファイル用
CREATE WAREHOUSE IF NOT EXISTS my_small_wh 
    WITH WAREHOUSE_SIZE = SMALL
    AUTO_SUSPEND = 60;

-- 確認のため、最初は別のウェアハウスにしておきます
USE WAREHOUSE TEMP_WH;
```

### Step 2: テーブル・ステージの準備

ロード先のテーブルと、S3 を参照するステージを作成します。

```sql
-- ロード先テーブル
CREATE OR REPLACE TABLE WEEK10_RESULTS (
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
```

### Step 3: ストアドプロシージャの作成

ここが本題です。
ロジックは以下の通りです。
1.  `LIST` コマンドでファイル一覧とサイズを取得。
2.  結果をループし、サイズ判定。
3.  `USE WAREHOUSE` でウェアハウスを切り替え。
4.  `COPY INTO` で1ファイルずつロード。



```sql
create or replace procedure dynamic_warehouse_data_load(stage_name string, table_name string)
  returns variant
  language sql
  execute as caller -- 呼び出し元の権限(SYSADMIN)で実行するために必要
as
$$
declare
  v_threshold_bytes number := 10240; -- 閾値: 10KB
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
    
    -- ファイルサイズ判定とウェアハウス切り替え
    if (r.size_bytes > v_threshold_bytes) then
      execute immediate 'use warehouse my_small_wh';
      v_small := v_small + 1;
    else
      execute immediate 'use warehouse my_xsmall_wh';
      v_xsmall := v_xsmall + 1;
    end if;

    -- パスからファイル名部分だけを抽出 (s3://.../file.csv -> file.csv)
    -- LISTコマンドの結果(name)はフルパスで返るため、COPYのFILES句用にファイル名のみにする
    v_file_name := split_part(r.name, '/', -1);

    -- そのファイルだけ COPY
    v_sql := 'copy into ' || table_name ||
             ' from @' || stage_name ||
             ' files = (''' || v_file_name || ''')';
    
    execute immediate :v_sql;

    v_files := v_files + 1;
  end for;

  -- 4. 挿入行数の取得
  -- 全ロード完了後の行数をカウント
  v_sql := 'select count(*) from ' || table_name;
  execute immediate :v_sql;
  
  -- 直前の結果(count)を取得
  let cur_count cursor for select $1 from table(result_scan(last_query_id()));
  open cur_count;
  fetch cur_count into :v_rows;
  close cur_count;

  -- 結果をJSON形式で返す
  return object_construct(
    'files_processed', v_files,
    'files_via_xsmall', v_xsmall,
    'files_via_small',  v_small,
    'rows_in_table',    v_rows
  );
end;
$$;
```

### Step 4: 実行と裏取り確認

プロシージャを実行し、本当にウェアハウスが切り替わったかを確認します。

```sql
-- 1. プロシージャの実行
CALL dynamic_warehouse_data_load('week_10_frosty_stage', 'WEEK10_RESULTS');
```

実行結果（JSON）が表示され、`files_via_xsmall` と `files_via_small` の両方がカウントされていれば、ロジック自体は動いています。

次に、本当にウェアハウスが切り替わって実行されたのか、`QUERY_HISTORY` で「裏取り」をします。



```sql
-- 3. QUERY_HISTORY によるウェアハウス使用履歴の確認
SELECT 
    QUERY_ID,
    QUERY_TEXT,
    WAREHOUSE_NAME, -- ここで実際に使われたWHを確認！
    START_TIME
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 100)) 
ORDER BY START_TIME DESC;
```


**確認ポイント:**
`WAREHOUSE_NAME` 列を見てください。
`my_xsmall_wh` と `my_small_wh` が混在していれば大成功です。
ファイルサイズに応じて、動的にウェアハウスを乗り換えながらロードが実行された証拠です。


### Step 5: お片付け

検証が終わったら、リソースを削除しておきましょう。

```sql
-- 後片付け
DROP PROCEDURE IF EXISTS dynamic_warehouse_data_load(STRING, STRING);
DROP STAGE IF EXISTS week_10_frosty_stage;
DROP TABLE IF EXISTS WEEK10_RESULTS;
DROP FILE FORMAT IF EXISTS FF_CSV;
DROP WAREHOUSE IF EXISTS my_xsmall_wh;
DROP WAREHOUSE IF EXISTS my_small_wh;
DROP SCHEMA IF EXISTS WEEK_010;
```



## 学びとポイント

今回の課題は、Snowflake Scripting の威力を体感できる良い例でした。

1.  **プロシージャによる制御**
    * SQL だけでは不可能な「ループ処理」や「条件分岐」も、ストアドプロシージャを使えば実現できます。
2.  **`RESULT_SCAN` の活用**
    * `LIST` コマンドの結果をテーブルのように扱えるのは非常に強力です。メタデータ駆動の処理を作る際の鉄板パターンです。
3.  **動的 SQL (`EXECUTE IMMEDIATE`)**
    * 変数を使って SQL を組み立て、実行時に評価させることで、柔軟な処理が可能になります。
4.  **`EXECUTE AS CALLER`**
    * プロシージャ内で `USE WAREHOUSE` のようなセッション設定を変更するコマンドを実行する場合、`EXECUTE AS CALLER`（呼び出し元の権限で実行）を指定する必要があります。（デフォルトの `OWNER` だと権限エラーになることがあります）


## 次回予告

次回は **Week 11** に挑戦予定です。
テーマは再び **「タスク (Tasks)」** のようですが、より高度なスケジューリングや依存関係の管理が登場しそうです。


## 参考資料
* [Frosty Friday Week 10](https://frostyfriday.org/blog/2022/08/19/week-10-hard/)
* [Snowflake Docs: Snowflake Scripting 開発ガイド](https://docs.snowflake.com/ja/developer-guide/snowflake-scripting/index)
* [Snowflake Docs: RESULT_SCAN](https://docs.snowflake.com/ja/sql-reference/functions/result_scan)
* [Snowflake Docs: EXECUTE IMMEDIATE](https://docs.snowflake.com/ja/sql-reference/sql/execute-immediate)
