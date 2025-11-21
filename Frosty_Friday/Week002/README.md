# 【Snowflake】Frosty Friday Week 2 やってみた：Streamで特定の列の変更（CDC）を追跡する
# Zenn:https://zenn.dev/yujmatsu/articles/20251126_frostyfriday_002

## はじめに

Snowflake の実践的なスキルを磨くためのコミュニティ課題、「Frosty Friday」。

今回の **Week 2** は、タイトルに "Intermediate"（中級）とある通り、少しテクニカルな内容に入ります。
テーマは **「ストリーム (Stream) による変更データキャプチャ (Change Data Capture: CDC)」** です。

しかも、ただ変更を追うだけではありません。
**「特定の列（DEPT と JOB_TITLE）が変更された時だけ検知したい！」**
という、実務でもよくある要件に対応する方法を学びます。

今回は **Snowflake の Stream 機能の仕組み** を図解イメージで解説しながら、この少しひねった課題を攻略していきます。

## 今週の課題：Week 2 - Intermediate

課題の詳細は公式サイトで確認できます。
[Week 2 – Intermediate – Streams](https://frostyfriday.org/blog/2022/07/15/week-2-intermediate/)

![](/images/20251126_frostyfriday_002/1.png) 

### 課題の要約
1.  **Parquet形式** のデータが入った S3 バケットがある。
2.  そのデータをテーブルにロードする。
3.  **「DEPT」と「JOB_TITLE」列の変更のみを追跡するストリーム**を作成する。
4.  テーブルに対していくつかの `UPDATE`（更新）を実行する。
5.  ストリームを使って、**「対象の列が変更された行だけ」** が捕捉できているか確認する。

要するに、**「テーブル全体の変更ではなく、特定の列の変更だけを監視したい」** という課題です。


## そもそも「Stream (ストリーム)」とは？

Snowflake の Stream は、**テーブルに対する変更（INSERT, UPDATE, DELETE）を記録してくれる「しおり（ブックマーク）」のようなオブジェクト**です。

### 普通のテーブルと何が違う？
* **テーブル:** 「現在の」データが入っている。
* **ストリーム:** 「ある時点から、**何が変わったか**（差分）」が入っている。

Snowflake のストリームでは、`UPDATE`（更新）は**「古い行の削除 (DELETE)」＋「新しい行の追加 (INSERT)」のペア**として記録されます。

この機能を使うと、「昨日から今日にかけて変更があったデータだけを別のテーブルに移す」といった処理（増分ロード）が非常に簡単に作れるようになります。

以下は以前執筆した記事です。
https://zenn.dev/yujmatsu/articles/20251027_sf_stream


## 実践：ハンズオン

それでは、Snowsight でやっていきましょう。
今回も `SYSADMIN` ロールと、前回作成したウェアハウスを使用します。

### Step 0: コンテキストの設定

```sql
USE ROLE SYSADMIN;
USE WAREHOUSE FF_WH;
USE DATABASE FROSTY_FRIDAY;

-- Week 2 用のスキーマを作成
CREATE SCHEMA IF NOT EXISTS WEEK_002;
USE SCHEMA WEEK_002;
```

### Step 1: ステージとファイル形式の準備

今回のデータは CSV ではなく **Parquet (パーケット)** という形式です。
まずはステージとファイルフォーマットを作成します。

```sql
-- 外部ステージの作成
CREATE OR REPLACE TEMPORARY STAGE WEEK2_STAGE
  URL = 's3://frostyfridaychallenges/challenge_2/';

-- ファイルの確認
LIST @WEEK2_STAGE;
```

Parquet ファイルの中身を確認します。単に `SELECT $1` するだけでなく、カラム名やデータ型の当たりをつけておくと、後続の作業がスムーズです。

```sql
-- Parquetをフィールド名付き・型付きで事前確認
-- (巨大ファイルに備えて LIMIT を付けるのが鉄則です)
SELECT
  $1:id::int                  AS id,
  $1:first_name::string       AS first_name,
  $1:last_name::string        AS last_name,
  $1:email::string            AS email,
  $1:dept::string             AS dept,
  $1:job_title::string        AS job_title
FROM @WEEK2_STAGE (FILE_FORMAT => (TYPE='PARQUET'))
LIMIT 10;
```

`id`, `dept`, `job_title` などのカラムが含まれていることが確認できました。

### Step 2: テーブル作成 (INFER_SCHEMA)

Parquet ファイルにはスキーマ情報（列名やデータ型）が含まれています。
手動で `CREATE TABLE` を書くのは大変なので、**`INFER_SCHEMA`** 関数を使って Snowflake に自動的にテーブル定義を作ってもらいましょう。

```sql
-- スキーマを自動推定してテーブル作成
CREATE OR REPLACE TABLE EMPLOYEES
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'@WEEK2_STAGE',
        FILE_FORMAT=>(TYPE='PARQUET')
      )
    )
  );
```

これで、カラム名や型が完璧な `EMPLOYEES` テーブルが作成されました。

### Step 3: データのロード

作成したテーブルにデータをロードします。
Parquet の列名とテーブルの列名を自動でマッチングさせるオプション (`MATCH_BY_COLUMN_NAME`) を使います。

```sql
COPY INTO EMPLOYEES
FROM @WEEK2_STAGE
FILE_FORMAT = (TYPE='PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

### Step 4: 特定列のみ監視するストリームの作成

ここが今回の最大のポイントです。
通常、`CREATE STREAM ON TABLE ...` とするとテーブル全体の変更を拾ってしまいます。
特定の列（`DEPT`, `JOB_TITLE`）の変更だけを検知するには、**ビュー (View) 上にストリームを作成**します。

ただし、ビューにストリームを作るには、ベーステーブル側で **変更追跡 (Change Tracking)** を有効にする必要があります。

```sql
-- 1. ベーステーブルの変更追跡を有効化
ALTER TABLE EMPLOYEES SET CHANGE_TRACKING = TRUE;

-- 2. 監視したい列だけを含むビューを作成
-- (INFER_SCHEMAで作成された列名に合わせます)
CREATE OR REPLACE VIEW V_EMP_CHANGES AS
SELECT ID AS EMPLOYEE_ID, DEPT, JOB_TITLE
FROM EMPLOYEES;

-- 3. ビューの上にストリームを作成
CREATE OR REPLACE STREAM S_EMP_CHANGES ON VIEW V_EMP_CHANGES;
```

これで、「`EMPLOYEE_ID`, `DEPT`, `JOB_TITLE` に変更があった時だけ記録されるストリーム」が完成しました！

> **TIPS: ストリームの状態確認**
> ストリームにデータ（変更分）が溜まっているかどうかは、以下の関数で確認できます。
> `SELECT SYSTEM$STREAM_HAS_DATA('S_EMP_CHANGES');`
> （現時点では作成直後なので `FALSE` が返ります）

### Step 5: データの変更 (UPDATE)

課題の要件に従って、いくつかのデータを更新してみます。
以下の5つの更新を行いますが、ストリームが反応すべきなのは `DEPT` と `JOB_TITLE` を更新した **ID: 25 と 68 だけ**のはずです。

```sql
-- 以下の変更を実行
UPDATE EMPLOYEES SET COUNTRY   = 'Japan'        WHERE ID = 8;   -- 対象外
UPDATE EMPLOYEES SET LAST_NAME = 'Smith'        WHERE ID = 22; -- 対象外
UPDATE EMPLOYEES SET DEPT      = 'Engineering'  WHERE ID = 25; -- ★対象！
UPDATE EMPLOYEES SET TITLE     = 'Ms'            WHERE ID = 32; -- 対象外
UPDATE EMPLOYEES SET JOB_TITLE = 'Senior Dev'    WHERE ID = 68; -- ★対象！
```

### Step 6: ストリームの確認 (答え合わせ)

さあ、ストリームの中身を見てみましょう。
期待通りなら、ID 25 と 68 の変更ログだけが入っているはずです。

```sql
SELECT
  EMPLOYEE_ID, DEPT, JOB_TITLE,
  METADATA$ACTION,   -- INSERT or DELETE
  METADATA$ISUPDATE  -- UPDATEによる変更か？
FROM S_EMP_CHANGES
ORDER BY EMPLOYEE_ID, METADATA$ACTION;
```

**実行結果のイメージ:**
| EMPLOYEE_ID | ... | METADATA$ACTION | METADATA$ISUPDATE |
| :--- | :--- | :--- | :--- |
| 25 | ... | DELETE | TRUE |
| 25 | ... | INSERT | TRUE |
| 68 | ... | DELETE | TRUE |
| 68 | ... | INSERT | TRUE |

ID 8, 22, 32 の変更は（ビューに含まれない列の変更なので）ストリームには記録されず、**ID 25 と 68 だけ**が見事にキャプチャされました！

### Step 7: ストリームの「消費」を体験する

最後に重要な挙動を確認します。
ストリームのデータは、`SELECT` するだけでは消えません（オフセットが進みません）。
`INSERT` や `CREATE TABLE AS SELECT` などの **DML** で使われて初めて「消費」され、空になります。

```sql
-- 差分データを別テーブルに退避して「消費」する
CREATE OR REPLACE TABLE EMPLOYEES_DELTA AS
SELECT * FROM S_EMP_CHANGES;

-- 直後にもう一度ストリームを見ると、空になっているはず
SELECT COUNT(*) FROM S_EMP_CHANGES; 
-- 結果: 0
```

これで、次回の変更までストリームは空の状態に戻りました。


## 今回の「コスト意識」ポイント

ストリームを使う際のコストや仕様についても触れておきましょう。

1.  **ストリーム自体は安い**
    * ストリームを作成しても、物理的にデータが複製されるわけではありません（オフセットという「しおり」を持つだけ）。そのため、ストレージコストは最小限で済みます。
2.  **SELECT では消費されない**
    * Step 6, 7 で見た通り、ただ見るだけなら何度でも確認できます。「処理が終わった（消費した）」とみなされるのは DML を実行した時だけです。
3.  **Staleness (鮮度切れ) に注意**
    * ストリームを作成したまま放置し、元のテーブルの Time Travel 期間（デフォルト1日）を過ぎると、ストリームは「Stale（古くなった）」状態になり、使えなくなります。
    * 実運用では `SYSTEM$STREAM_HAS_DATA` などで監視し、定期的にデータを消費するパイプラインを組む必要があります。


## 次回予告

次回も続けて **Week 3** に挑戦予定です。
今回の `Stream` は「テーブルデータの変更」を検知する機能でしたが、次回は「メタデータ」管理に関する課題のようです。

## 参考資料
* [Frosty Friday Week 2](https://frostyfriday.org/blog/2022/07/15/week-2-intermediate/)
* [Snowflake Docs: CREATE STREAM](https://docs.snowflake.com/ja/sql-reference/sql/create-stream)
* [Snowflake Docs: INFER_SCHEMA](https://docs.snowflake.com/ja/sql-reference/functions/infer_schema)
* [Snowflake Docs: 変更追跡 (Change Tracking)](https://docs.snowflake.com/ja/user-guide/streams-intro)