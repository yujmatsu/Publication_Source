# 【Snowflake】Frosty Friday Week 3 やってみた：S3上のファイル名（メタデータ）を検索する
# Zenn:https://zenn.dev/yujmatsu/articles/20251128_frostyfriday_003

## はじめに

Snowflake の実践的なスキルを磨くためのコミュニティ課題、「Frosty Friday」。

今回の **Week 3** は、再び難易度 "Basic"（初級）に戻りますが、非常に重要なテーマです。
ズバリ、**「メタデータ（Metadata）の活用」** です。

「S3 に大量のファイルがあるけど、特定の名前が含まれるファイルだけ処理したい」
「ファイルの中身をロードする前に、ファイル名の一覧を取得したい」

ETL処理を作っていると、必ずこの壁にぶつかります。
今回は、Snowflake の強力なメタデータ参照機能を使って、この課題をサクッと解決しましょう。

## 今週の課題：Week 3 - Basic

課題の詳細は公式サイトで確認できます。
[Week 3 – Basic – Metadata queries](https://frostyfriday.org/blog/2022/07/15/week-3-basic/)

### 課題のストーリー
S3 バケットに大量のファイルが置かれています。
また、同じ場所に `keywords.csv` というファイルがあり、そこには「検索キーワード」が書かれています。

### やること
1.  外部ステージを作成する。
2.  `keywords.csv` の中身（キーワード）を読み込む。
3.  ステージ上の全ファイルの中から、**ファイル名にそのキーワードが含まれているファイル** を探し出す。
4.  該当するファイルの「ファイル名」と「行数」をリストアップしたテーブルを作成する。

※公式課題では「ファイル名の一覧」が必須条件ですが、この記事では実務を意識して **行数も一緒に集計** してみます。

## 知識：METADATA$FILENAME とは？

Snowflake では、ステージ（S3など）にあるファイルに対して `SELECT` をかける際、ファイルの中身（`$1`, `$2`...）だけでなく、**ファイルそのものの情報（メタデータ）** も取得できます。

その代表格が **`METADATA$FILENAME`** です。

* **`$1`**: ファイルのデータ（1列目）
* **`METADATA$FILENAME`**: そのファイルのパスと名前
* **`METADATA$FILE_ROW_NUMBER`**: その行がファイルの何行目か

これを使うと、データをロードしなくても「どんなファイルがあるか」を SQL で自由にフィルタリングできるのです。

## 実践：ハンズオン

それでは、Snowsight でやっていきましょう。

### Step 0: コンテキストの設定

```sql
USE ROLE SYSADMIN;
USE WAREHOUSE FF_WH;
USE DATABASE FROSTY_FRIDAY;

-- Week 3 用のスキーマを作成
CREATE SCHEMA IF NOT EXISTS WEEK_003;
USE SCHEMA WEEK_003;
```

### Step 1: 外部ステージの作成

まずはデータが置かれている S3 バケットへのステージを作成します。

**※注記:** 今回使用する S3 バケットは**パブリック（公開）**設定になっているため、`STORAGE INTEGRATION`（認証設定）は不要です。URLを指定するだけでアクセスできます。

```sql
/*
* 1. 外部ステージ（S3バケット）を作成する
*/

CREATE OR REPLACE STAGE WEEK3_STAGE
  URL = 's3://frostyfridaychallenges/challenge_3/';

-- ファイルの確認
LIST @WEEK3_STAGE;
```

`LIST` コマンドを実行すると、`week3_data_...` というファイルが大量にあることがわかります。
そして、一つだけ `keywords.csv` というファイルも混ざっています。

### Step 2: ファイルフォーマットの準備

CSVファイルを読み込むためのフォーマットを定義します。
今回のデータ（およびキーワードファイル）にはヘッダーが含まれている可能性があるため、`SKIP_HEADER = 1` を設定しておきます。

```sql
/*
* 2. キーワードファイルの読み込み準備
* キーワードが入っている 'keywords.csv' を読むためにファイルフォーマットを作成します。
*/

-- CSV用のファイルフォーマット
CREATE OR REPLACE FILE FORMAT FF_CSV
TYPE = CSV
SKIP_HEADER = 1; -- ヘッダーがある場合に備えて1行スキップ
```

### Step 3: キーワードのテーブル化（重要テクニック）

ここが今回のポイントです。
ステージ上にある `keywords.csv` を、直接クエリで使うのではなく、一度 **一時テーブル (Temporary Table)** にロードします。

**なぜテーブル化するの？**
ステージ上のファイルを直接サブクエリなどで何度も参照すると、毎回S3へのアクセスが発生し、パフォーマンスが悪くなったりエラーの原因になったりします。
検索条件となるマスタデータは、先にSnowflake内のテーブルに取り込んでおくのがベストプラクティスです。

```sql
/*
* 3. キーワードのテーブル化
* ステージ上のファイルを直接サブクエリで何度も呼ぶと失敗しやすいため、
* 一度小さな一時テーブルにキーワードをロードします。
*/

CREATE OR REPLACE TEMPORARY TABLE WEEK3_KEYWORDS AS
SELECT $1::STRING AS KEYWORD
FROM @WEEK3_STAGE/keywords.csv (FILE_FORMAT => 'FF_CSV');

-- 中身を確認
SELECT * FROM WEEK3_KEYWORDS;
```

実行すると、キーワードが格納されたことがわかります。

### Step 4: ファイル名でのフィルタリングと集計

準備が整いました。
「ステージ上の全ファイル」の中から、「キーワードテーブル」に含まれる単語をファイル名に持つものを抽出します。

ここでは **`EXISTS`** を使ってフィルタリングします。
（単純な `JOIN` だと、1つのファイル名に複数のキーワードが含まれていた場合に**行数が重複カウント（水増し）されてしまう**リスクがあるためです）


```sql
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
```

**解説:**
1.  `FROM @WEEK3_STAGE ...`: ステージ上のファイルを読み込みます。
2.  `PATTERN => '.*week3_data.*'`: `keywords.csv` 自身が集計対象にならないよう、ファイル名のパターンで絞り込みます。
3.  `WHERE EXISTS (...)`: サブクエリを使って、「ファイル名にキーワードが含まれるか？」を判定します。
4.  `GROUP BY D.METADATA$FILENAME`: ファイル単位でグループ化し、
5.  `COUNT(*)`: そのファイルの行数を数えます。

### Step 5: 結果の確認とお片付け

作成されたテーブルを確認してみましょう。

```sql
/*
* 5. 結果の確認
*/
SELECT * FROM WEEK3_RESULTS;
ORDER BY FILENAME;
```

**実行結果イメージ:**

| FILENAME | NUMBER_OF_ROWS |
| :--- | :--- |
| challenge_3/week3_data2_stacy_forgot_to_upload.csv | 11 |
| challenge_3/week3_data4_extra.csv | 12 |
| challenge_3/week3_data5_added.csv | 13 |

キーワードを含むファイルだけが抽出され、それぞれの行数が正しく集計されていれば成功です。
最後に、作成したオブジェクトを削除しておきましょう。

```sql
-- 任意：練習後に片付け
DROP TABLE IF EXISTS WEEK3_RESULTS;
DROP TABLE IF EXISTS WEEK3_KEYWORDS;
DROP STAGE IF EXISTS WEEK3_STAGE;
DROP FILE FORMAT IF EXISTS FF_CSV;
DROP SCHEMA IF EXISTS WEEK_003;
```

## 学びとポイント

今回の課題では、Snowflake の柔軟なデータ処理能力を体験できました。

1.  **メタデータの活用 (`METADATA$FILENAME`)**
    * データをロードするだけでなく、ファイル名そのものを条件にして処理対象を選別できました。
2.  **ステージ × テーブル の連携**
    * 「ステージ上のファイル」と「Snowflake上のテーブル」を組み合わせて、柔軟なフィルタリングが可能になります。
    * 今回のように `EXISTS` を使うことで、重複カウントのリスクを避けつつ安全に抽出できます。
3.  **マスタデータのテーブル化**
    * 外部にある参照データ（今回のようなキーワードリスト）は、都度アクセスするのではなく、一時テーブルに取り込んでから使うことで、クエリの安定性とパフォーマンスが向上します。

## 次回予告

次回は **Week 4** に挑戦予定です。
テーマは **JSONデータの解析** です。
Snowflake が得意とする半構造化データ（VARIANT型）の扱い方を学んでいくことになりそうです。

## 参考資料
* [Frosty Friday Week 3](https://frostyfriday.org/blog/2022/07/15/week-3-basic/)
* [Snowflake Docs: メタデータのクエリ](https://docs.snowflake.com/ja/user-guide/querying-metadata)
* [Snowflake Docs: CONTAINS 関数](https://docs.snowflake.com/ja/sql-reference/functions/contains)