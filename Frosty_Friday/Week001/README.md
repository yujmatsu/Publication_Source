# 【Snowflake】Frosty Friday Week 1 やってみた：S3にあるCSVファイルをテーブルにロードする
# Zenn:https://zenn.dev/yujmatsu/articles/20251122_frostyfriday_001

## はじめに

Snowflake の実践的なスキルを磨くためのコミュニティ課題、「Frosty Friday」。

前回の記事（[Snowflakeの実践力向上！「Frosty Friday」に挑戦するための準備と環境構築](https://zenn.dev/yujmatsu/articles/20251120_frostyfriday_000)）で環境は整いました。今回からいよいよ実践編です！

記念すべき第1回目、Week 1 のテーマは、Snowflake の基本中の基本である 「外部ステージ（S3）からのデータロード」 です。

「S3 にあるファイルを、どうやって Snowflake のテーブルに入れるの？」
という、実務でも最初に行う作業ですね。

今回は、単にロードするだけでなく、「コストを意識した」 アプローチで、実務でも通用する丁寧な手順で解いていきたいと思います。

---

## 今週の課題：Week 1 - Basic

課題の詳細は公式サイトで確認できます。
[Week 1 – Basic – External Stages](https://frostyfriday.org/blog/2022/07/14/week-1/)


### 課題の要約
1.  外部ステージ（S3バケット）を作成する。
    * S3 URI: `s3://frostyfridaychallenges/challenge_1/`
2.  そのステージにある CSV ファイルを読み込む。
3.  ロード先のテーブルを作成し、データを投入する。
4.  結果を確認する。

要するに、「指定されたS3にあるCSVファイルを、Snowflakeのテーブルに取り込んでね」 ということです。

---

## 実践：ハンズオン

それでは、Snowsight のワークシートを開いてやっていきましょう。
コスト意識を持ちつつ、一つずつ確認しながら進めます。

### Step 0: コンテキストの設定

まずは「どこで」「どのウェアハウスを使って」実行するかを明示します。
意図しないDBやWHを使ってしまう事故を防ぐため、ワークシートの冒頭には必ずこれを書く癖をつけます。

```sql
-- 各コンテキストの設定
USE ROLE SYSADMIN;
USE WAREHOUSE FF_WH; -- (準備編で作成したウェアハウスを指定。私は事前に作成済みの TEMP_WH を利用)
USE DATABASE FROSTY_FRIDAY;

-- Week 1 用のスキーマを作成して移動
-- (週数が増えてもソートしやすいよう 0埋めしています)
CREATE SCHEMA IF NOT EXISTS WEEK_001;
USE SCHEMA WEEK_001;
```

### Step 1: 外部ステージの作成

Snowflake から S3 にアクセスするための「窓口」となるステージを作成します。

通常、業務でプライベートな S3 バケットにアクセスする場合は、ストレージ統合 (Storage Integration) オブジェクトを作成して認証情報（IAMロール等）を設定する必要があります。

しかし、今回の課題で使用する S3 バケットは パブリック（公開設定） になっているため、認証設定は不要です。単純に `URL` を指定するだけでアクセスできます。

最後に、今回は課題専用の一時的なステージなので、`TEMPORARY` キーワードを使って作成しました。

```sql
/*
 * CREATE STAGE
 * 通常はストレージ統合(Storage Integration)が必要ですが、
 * 今回のバケットはパブリック公開されているためURL指定のみでOKです。
 */

-- 外部ステージ（S3バケット）の作成（TEMPORARYで作成）
CREATE OR REPLACE TEMPORARY STAGE STRANGE_STAGE
  URL = 's3://frostyfridaychallenges/challenge_1/';

-- 作成されたか確認
SHOW STAGES;
```

> **コスト/運用ポイント：TEMPORARY STAGE**
> ここで `TEMPORARY` を使っているのがポイントです。
> このステージはセッション終了後（ログアウトなど）に自動的に削除されます。「学習用や検証用に作ったオブジェクトが残り続けてゴミになる」のを防ぐ、クリーンなアプローチです。

### Step 2: ファイルの中身を「覗き見」する

いきなりロードする前に、どんなファイルがあるか、中身はどんな形式かを確認します。

```sql
/*
 * LISTコマンドでファイル一覧を確認
 */
-- `1.csv`, `2.csv`, `3.csv` というファイルが確認できればOK！
LIST @STRANGE_STAGE;
```

次に、ファイルの中身を少しだけ `SELECT` して確認します。
これを行わないと、「ヘッダーの有無」や「カラム数」がわからず、ロードエラーの原因になります。

```sql
/*
 * ファイルの内容をプレビュー（ロード前の確認）
 * 巨大ファイルの場合に備えて LIMIT を付け、FILE_FORMAT を明示するのが安全です。
 */
SELECT 
    $1, -- 1列目 (実際のデータは1列のみ)
    $2  -- 2列目 (存在しない列を指定すると NULL が返ります)
FROM @STRANGE_STAGE (FILE_FORMAT => (TYPE = 'CSV'))
LIMIT 10;
```

**確認できたこと:**
* ヘッダーは無さそう。
* 3ファイルとも文字型の1列のみのデータっぽい。

> **コスト意識ポイント：無駄なスキャン**
> `SELECT ... FROM @STAGE` は非常に便利ですが、裏側ではウェアハウスを使って外部ストレージのファイルを読み込んでいます。
> もしファイルサイズが TB（テラバイト）級 だった場合、`LIMIT` なしで `SELECT` すると大量のスキャンが発生し、ウェアハウスが長時間稼働してしまいます。
> 実務でファイルを覗くときは、必ず `LIMIT 10` などをつけてスキャン量を抑える意識が大切です。

### Step 3: テーブル作成と検証 (Validation)

データの形状がわかったので、テーブルを作成します。

```sql
-- テーブルの作成
CREATE OR REPLACE TABLE WEEK1_TABLE (
    result VARCHAR
);
```

いきなりロードしても良いのですが、実務では「エラーが出ないかテスト（Dry Run）」をするのが安全です。
`VALIDATION_MODE` オプションを使うと、データはロードせずにエラーチェックだけを行えます。

```sql
/*
 * 検証（ロードはしない）
 * エラーがあればその内容を、なければロード予定の行情報を返します
 */
COPY INTO WEEK1_TABLE
FROM @STRANGE_STAGE
  PATTERN = '.*\\.csv$'  -- .csv ファイルのみを対象にする
  FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 0)
  VALIDATION_MODE = 'RETURN_ERRORS';
```

実行結果が空（エラーなし）であれば、安心して本番ロードに進めます。

### Step 4: 本番ロード (COPY INTO)

問題ないので、実際にデータをロードします。
`PATTERN` を指定して、余計なファイルを取り込まないようにするのがコツです。

```sql
/*
 * 本番ロード
 */
COPY INTO WEEK1_TABLE
FROM @STRANGE_STAGE
  PATTERN = '.*\\.csv$'
  FILE_FORMAT = (
    TYPE = 'CSV',
    SKIP_HEADER = 0 -- プレビューでヘッダーが無いことを確認したので0
  );
```

### Step 5: 結果の確認とお片付け

最後にデータが入ったか確認して終了です！

```sql
-- 投入されたデータの確認
SELECT * FROM WEEK1_TABLE;
```

実行結果に11件のデータが表示されていれば成功です！

最後に、作成したオブジェクトを片付けておきましょう。
（TEMPORARY STAGE は勝手に消えますが、明示的に消す癖をつけるのも良いことです）

```sql
-- 任意：練習後に片付け
DROP STAGE IF EXISTS STRANGE_STAGE;
DROP TABLE IF EXISTS WEEK1_TABLE;
DROP SCHEMA FROSTY_FRIDAY.WEEK_001;

-- ウェアハウスを停止して課金を止める
ALTER WAREHOUSE FF_WH SUSPEND;
```

---

## 今回の「コスト意識」ポイント

前回の準備編で掲げた「コストを意識する」という観点で、今回のコードを振り返ってみます。

1.  **ウェアハウスのサイズ**
    * 今回のデータ量はCSV数行程度と非常に小さいです。`X-SMALL` サイズのウェアハウスで十分お釣りが来ます。ここで `LARGE` などを起動してしまうと、1分あたりの単価が高くなり、無駄なコストになります。
2.  **TEMPORARY オブジェクトの活用**
    * `CREATE OR REPLACE TEMPORARY STAGE` を使用しました。学習や検証が終われば消えるオブジェクトなので、ゴミを残さずに済みます。
3.  **事前のデータ確認とスキャン抑制**
    * いきなりロードせず、`LIMIT` 付きの `SELECT` でファイルの中身を確認したり、`VALIDATION_MODE` でテストすることで、手戻りや無駄なリトライ（＝無駄なクレジット消費）を防ぎました。
    * ステージ上のファイル読み込み (`SELECT FROM @STAGE`) もコンピュートリソースを使うため、ここでも `LIMIT` は重要です。


## 参考資料
* [Frosty Friday Week 1](https://frostyfriday.org/blog/2022/07/14/week-1/)
* [Snowflake Docs: CREATE STAGE](https://docs.snowflake.com/ja/sql-reference/sql/create-stage)
* [Snowflake Docs: COPY INTO <table>](https://docs.snowflake.com/ja/sql-reference/sql/copy-into-table)