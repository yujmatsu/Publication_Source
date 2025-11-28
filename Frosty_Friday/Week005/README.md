# 【Snowflake】Frosty Friday Week 5 やってみた：SQLで「自分だけの関数 (UDF)」を作る！
# Zenn:https://zenn.dev/yujmatsu/articles/20251130_frostyfriday_005

## はじめに

Snowflake の実践的なスキルを磨くためのコミュニティ課題、「Frosty Friday」。

今回の **Week 5** は、再び "Basic"（初級）に戻り、Snowflake の拡張性を支える重要機能 **「UDF (User Defined Functions: ユーザー定義関数)」** がテーマです。

「`SELECT sum(col) ...` のような標準関数は使うけど、自分で関数を作ったことはない」
「複雑な計算ロジックを毎回 SQL に書くのが面倒くさい...」

そんな悩みをお持ちの方に朗報です。Snowflake では、SQL（または Python, Java, Scala, JavaScript）を使って、**自分だけのオリジナル関数**を簡単に作ることができます。

今回は、最も基本的な「入力値を3倍にする関数」を作りながら、UDF の基礎をマスターしましょう。

## 今週の課題：Week 5 - Basic

課題の詳細は公式サイトで確認できます。
[Week 5 – Basic – User Defined Functions](https://frostyfriday.org/blog/2022/07/15/week-5-basic/)


### 課題の要約
1.  数値データ（`start_int`）を持つテーブルを作成する。
2.  **入力された数値を3倍にして返す UDF**（関数名: `timesthree`）を作成する。

非常にシンプルですね。
「関数を作る」という行為へのハードルを下げるのが今回の目的です。

## 知識：UDF (ユーザー定義関数) とは？

Snowflake には `SUM()` や `SUBSTR()` など多くの組み込み関数がありますが、業務特有のロジック（例：社内レートでの通貨換算、特殊な文字列クリーニング）まではカバーしていません。

そこで、**「入力 A を受け取って、ロジック B を適用し、結果 C を返す」** という処理を自分で定義して、`SELECT my_func(col)` のように呼び出せるようにするのが UDF です。

### UDF で使える言語
* **SQL** (一番かんたん・高速)
* **Python** (データ加工ライブラリが使えて便利)
* **JavaScript** (非構造化データの扱いに強い)
* **Java / Scala** (既存資産の流用など)

今回は基本の **SQL UDF** をメインに扱いますが、追加で**Python UDF** にも挑戦です。


## 実践：ハンズオン

それでは、Snowsight でやっていきましょう。

### Step 0: コンテキストの設定

```sql
USE ROLE SYSADMIN;
USE WAREHOUSE FF_WH;
USE DATABASE FROSTY_FRIDAY;

-- Week 5 用のスキーマを作成
CREATE SCHEMA IF NOT EXISTS WEEK_005;
USE SCHEMA WEEK_005;
```

### Step 1: データの準備

まずは計算の元となるデータを作成します。
ランダムな数値を持つテーブルを作ります。

```sql
-- 初期データテーブルの作成
CREATE OR REPLACE TABLE FF_WEEK_5_START (
    start_int INT
);

-- データの投入 (1, 2, 3, ... と適当な数値を入れる)
-- テストデータを挿入
INSERT INTO FF_WEEK_5_START (start_int) VALUES 
    (10),
    (20),
    (33),
    (100),
    (-5),
    (NULL);-- NULLの挙動も見ておきましょう

-- 確認
SELECT * FROM FF_WEEK_5_START;
```

### Step 2: SQL UDF の作成

ここが今回のメインイベントです。
「入力を3倍にする」関数 `timesthree` を作成します。

構文は非常に直感的です。
`CREATE FUNCTION <関数名> (<引数> <型>) RETURNS <戻り値の型> AS <処理>`

```sql
-- SQL UDF の作成
CREATE OR REPLACE FUNCTION timesthree_sql(input_int INT)
RETURNS INT
AS
$$
    input_int * 3
$$;
```

> **ポイント**
> * `$$...$$` で囲まれた部分が、関数の本体（ロジック）です。
> * SQL UDF の場合、この中身は単一の SQL 式である必要があります。

### Step 3: 関数のテスト

作成した関数を `SELECT` 文で使ってみましょう。

```sql
-- 関数を使ってみる
SELECT 
    start_int, 
    timesthree_sql(start_int) AS SQL_RESULT
FROM FF_WEEK_5_START;
```

**実行結果:**
| START_INT | SQL_RESULT |
| :--- | :--- |
| 10 | 30 |
| 20 | 60 |
| 33 | 99 |
| 100 | 300 |
| -5 | -15 |
| NULL | NULL |

見事に3倍されています。
`NULL` を入力した場合は、自動的に `NULL` が返ってきていますね。
これは SQL の標準的な挙動で、`NULL * 3` のように **NULL を含む算術演算の結果は NULL になるため** です。

これで課題クリアです。


## 【発展】Python UDF でやってみる

せっかくなので、最近のトレンドである **Python** を使った UDF も作ってみましょう。
SQL では書きにくい複雑なロジック（例：if文分岐やライブラリ利用）も、Python なら簡単に書けます。

### Python UDF の定義
`LANGUAGE PYTHON` を指定し、Python のコードを記述します。

```sql
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
```

> **注意: Python ランタイム**
> `RUNTIME_VERSION` で指定した Python のバージョンが、お使いの Snowflake アカウントでサポートされている必要があります（Snowflake がサポートしている最新版の Python を指定してください。基本的には新しい安定版である 3.10 や 3.11 以降が推奨されます）。

### 実行確認
SQL UDF と全く同じように使えます。

```sql
SELECT 
    start_int,
    timesthree_python(start_int) AS PYTHON_RESULT  -- Python UDFの結果
FROM FF_WEEK_5_START;
```

今回は単純な掛け算なのでメリットが薄いですが、「正規表現で複雑な文字列操作をする」や「外部ライブラリを使って計算する」といった場面では Python UDF が最強の武器になります。

## 今回の「コスト意識」ポイント

UDF を使う際のコストとパフォーマンスの勘所です。

1.  **SQL UDF vs その他の言語**
    * 基本的な計算（四則演算や文字列結合など）であれば、**SQL UDF が最も高速で低コスト**です。Snowflake のオプティマイザが最適化しやすいからです。
    * Python や Java UDF は、呼び出しのオーバーヘッドが少し発生します。SQL で書けるものは SQL で書きましょう。今回のケースでも簡単な処理のためか処理時間にそこまで大きな差はなかった。
2.  **スカラー関数 (Scalar Function) の特性**
    * 今回作ったのは「1行入力して1行返す」スカラー関数です。
    * 数億行のデータに対して Python UDF を実行すると、処理時間が長くなる可能性があります。その場合は「ベクトル化 UDF (Vectorized UDF)」という、データをバッチ（塊）で処理する高速化手法を検討します。
3.  **ロジックの共通化による保守コスト削減**
    * 「消費税計算」のような共通ロジックを UDF 化しておけば、税率が変わった時に UDF だけを直せば済みます。クエリを毎回書き直す人的コストとミスを削減できる点も、広い意味でのコスト削減です。


## 次回予告

次回は **Week 6** に挑戦します。
テーマは **「GeoSpatial (地理空間情報)」** です。
自分としてはあまり知識のない未知の領域なのでいろいろと調べながらの学習となりそうです。

## 参考資料
* [Frosty Friday Week 5](https://frostyfriday.org/blog/2022/07/15/week-5-basic/)
* [Snowflake Docs: ユーザー定義関数 (UDF) の概要](https://docs.snowflake.com/ja/developer-guide/udf/udf-overview)
* [Snowflake Docs: SQL UDF の導入](https://docs.snowflake.com/ja/developer-guide/udf/sql/udf-sql-introduction)
* [Snowflake Docs: Python UDF の導入](https://docs.snowflake.com/ja/developer-guide/udf/python/udf-python-introduction)