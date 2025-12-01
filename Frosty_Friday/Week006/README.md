# 【Snowflake】Frosty Friday Week 6 やってみた：点をつないでポリゴンを作る！GeoSpatial分析入門
# Zenn:https://zenn.dev/yujmatsu/articles/20251202_frostyfriday_006

## はじめに

Snowflake の実践的なスキルを磨くためのコミュニティ課題、「Frosty Friday」。

今回の **Week 6** は、難易度が **"Hard"（上級）** に設定されています。
テーマは **「GeoSpatial (地理空間情報) 分析」** です。

「位置情報の分析なんて、専用のGISソフトがないと無理でしょ？」
「緯度経度のデータはあるけど、どうやって『領域』として扱えばいいの？」

そんな疑問をお持ちの方も多いと思います。
実は Snowflake は地理空間データのサポートが非常に充実しており、標準の SQL だけで高度な空間分析が可能です。



今回は、**「大量の点（ポイント）データをつなぎ合わせて、エリア（ポリゴン）を構築し、重なりを判定する」** という、GISエンジニア顔負けの処理に挑戦します。

特に、**「バラバラの座標データをどうやってひとつの地図データにするのか？」** という手順を、初心者の方にもわかるように丁寧に分解して解説します。

## 今週の課題：Week 6 - Hard

課題の詳細は公式サイトで確認できます。
[Week 6 – Hard – Geospatial](https://frostyfriday.org/blog/2022/07/22/week-6-hard/)

### 課題のストーリー
選挙ロビー活動団体「Frosty Lobbying」は、次の英国総選挙に向けて候補者の地理的な分布を把握したいと考えています。
手元にあるのは、「国・地域（Regions）」と「選挙区（Constituencies）」を構成する**境界線の座標点データ**だけです。

### やること
1.  S3にある2つのCSVファイル（地域データ、選挙区データ）をロードする。
2.  バラバラの「点（Point）」データを繋ぎ合わせて、「面（Polygon）」を作成する。
    * ※1つの地域が複数の「パーツ（Part）」（例：島と本土）に分かれている場合がある点に注意。
3.  作成した「地域のポリゴン」と「選挙区のポリゴン」を重ね合わせ（交差判定）、**各地域に含まれる選挙区の数**を集計する。

要するに、**「点から地図（ポリゴン）を作り、どの選挙区がどの地域にあるかを数えてね」** という課題です。


## 知識：点をつないで面にする仕組み

今回の最大の難所は、**「点 (Point) → 線 (LineString) → 面 (Polygon)」** という変換プロセスです。

Snowflake は **WKT (Well-Known Text)** という「文字で図形を表すフォーマット」を解釈できます。
これを利用して、以下のようにデータを加工していきます。



1.  **点 (Point):** `経度 緯度` (例: `139.76 35.68`)
2.  **線 (LineString):** 点をカンマでつないだもの。
    * `LINESTRING(経度1 緯度1, 経度2 緯度2, 経度3 緯度3, ...)`
3.  **面 (Polygon):** 線の始点と終点が一致して閉じたもの。

つまり、SQLで行うべき操作は、**「バラバラの行にある経度・緯度を、順番通りに一つの長い文字列として結合する」** ことです。


## 実践：ハンズオン

それでは、Snowsight でやっていきましょう。

### Step 0: コンテキストの設定

```sql
-- コンテキストの設定
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;

-- Week 6 用のスキーマを作成して移動
CREATE SCHEMA IF NOT EXISTS WEEK_006;
USE SCHEMA WEEK_006;
```

### Step 1: ステージとファイルフォーマットの作成

S3 バケットへのステージと、CSV用のフォーマットを作成します。

```sql
/*
* 1. ステージとファイルフォーマットの作成
*/

-- S3バケットを参照するステージの作成
CREATE OR REPLACE TEMPORARY STAGE WEEK6_STAGE
    URL = 's3://frostyfridaychallenges/challenge_6/';

-- ファイルフォーマットの作成 (CSV, ヘッダーあり)
CREATE OR REPLACE FILE FORMAT FF_CSV_HEADER
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' -- カンマを含むデータに対応
    SKIP_HEADER = 1;
```

### Step 2: テーブル作成とデータロード

ここでのポイントは、**2つのCSVファイルで列構成が微妙に異なる**ことです。
それぞれのファイルに合わせてテーブルを定義します。

```sql
/*
* 2. テーブル作成とデータロード
* ポイント: CSVファイルによって列構成が異なるため、それぞれに合わせて定義します。
*/

-- 1) 国・地域データ用テーブル (TYPE列あり)
CREATE OR REPLACE TABLE NATIONS_REGIONS_POINTS (
    NATION_OR_REGION_NAME VARCHAR,
    TYPE VARCHAR,
    SEQUENCE_NUM INT, -- 点をつなぐ順番
    LONGITUDE FLOAT,  -- 経度
    LATITUDE FLOAT,   -- 緯度
    PART INT          -- パーツID (飛び地や島など)
);

-- 2) 選挙区データ用テーブル (TYPE列なし)
CREATE OR REPLACE TABLE WESTMINSTER_CONSTITUENCY_POINTS (
    CONSTITUENCY_NAME VARCHAR,
    SEQUENCE_NUM INT,
    LONGITUDE FLOAT,
    LATITUDE FLOAT,
    PART INT
);

-- データのロード
COPY INTO NATIONS_REGIONS_POINTS
FROM @WEEK6_STAGE/nations_and_regions.csv
FILE_FORMAT = (FORMAT_NAME = 'FF_CSV_HEADER');

COPY INTO WESTMINSTER_CONSTITUENCY_POINTS
FROM @WEEK6_STAGE/westminster_constituency_points.csv
FILE_FORMAT = (FORMAT_NAME = 'FF_CSV_HEADER');
```

### Step 3: 点をつないでポリゴンを作る (詳細解説)

ここが今回のハイライトです。
以下の4段階のステップを **1つのSQL** で実現します。

1.  **座標の文字列化:** 数値の `経度`, `緯度` を、`'経度 緯度'` という文字列に変換する。
2.  **集約 (LISTAGG):** グループ（地域や島）ごとに、点をつなぎ合わせて長い文字列にする。
    * ※この時、`SEQUENCE_NUM` 順に並べないと形が崩れてしまいます。
3.  **WKTの完成:** 先頭に `'LINESTRING('` 、末尾に `')'` をつける。
4.  **ポリゴン化:** Snowflake の関数 `TO_GEOGRAPHY` と `ST_MAKEPOLYGON` で地図データに変換する。

#### ロジックのイメージ
例えば、「おにぎり島」という島が3つの点でできているとします。

| NAME | PART | SEQ | LON | LAT |
| :--- | :--- | :--- | :--- | :--- |
| おにぎり | 1 | 1 | 10 | 10 |
| おにぎり | 1 | 2 | 20 | 10 |
| おにぎり | 1 | 3 | 15 | 20 |
| おにぎり | 1 | 4 | 10 | 10 | (始点に戻る) |

これを `LISTAGG` で繋ぐと...
`'10 10, 20 10, 15 20, 10 10'` という文字列になります。

これに `LINESTRING()` を被せると...
`'LINESTRING(10 10, 20 10, 15 20, 10 10)'` という **WKT形式** になります。

これを `ST_MAKEPOLYGON` に渡せば完成です。

```sql
/*
* 3. ポリゴンの構築
* 地理空間関数を使用して、点(Points)をポリゴン(Polygons)に変換します。
* LISTAGGを使用してWKT形式の文字列(LINESTRING)を作成することで、型変換エラーを回避します。
*/

-- 国・地域のポリゴン化
CREATE OR REPLACE TABLE NATIONS_REGIONS_POLYGONS AS
SELECT 
    NATION_OR_REGION_NAME,
    PART,
    ST_MAKEPOLYGON(                 -- 3. 線(LineString)から面(Polygon)を作る
        TO_GEOGRAPHY(               -- 2. 文字列を地理情報型(Geography)に変換する
            'LINESTRING(' || 
            -- 1. 点をカンマ区切りでつなぐ (順序指定が重要！)
            LISTAGG(LONGITUDE::VARCHAR || ' ' || LATITUDE::VARCHAR, ',') 
                WITHIN GROUP (ORDER BY SEQUENCE_NUM) || 
            ')'
        )
    ) AS GEO_POLYGON
FROM NATIONS_REGIONS_POINTS
GROUP BY NATION_OR_REGION_NAME, PART; -- 地域名とパーツ(島など)ごとに集約

-- 選挙区のポリゴン化 (ロジックは同じ)
CREATE OR REPLACE TABLE CONSTITUENCY_POLYGONS AS
SELECT 
    CONSTITUENCY_NAME,
    PART,
    ST_MAKEPOLYGON(
        TO_GEOGRAPHY(
            'LINESTRING(' || 
            LISTAGG(LONGITUDE::VARCHAR || ' ' || LATITUDE::VARCHAR, ',') 
                WITHIN GROUP (ORDER BY SEQUENCE_NUM) || 
            ')'
        )
    ) AS GEO_POLYGON
FROM WESTMINSTER_CONSTITUENCY_POINTS
GROUP BY CONSTITUENCY_NAME, PART;
```

> **TIPS: ポリゴンの「閉合」について**
> `ST_MAKEPOLYGON` で作成するポリゴンは、本来「始点と終点が同じ座標（閉じたループ）」である必要があります。
> 今回のデータはその要件を満たしているためそのまま変換できますが、一般的なデータでは「始点を最後にもう一度追加する」等の前処理が必要になることがあります。

### Step 4: 空間結合 (Spatial Join) と集計

最後に、2つのポリゴンテーブルを結合します。
通常の `JOIN` は `ON A.id = B.id` のように値を比較しますが、今回は **「領域が重なっているか」** を条件にします。これが **空間結合 (Spatial Join)** です。



```sql
/*
* 4. 空間結合と集計
* ST_INTERSECTS を使用して、地域と選挙区の重なりを判定します。
*/

SELECT 
    R.NATION_OR_REGION_NAME,
    -- 同じ選挙区が複数のPart（島など）で重複してカウントされないようDISTINCT
    COUNT(DISTINCT C.CONSTITUENCY_NAME) AS INTERSECTING_CONSTITUENCIES
FROM NATIONS_REGIONS_POLYGONS R
JOIN CONSTITUENCY_POLYGONS C
    -- 空間結合: Region と Constituency が重なるか判定
    ON ST_INTERSECTS(R.GEO_POLYGON, C.GEO_POLYGON)
GROUP BY R.NATION_OR_REGION_NAME
ORDER BY INTERSECTING_CONSTITUENCIES DESC;
```

> **TIPS: ST_INTERSECTS の威力**
> `ST_INTERSECTS(GeoA, GeoB)` 関数は、2つの図形が少しでも重なっていれば `TRUE` を返します。
> Snowflake のジオスペーシャル機能は最適化されており、このような複雑な幾何計算も高速に処理できます。

### 結果確認

実行結果を確認してみましょう。

**実行結果イメージ:**
| NATION_OR_REGION_NAME | INTERSECTING_CONSTITUENCIES |
| :--- | :--- |
| South East | 116 |
| North West | 95 |
| London | 91 |
| East of England | 82 |
| ... | ... |

地域ごとの選挙区数が集計されました。
（※数値は公式解答のものです。環境により多少前後する場合があります）


## 学びとポイント

"Hard" とされるだけあって、少し手応えのある課題でした。

1.  **WKT (Well-Known Text) の構築**
    * 地理空間データを作る際、元データがバラバラの座標値の場合は、まずテキスト処理 (`LISTAGG`) で WKT 形式（`LINESTRING(...)`）に整形し、それをパースさせるのが鉄板パターンのようです。
2.  **ST_MAKEPOLYGON**
    * 線（LineString）は、始点と終点が一致していれば、面（Polygon）として閉じることができます。
3.  **Spatial Join (空間結合)**
    * `JOIN ... ON ST_INTERSECTS(...)` という書き方は、GIS分析の基本にして奥義です。「場所」をキーにしてデータを繋げられるようになると、分析の幅が劇的に広がります。


## 次回予告

次回は **Week 7** に挑戦します。
テーマは **「Snowflake のタグ付けとアクセス制御」** のようです。
セキュリティやガバナンスに関わる、管理者必見の内容になりそうです。


## 参考資料
* [Frosty Friday Week 6](https://frostyfriday.org/blog/2022/07/22/week-6-hard/)
* [Snowflake Docs: 地理空間データ型 (Geography)](https://docs.snowflake.com/ja/sql-reference/data-types-geospatial)
* [Snowflake Docs: ST_MAKEPOLYGON](https://docs.snowflake.com/ja/sql-reference/functions/st_makepolygon)
* [Snowflake Docs: ST_INTERSECTS](https://docs.snowflake.com/ja/sql-reference/functions/st_intersects)