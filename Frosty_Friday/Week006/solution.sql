----
-- Week006 回答例
----
-- 問題(英文)
-- This week we’re going to play with spatial functions. Frosty Lobbying is thinking of supporting some candidates in the next UK General Election. What they need is to understand the geographic spread of candidates by nation/region of the UK.
-- Your job is to build both the nations/regions and parliamentary seats into polygons, and then work out how many Westminster seats intersect with region polygons. 
-- Be wary that some seats may sit within two different regions, some may not sit within any (Northern Ireland is not included in the data provided) and some may just be awkward.
-- Note: Within the data, the ‘part’ column is an integer given to each landmass that makes up that region/nation/constituency – for example, the Isle of Mull could be ‘part 34’ of Scotland, and ‘part 12’ of the Argyll and Bute constituency.
-- You can find the nations are regions data here and the Westminster constituency data here.
-- Source: ONS, Open Geography Portal
--
-- 問題(和訳)
-- 今週は空間関数を使って遊びます。Frosty Lobbyingは、次の英国総選挙でいくつかの候補者を支援しようと考えています。彼らが必要としているのは、英国の国/地域ごとの候補者の地理的な広がりを理解することです。
-- あなたの仕事は、国/地域と議席（選挙区）の両方をポリゴン（多角形）に構築し、いくつのウェストミンスター議席が地域のポリゴンと交差するかを計算することです。
-- いくつかの議席は2つの異なる地域にまたがっている可能性があり、いくつかの議席はどの地域にも属していない可能性があり（北アイルランドは提供されたデータに含まれていません）、いくつかは単に厄介な位置にある可能性があることに注意してください。
-- 注：データ内の「part」列は、その地域/国/選挙区を構成する各陸塊に与えられた整数です。たとえば、マル島はスコットランドの「part 34」であり、アーガイル・ビュート選挙区の「part 12」である可能性があります。
--
-- ■やること
-- 1. S3バケットを参照するステージを作成する。
-- 2. 点データをロードする。
-- 3. 点データをつないでポリゴンを作成する（ST_MAKELINE -> ST_MAKEPOLYGON）。
-- 4. 地域（Region）と選挙区（Constituency）のポリゴンを空間結合（INTERSECTS）し、集計する。

/*
* 準備
*/

-- コンテキストの設定
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;

-- Week 6 用のスキーマを作成して移動
CREATE SCHEMA IF NOT EXISTS WEEK_006;
USE SCHEMA WEEK_006;


/*
* 1. ステージとファイルフォーマットの作成
*/

-- S3バケットを参照するステージの作成
CREATE OR REPLACE TEMPORARY STAGE WEEK6_STAGE
    URL = 's3://frostyfridaychallenges/challenge_6/';

-- ファイルフォーマットの作成 (CSV, ヘッダーあり)
CREATE OR REPLACE FILE FORMAT FF_CSV_HEADER
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;


/*
* 2. テーブル作成とデータロード
*/

-- 1) 国・地域データ用テーブル
CREATE OR REPLACE TABLE NATIONS_REGIONS_POINTS (
    NATION_OR_REGION_NAME VARCHAR,
    TYPE VARCHAR,
    SEQUENCE_NUM INT,
    LONGITUDE FLOAT,
    LATITUDE FLOAT,
    PART INT
);

-- 2) 選挙区データ用テーブル
CREATE OR REPLACE TABLE WESTMINSTER_CONSTITUENCY_POINTS (
    CONSTITUENCY_NAME VARCHAR,
    SEQUENCE_NUM INT,
    LONGITUDE FLOAT,
    LATITUDE FLOAT,
    PART INT
);

-- データのロード
-- ステージ上のファイルを指定してロードします
COPY INTO NATIONS_REGIONS_POINTS
FROM @WEEK6_STAGE/nations_and_regions.csv
FILE_FORMAT = (FORMAT_NAME = 'FF_CSV_HEADER');

COPY INTO WESTMINSTER_CONSTITUENCY_POINTS
FROM @WEEK6_STAGE/westminster_constituency_points.csv
FILE_FORMAT = (FORMAT_NAME = 'FF_CSV_HEADER');


/*
* 3. ポリゴンの構築
* 地理空間関数を使用して、点(Points)をポリゴン(Polygons)に変換します。
*/

-- 国・地域のポリゴン化
CREATE OR REPLACE TABLE NATIONS_REGIONS_POLYGONS AS
SELECT 
    NATION_OR_REGION_NAME,
    PART,
    ST_MAKEPOLYGON(
        TO_GEOGRAPHY(
            'LINESTRING(' || 
            LISTAGG(LONGITUDE::VARCHAR || ' ' || LATITUDE::VARCHAR, ',') 
                WITHIN GROUP (ORDER BY SEQUENCE_NUM) || 
            ')'
        )
    ) AS GEO_POLYGON
FROM NATIONS_REGIONS_POINTS
GROUP BY NATION_OR_REGION_NAME, PART;

-- 選挙区のポリゴン化
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


/*
* 結果の確認
*/
-- これで South East や London などの地域ごとに、重なっている選挙区の数が表示されます。


-- 任意：後片付け
-- DROP TABLE IF EXISTS NATIONS_REGIONS_POLYGONS;
-- DROP TABLE IF EXISTS CONSTITUENCY_POLYGONS;
-- DROP TABLE IF EXISTS NATIONS_REGIONS_POINTS;
-- DROP TABLE IF EXISTS WESTMINSTER_CONSTITUENCY_POINTS;
-- DROP STAGE IF EXISTS WEEK6_STAGE;
-- DROP FILE FORMAT IF EXISTS FF_CSV_HEADER;
-- DROP SCHEMA IF EXISTS WEEK_006;