----
-- Week009 回答例 (完全修正版)
----
-- 問題(英文)
-- STORY
-- It’s not just bad guys that need to guard their secrets!
-- Superheroes are our first line of defence against those evil-doers so we really need to protect their information.
-- ... (中略) ...
-- CHALLENGE
-- With the use of Tags and Masking , we want to mask the first_name and last_name columns from our data_to_be_masked table.
-- We want the following :
-- The default user that has access can only see the hero_name data unmasked
-- Role foo1 can only see hero_name and first_name
-- Role foo2 can see the contents of the whole table
-- The used masking policy should NOT use a role checking feature. (current_role = … etc.)
--
-- 問題(和訳)
-- ストーリー
-- 秘密を守る必要があるのは悪人だけではありません！
-- スーパーヒーローの身元を守るため、組織内の役割に応じて情報アクセスを制御する必要があります。
-- チャレンジ
-- タグ（Tag）とマスキング（Masking）を使用して、data_to_be_maskedテーブルのfirst_name列とlast_name列をマスクします。
-- 要件：
-- ・デフォルトユーザー: hero_name のみ閲覧可（他はマスク）
-- ・ロール foo1: hero_name と first_name を閲覧可
-- ・ロール foo2: 全てのデータを閲覧可
-- ・条件: マスキングポリシー内にロール名を直接記述（ハードコーディング）してはいけません。
--
-- ■やること
-- 1. コンテキスト設定と、実行ユーザーの取得（変数化）。
-- 2. 提供されたコードでデータとロールを作成する。
-- 3. 権限管理用のマッピングテーブルを作成する。
-- 4. タグとマスキングポリシーを作成する。
-- 5. 【重要】ACCOUNTADMIN権限でタグにポリシーを適用する。
-- 6. 列にタグを付与し、各ロールでの見え方を確認する。

/*
* ------------------------------------------------------------------------------
* STEP 0: コンテキストと変数の準備
* ------------------------------------------------------------------------------
*/
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;

CREATE SCHEMA IF NOT EXISTS WEEK_009;
USE SCHEMA WEEK_009;

-- 権限付与のために、現在のユーザー名を変数に格納します（$USERエラー回避策）
SET my_current_user = CURRENT_USER();


/*
* ------------------------------------------------------------------------------
* STEP 1: ロールとデータの作成 (Start Up Code)
* ------------------------------------------------------------------------------
*/

-- 1-1. データの作成
CREATE OR REPLACE TABLE data_to_be_masked(first_name varchar, last_name varchar,hero_name varchar);
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Eveleen', 'Danzelman','The Quiet Antman');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Harlie', 'Filipowicz','The Yellow Vulture');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Mozes', 'McWhin','The Broken Shaman');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Horatio', 'Hamshere','The Quiet Charmer');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Julianna', 'Pellington','Professor Ancient Spectacle');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Grenville', 'Southouse','Fire Wonder');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Analise', 'Beards','Purple Fighter');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Darnell', 'Bims','Mister Majestic Mothman');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Micky', 'Shillan','Switcher');
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES ('Ware', 'Ledstone','Optimo');

-- 1-2. ロールの作成とユーザーへの紐付け
-- SECURITYADMIN に切り替えて実行
USE ROLE SECURITYADMIN;
CREATE OR REPLACE ROLE foo1;
CREATE OR REPLACE ROLE foo2;

-- 変数を使って現在のユーザーにロールを付与
GRANT ROLE foo1 TO USER IDENTIFIER($my_current_user);
GRANT ROLE foo2 TO USER IDENTIFIER($my_current_user);

-- 1-3. ロールへのアクセス権限付与
-- SYSADMIN に戻ってオブジェクト権限を付与
USE ROLE SYSADMIN;

GRANT USAGE ON DATABASE FROSTY_FRIDAY TO ROLE foo1;
GRANT USAGE ON SCHEMA WEEK_009 TO ROLE foo1;
GRANT USAGE ON WAREHOUSE TEMP_WH TO ROLE foo1;
GRANT SELECT ON TABLE data_to_be_masked TO ROLE foo1;

GRANT USAGE ON DATABASE FROSTY_FRIDAY TO ROLE foo2;
GRANT USAGE ON SCHEMA WEEK_009 TO ROLE foo2;
GRANT USAGE ON WAREHOUSE TEMP_WH TO ROLE foo2;
GRANT SELECT ON TABLE data_to_be_masked TO ROLE foo2;


/*
* ------------------------------------------------------------------------------
* STEP 2: マッピングテーブル（権限管理テーブル）の作成
* ------------------------------------------------------------------------------
*/

-- 権限マトリクス: どのロールがどの機密レベル(Tag Value)を見れるか
CREATE OR REPLACE TABLE auth_matrix (
    role_name VARCHAR,
    allowed_tag_value VARCHAR
);

-- ルール定義
-- foo1 は 'LEVEL_1' (First Name) だけ見れる
-- foo2 は 'LEVEL_1' と 'LEVEL_2' (Last Name) の両方が見れる
INSERT INTO auth_matrix (role_name, allowed_tag_value) VALUES 
    ('FOO1', 'LEVEL_1'),
    ('FOO2', 'LEVEL_1'),
    ('FOO2', 'LEVEL_2');


/*
* ------------------------------------------------------------------------------
* STEP 3: タグとマスキングポリシーの作成
* ------------------------------------------------------------------------------
*/

-- タグの作成
CREATE OR REPLACE TAG sensitive_data_tag COMMENT = 'Controls visibility of sensitive data';

-- マスキングポリシーの作成
-- 引数(val)に対して、現在のロールとカラムのタグ値を条件にマスクするか判定
CREATE OR REPLACE MASKING POLICY dynamic_tag_policy AS (val string) RETURNS string ->
    CASE
        -- 現在のロール(CURRENT_ROLE)が、
        -- 現在のカラムについているタグの値(SYSTEM$GET_TAG_ON_CURRENT_COLUMN)に対して
        -- アクセス権を持っているかをマッピングテーブルでチェック
        WHEN EXISTS (
            SELECT 1 
            FROM auth_matrix 
            WHERE role_name = CURRENT_ROLE()
              AND allowed_tag_value = SYSTEM$GET_TAG_ON_CURRENT_COLUMN('sensitive_data_tag')
        ) THEN val
        ELSE '*********' -- 権限がない場合はマスク
    END;


/*
* ------------------------------------------------------------------------------
* STEP 4: タグへのポリシー適用 (要ACCOUNTADMIN権限)
* ------------------------------------------------------------------------------
*/

-- タグベースのポリシー設定は強力な権限が必要なため、ACCOUNTADMINで行います
USE ROLE ACCOUNTADMIN;

-- タグにマスキングポリシーを紐付け
ALTER TAG FROSTY_FRIDAY.WEEK_009.sensitive_data_tag 
SET MASKING POLICY FROSTY_FRIDAY.WEEK_009.dynamic_tag_policy;

-- 作業が終わったらSYSADMINに戻ります
USE ROLE SYSADMIN;


/*
* ------------------------------------------------------------------------------
* STEP 5: テーブルの列にタグを適用
* ------------------------------------------------------------------------------
*/

-- First Name には 'LEVEL_1' を設定 (foo1, foo2が見れる)
ALTER TABLE data_to_be_masked 
MODIFY COLUMN first_name 
SET TAG sensitive_data_tag = 'LEVEL_1';

-- Last Name には 'LEVEL_2' を設定 (foo2だけが見れる)
ALTER TABLE data_to_be_masked 
MODIFY COLUMN last_name 
SET TAG sensitive_data_tag = 'LEVEL_2';


/*
* ------------------------------------------------------------------------------
* STEP 6: 結果の確認
* ------------------------------------------------------------------------------
*/

-- 1. SYSADMIN (デフォルトユーザー): マッピングテーブルに定義がないため両方マスク
USE ROLE SYSADMIN;
SELECT * FROM data_to_be_masked LIMIT 5;
-- 結果: first_name=***, last_name=***, hero_name=見える

-- 2. FOO1: LEVEL_1 (First Name) だけ見える
USE ROLE foo1;
SELECT * FROM data_to_be_masked LIMIT 5;
-- 結果: first_name=見える, last_name=***, hero_name=見える

-- 3. FOO2: LEVEL_1, LEVEL_2 両方見える
USE ROLE foo2;
SELECT * FROM data_to_be_masked LIMIT 5;
-- 結果: first_name=見える, last_name=見える, hero_name=見える


/*
* 後片付け (必要に応じて実行)
*/
/*
USE ROLE ACCOUNTADMIN;
-- ポリシーの紐付けを解除しないとタグを削除できません
ALTER TAG FROSTY_FRIDAY.WEEK_009.sensitive_data_tag UNSET MASKING POLICY;

USE ROLE SECURITYADMIN;
DROP ROLE IF EXISTS foo1;
DROP ROLE IF EXISTS foo2;

USE ROLE SYSADMIN;
DROP TAG IF EXISTS sensitive_data_tag;
DROP MASKING POLICY IF EXISTS dynamic_tag_policy;
DROP TABLE IF EXISTS auth_matrix;
DROP TABLE IF EXISTS data_to_be_masked;
DROP SCHEMA IF EXISTS WEEK_009;
*/