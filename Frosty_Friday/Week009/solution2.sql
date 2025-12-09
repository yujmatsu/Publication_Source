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
-- 3. タグとマスキングポリシーの作成する
-- 4. ロールにタグ情報を付与する。
-- 5. テーブルにタグ情報の付与する。
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
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE TEMP_WH TO ROLE foo1;
USE ROLE SYSADMIN;
GRANT SELECT ON TABLE data_to_be_masked TO ROLE foo1;

GRANT USAGE ON DATABASE FROSTY_FRIDAY TO ROLE foo2;
GRANT USAGE ON SCHEMA WEEK_009 TO ROLE foo2;
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE TEMP_WH TO ROLE foo2;
USE ROLE SYSADMIN;
GRANT SELECT ON TABLE data_to_be_masked TO ROLE foo2;


/*
* ------------------------------------------------------------------------------
* STEP 2: タグとマスキングポリシーの作成
* ------------------------------------------------------------------------------
*/
USE ROLE ACCOUNTADMIN;
USE SCHEMA FROSTY_FRIDAY.WEEK_009;

-- (A) Column side: sensitivity tag
CREATE OR REPLACE TAG sensitive_level_tag
  ALLOWED_VALUES 'LEVEL_1', 'LEVEL_2'
  COMMENT = 'Column sensitivity (LEVEL_1/LEVEL_2)';

-- (B) Role side: clearance tag
CREATE OR REPLACE TAG role_clearance_tag
  ALLOWED_VALUES 'LEVEL_0', 'LEVEL_1', 'LEVEL_2'
  COMMENT = 'Role clearance (LEVEL_0/LEVEL_1/LEVEL_2)';

-- (C) Masking policy:
--  - 列のタグ値: SYSTEM$GET_TAG_ON_CURRENT_COLUMN()
--  - ロールのタグ値: SYSTEM$GET_TAG(..., <role>, 'ROLE')
--  - “ロール名の比較” はしない（タグ値のレベル比較のみ）
CREATE OR REPLACE MASKING POLICY mask_by_tag_level AS (val STRING) RETURNS STRING ->
  CASE
    WHEN val IS NULL THEN NULL
    WHEN
      /* role clearance */
      COALESCE(
        CASE SYSTEM$GET_TAG('FROSTY_FRIDAY.WEEK_009.role_clearance_tag', CURRENT_ROLE(), 'ROLE')
          WHEN 'LEVEL_2' THEN 2
          WHEN 'LEVEL_1' THEN 1
          WHEN 'LEVEL_0' THEN 0
          ELSE 0
        END
      ,0)
      >=
      /* column sensitivity */
      COALESCE(
        CASE SYSTEM$GET_TAG_ON_CURRENT_COLUMN('FROSTY_FRIDAY.WEEK_009.sensitive_level_tag')
          WHEN 'LEVEL_2' THEN 2
          WHEN 'LEVEL_1' THEN 1
          ELSE 999
        END
      ,999)
    THEN val
    ELSE '*********'
  END;

-- (D) Bind policy to the tag  (= tag-based masking)
ALTER TAG sensitive_level_tag SET MASKING POLICY mask_by_tag_level;

-- (E) Allow admins to APPLY tags (最小権限の観点ではここが超重要)
GRANT APPLY ON TAG sensitive_level_tag TO ROLE SYSADMIN;       -- 列にタグ付けする側
GRANT APPLY ON TAG role_clearance_tag TO ROLE SECURITYADMIN;   -- ロールにタグ付けする側

/*
* ------------------------------------------------------------------------------
* STEP 3: ロールにタグ情報を付与
* ------------------------------------------------------------------------------
*/
USE ROLE ACCOUNTADMIN;

ALTER ROLE foo1 SET TAG FROSTY_FRIDAY.WEEK_009.role_clearance_tag = 'LEVEL_1';
ALTER ROLE foo2 SET TAG FROSTY_FRIDAY.WEEK_009.role_clearance_tag = 'LEVEL_2';
-- SYSADMIN は未設定 (= LEVEL_0扱いでマスクされる)


/*
* ------------------------------------------------------------------------------
* STEP 4: テーブルにタグ情報の付与
* ------------------------------------------------------------------------------
*/
USE ROLE ACCOUNTADMIN;

ALTER TABLE data_to_be_masked
  MODIFY COLUMN first_name
  SET TAG FROSTY_FRIDAY.WEEK_009.sensitive_level_tag = 'LEVEL_1';

ALTER TABLE data_to_be_masked
  MODIFY COLUMN last_name
  SET TAG FROSTY_FRIDAY.WEEK_009.sensitive_level_tag = 'LEVEL_2';


/*
* ------------------------------------------------------------------------------
* STEP 5: 結果の確認
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
USE DATABASE FROSTY_FRIDAY;
USE SCHEMA WEEK_009;

-- 1) タグとポリシーの紐付け解除（これを先にやらないと DROP しにくい）
ALTER TAG FROSTY_FRIDAY.WEEK_009.sensitive_level_tag UNSET MASKING POLICY;

-- 2) 列からタグを外す（タグDROPの前提）
ALTER TABLE FROSTY_FRIDAY.WEEK_009.data_to_be_masked
  MODIFY COLUMN first_name UNSET TAG FROSTY_FRIDAY.WEEK_009.sensitive_level_tag;

ALTER TABLE FROSTY_FRIDAY.WEEK_009.data_to_be_masked
  MODIFY COLUMN last_name  UNSET TAG FROSTY_FRIDAY.WEEK_009.sensitive_level_tag;

-- 3) ロールからタグを外す（タグDROPの前提）
ALTER ROLE foo1 UNSET TAG FROSTY_FRIDAY.WEEK_009.role_clearance_tag;
ALTER ROLE foo2 UNSET TAG FROSTY_FRIDAY.WEEK_009.role_clearance_tag;

-- 4) （任意）APPLY 権限を剥がす（DROPすれば消えますが、明示的に外す場合）
REVOKE APPLY ON TAG FROSTY_FRIDAY.WEEK_009.sensitive_level_tag FROM ROLE SYSADMIN;
REVOKE APPLY ON TAG FROSTY_FRIDAY.WEEK_009.role_clearance_tag   FROM ROLE SECURITYADMIN;

-- 5) ポリシー・タグを削除
DROP MASKING POLICY IF EXISTS FROSTY_FRIDAY.WEEK_009.mask_by_tag_level;
DROP TAG IF EXISTS FROSTY_FRIDAY.WEEK_009.sensitive_level_tag;
DROP TAG IF EXISTS FROSTY_FRIDAY.WEEK_009.role_clearance_tag;

-- 6) テーブル削除
DROP TABLE IF EXISTS FROSTY_FRIDAY.WEEK_009.data_to_be_masked;

-- 7) （任意）スキーマ削除（Week9用に作ったものなら消してOK）
DROP SCHEMA IF EXISTS FROSTY_FRIDAY.WEEK_009;

-- 8) ロール削除（最後）
DROP ROLE IF EXISTS foo1;
DROP ROLE IF EXISTS foo2;
*/