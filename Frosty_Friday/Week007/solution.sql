----
-- Week007 回答例
----
-- 問題(英文)
-- Being a villain is hard enough as it is and data issues aren’t only a problem for the good guys. Villains have got a lot of overhead and information to keep track of and EVIL INC. has started using Snowflake for it’s needs. 
-- However , you’ve noticed that the most important part of your superweapons have been leaked :  The catch-phrase!
-- Fortunately , you’ve set up tagging to allow you to keep track of who accessed what information!
-- 
-- Your challenge is to figure out who accessed data that was tagged with “Level Super Secret A+++++++”
-- Because it might be a bit too difficult to create users to access the data, we’re using roles instead of users.
-- The following is the preliminary code we want you to run before the challenge. Note that account_usage takes 2 hours to update, so we suggest running the below code and then coming back to the challenge at least a couple of hours later.
-- If you want to create a new database and schema for this challenge…
-- If you don’t want to create new databases/schemas for this challenge…
--
-- 問題(和訳)
-- 悪役でいることはそれだけで十分に大変ですが、データの問題は善人たちだけの問題ではありません。悪役には管理すべき多くの諸経費や情報があり、EVIL INC.（悪の株式会社）はそのニーズのためにSnowflakeを使用し始めました。
-- しかし、あなたはスーパーウェポンの最も重要な部分が漏洩していることに気づきました。それは「決め台詞」です！
-- 幸いなことに、あなたはタグ付けを設定していたので、誰がどの情報にアクセスしたかを追跡することができます！
--
-- あなたの課題は、「Level Super Secret A+++++++」というタグが付けられたデータに誰がアクセスしたかを突き止めることです。
-- データにアクセスするユーザーを作成するのは少し難しいため、ユーザーの代わりにロールを使用します。
-- 以下は、課題の前に実行してほしい準備コードです。account_usageの更新には2時間かかることに注意してください。そのため、以下のコードを実行してから、少なくとも数時間後に課題に戻ってくることをお勧めします。
--
-- ■やること
-- 1. 準備コードを実行して、テーブル・タグ・ロールを作成し、データをアクセスさせる（証拠作り）。
-- 2. 2〜3時間待つ（ACCOUNT_USAGEへの反映待ち）。
-- 3. ACCESS_HISTORY と TAG_REFERENCES ビュー等を結合して、特定タグ付きデータへアクセスした履歴を抽出する。

/*
* 準備 (Preliminary Code)
* このコードを実行して環境を作り、アクセスログを生成します。
* ※実行後、Account Usageに反映されるまで数時間の待ち時間が必要です。
*/

-- コンテキスト設定
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;
CREATE SCHEMA IF NOT EXISTS WEEK_007;
USE SCHEMA WEEK_007;

-- 1. テーブル作成とデータ投入
create or replace table week7_villain_information (
	id INT,
	first_name VARCHAR(50),
	last_name VARCHAR(50),
	email VARCHAR(50),
	Alter_Ego VARCHAR(50)
);
insert into week7_villain_information (id, first_name, last_name, email, Alter_Ego) values (1, 'Chrissy', 'Riches', 'criches0@ning.com', 'Waterbuck, defassa');
insert into week7_villain_information (id, first_name, last_name, email, Alter_Ego) values (2, 'Libbie', 'Fargher', 'lfargher1@vistaprint.com', 'Ibis, puna');
insert into week7_villain_information (id, first_name, last_name, email, Alter_Ego) values (3, 'Becka', 'Attack', 'battack2@altervista.org', 'Falcon, prairie');
insert into week7_villain_information (id, first_name, last_name, email, Alter_Ego) values (4, 'Euphemia', 'Whale', 'ewhale3@mozilla.org', 'Egyptian goose');
insert into week7_villain_information (id, first_name, last_name, email, Alter_Ego) values (5, 'Dixie', 'Bemlott', 'dbemlott4@moonfruit.com', 'Eagle, long-crested hawk');
insert into week7_villain_information (id, first_name, last_name, email, Alter_Ego) values (6, 'Giffard', 'Prendergast', 'gprendergast5@odnoklassniki.ru', 'Armadillo, seven-banded');
insert into week7_villain_information (id, first_name, last_name, email, Alter_Ego) values (7, 'Esmaria', 'Anthonies', 'eanthonies6@biblegateway.com', 'Cat, european wild');
insert into week7_villain_information (id, first_name, last_name, email, Alter_Ego) values (8, 'Celine', 'Fotitt', 'cfotitt7@baidu.com', 'Clark''s nutcracker');
insert into week7_villain_information (id, first_name, last_name, email, Alter_Ego) values (9, 'Leopold', 'Axton', 'laxton8@mac.com', 'Defassa waterbuck');
insert into week7_villain_information (id, first_name, last_name, email, Alter_Ego) values (10, 'Tadeas', 'Thorouggood', 'tthorouggood9@va.gov', 'Armadillo, nine-banded');

create or replace table week7_monster_information (
	id INT,
	monster VARCHAR(50),
	hideout_location VARCHAR(50)
);
insert into week7_monster_information (id, monster, hideout_location) values (1, 'Northern elephant seal', 'Huangban');
insert into week7_monster_information (id, monster, hideout_location) values (2, 'Paddy heron (unidentified)', 'Várzea Paulista');
insert into week7_monster_information (id, monster, hideout_location) values (3, 'Australian brush turkey', 'Adelaide Mail Centre');
insert into week7_monster_information (id, monster, hideout_location) values (4, 'Gecko, tokay', 'Tafí Viejo');
insert into week7_monster_information (id, monster, hideout_location) values (5, 'Robin, white-throated', 'Turośń Kościelna');
insert into week7_monster_information (id, monster, hideout_location) values (6, 'Goose, andean', 'Berezovo');
insert into week7_monster_information (id, monster, hideout_location) values (7, 'Puku', 'Mayskiy');
insert into week7_monster_information (id, monster, hideout_location) values (8, 'Frilled lizard', 'Fort Lauderdale');
insert into week7_monster_information (id, monster, hideout_location) values (9, 'Yellow-necked spurfowl', 'Sezemice');
insert into week7_monster_information (id, monster, hideout_location) values (10, 'Agouti', 'Najd al Jumā‘ī');

create or replace table week7_weapon_storage_location (
	id INT,
	created_by VARCHAR(50),
	location VARCHAR(50),
	catch_phrase VARCHAR(50),
	weapon VARCHAR(50)
);
insert into week7_weapon_storage_location (id, created_by, location, catch_phrase, weapon) 
    values (1, 'Ullrich-Gerhold', 'Mazatenango', 'Assimilated object-oriented extranet', 'Fintone');
insert into week7_weapon_storage_location (id, created_by, location, catch_phrase, weapon) 
    values (2, 'Olson-Lindgren', 'Dvorichna', 'Switchable demand-driven knowledge user', 'Andalax');
insert into week7_weapon_storage_location (id, created_by, location, catch_phrase, weapon) 
    values (3, 'Rodriguez, Flatley and Fritsch', 'Palmira', 'Persevering directional encoding', 'Toughjoyfax');
insert into week7_weapon_storage_location (id, created_by, location, catch_phrase, weapon) 
    values (4, 'Conn-Douglas', 'Rukem', 'Robust tangible Graphical User Interface', 'Flowdesk');
insert into week7_weapon_storage_location (id, created_by, location, catch_phrase, weapon) 
    values (5, 'Huel, Hettinger and Terry', 'Bulawin', 'Multi-channelled radical knowledge user', 'Y-Solowarm');
insert into week7_weapon_storage_location (id, created_by, location, catch_phrase, weapon) 
    values (6, 'Torphy, Ritchie and Lakin', 'Wang Sai Phun', 'Self-enabling client-driven project', 'Alphazap');
insert into week7_weapon_storage_location (id, created_by, location, catch_phrase, weapon) 
    values (7, 'Carroll and Sons', 'Digne-les-Bains', 'Profound radical benchmark', 'Stronghold');
insert into week7_weapon_storage_location (id, created_by, location, catch_phrase, weapon) 
    values (8, 'Hane, Breitenberg and Schoen', 'Huangbu', 'Function-based client-server encoding', 'Asoka');
insert into week7_weapon_storage_location (id, created_by, location, catch_phrase, weapon) 
    values (9, 'Ledner and Sons', 'Bukal Sur', 'Visionary eco-centric budgetary management', 'Ronstring');
insert into week7_weapon_storage_location (id, created_by, location, catch_phrase, weapon) 
    values (10, 'Will-Thiel', 'Zafar', 'Robust even-keeled algorithm', 'Tin');

-- 2. タグの作成と適用 (テーブルレベルでのタグ付け)
create or replace tag security_class comment = 'sensitive data';

alter table week7_villain_information set tag security_class = 'Level Super Secret A+++++++';
alter table week7_monster_information set tag security_class = 'Level B';
alter table week7_weapon_storage_location set tag security_class = 'Level Super Secret A+++++++';

-- 3. ロールの作成と権限付与
-- (SECURITYADMINまたはACCOUNTADMIN権限が必要です)
USE ROLE SECURITYADMIN;
create or replace role user1;
create or replace role user2;
create or replace role user3;

-- 自身に付与して操作可能にする
grant role user1 to role accountadmin;
grant role user2 to role accountadmin;
grant role user3 to role accountadmin;
-- (作業用) SYSADMINにも付与しておくと便利です
grant role user1 to role sysadmin;
grant role user2 to role sysadmin;
grant role user3 to role sysadmin;

-- 各ロールにオブジェクトへの権限を付与
grant USAGE on warehouse TEMP_WH to role user1;
grant usage on database FROSTY_FRIDAY to role user1;
grant usage on all schemas in database FROSTY_FRIDAY to role user1;
grant select on all tables in database FROSTY_FRIDAY to role user1;

grant USAGE on warehouse TEMP_WH to role user2;
grant usage on database FROSTY_FRIDAY to role user2;
grant usage on all schemas in database FROSTY_FRIDAY to role user2;
grant select on all tables in database FROSTY_FRIDAY to role user2;

grant USAGE on warehouse TEMP_WH to role user3;
grant usage on database FROSTY_FRIDAY to role user3;
grant usage on all schemas in database FROSTY_FRIDAY to role user3;
grant select on all tables in database FROSTY_FRIDAY to role user3;


/*
* 4. アクセス履歴の生成
* 指定されたロールでクエリを実行します
*/

use role user1;
select * from week7_villain_information; 
-- => Tag: 'Level Super Secret A+++++++' (検知されるべき)

use role user2;
select * from week7_monster_information; 
-- => Tag: 'Level B' (検知されない)

use role user3;
select * from week7_weapon_storage_location; 
-- => Tag: 'Level Super Secret A+++++++' (検知されるべき)


/*
* ==============================================================================
* 【重要】待機時間
* ACCOUNT_USAGE の更新には最大2〜3時間の遅延があります。
* ここで休憩をとってください。
* ==============================================================================
*/


/*
* 課題の回答コード (Solution)
* ACCESS_HISTORY, TAG_REFERENCES (Table Domain), QUERY_HISTORY を結合
*/

-- ACCOUNT_USAGEを参照するため権限のあるロールを使用
USE ROLE ACCOUNTADMIN; 
USE SCHEMA FROSTY_FRIDAY.WEEK_007;

SELECT 
    tr.TAG_NAME,
    tr.TAG_VALUE,
    qh.QUERY_ID,
    t.value:objectName::STRING AS TABLE_NAME,
    qh.ROLE_NAME
FROM 
    SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah
    -- 1. アクセスされたテーブルオブジェクトを展開
    CROSS JOIN LATERAL FLATTEN(input => ah.base_objects_accessed) t
    -- 2. タグ参照ビューと結合
    -- 今回は「テーブル」にタグが付いているため、DOMAIN='TABLE' で結合
    JOIN SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES tr
        ON t.value:objectId = tr.OBJECT_ID
        AND tr.DOMAIN = 'TABLE'
    -- 3. クエリ履歴と結合して実行ロールを取得
    JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        ON ah.QUERY_ID = qh.QUERY_ID
WHERE 
    -- ターゲットとなるタグの値でフィルタ
    tr.TAG_VALUE = 'Level Super Secret A+++++++'
    -- 自分が実行したクエリ（直近）に絞る場合
    AND qh.START_TIME > DATEADD('hour', -4, CURRENT_TIMESTAMP())
ORDER BY 
    qh.START_TIME DESC;

/*
* 結果の確認
* ROLE_NAME に 'USER1' と 'USER3' が表示されれば正解です。
* ('USER2' は 'Level B' のテーブルしか見ていないため除外されます)
*/

-- 任意：後片付け
/*
USE ROLE SECURITYADMIN;
DROP ROLE IF EXISTS user1;
DROP ROLE IF EXISTS user2;
DROP ROLE IF EXISTS user3;

USE ROLE SYSADMIN;
DROP TAG IF EXISTS security_class;
DROP TABLE IF EXISTS week7_villain_information;
DROP TABLE IF EXISTS week7_monster_information;
DROP TABLE IF EXISTS week7_weapon_storage_location;
DROP SCHEMA IF EXISTS WEEK_007;
*/