# 【Snowflake】Frosty Friday Week 7 やってみた：タグ付けされた機密データへのアクセスを監査する
# Zenn:https://zenn.dev/yujmatsu/articles/20251203_frostyfriday_007

## はじめに

Snowflake の実践的なスキルを磨くためのコミュニティ課題、**「Frosty Friday」**。

今回の **Week 7** は、難易度 "Intermediate"（中級）。
テーマは **「セキュリティ監査とタグ (Tagging)」** です。

「機密情報が入ったテーブルに、誰がアクセスしたか知りたい」
「『社外秘』タグが付いたデータを見たユーザーを特定したい」

こうしたガバナンス要件は、実務でも頻繁に発生します。
Snowflake では、**Object Tagging（タグ付け）** と **Account Usage** ビューを組み合わせることで、このような監査ログを簡単に抽出できます。



[Image of data security auditing concept]


今回は、悪の組織「EVIL INC.」の管理者になったつもりで、機密データ（決め台詞！）にアクセスした不届き者を特定してみましょう。

## 今週の課題：Week 7 - Intermediate

課題の詳細は公式サイトで確認できます。
[Week 7 – Intermediate – Access History & Tagging](https://frostyfriday.org/blog/2022/07/29/week-7-intermediate/)

![](/images/20251203_frostyfriday_007/1.png) 

### 課題のストーリー
悪の組織「EVIL INC.」では、スーパーウェポンの重要機密である「決め台詞（Catch-phrase）」が漏洩してしまいました。
幸い、データにはタグ付けが行われています。

**「`Level Super Secret A+++++++`」というタグが付いたデータにアクセスしたロール（Role）を特定してください。**

### やること
1.  テーブルを作成し、データを投入する。
2.  機密レベルを表す「タグ」を作成し、テーブルに付与する。
3.  複数のロール（User1, User2, User3）でデータにアクセスする（証拠作り）。
4.  `ACCESS_HISTORY`（アクセス履歴）と `TAG_REFERENCES`（タグ付与状況）を結合し、特定タグへのアクセス履歴を抽出する。

## 知識：監査のための2つのビュー

今回の攻略の鍵となるのは、`SNOWFLAKE.ACCOUNT_USAGE` スキーマにある2つのビューです。

1.  **`ACCESS_HISTORY`**:
    * 「いつ、誰が、どのクエリで、どのテーブル/列にアクセスしたか」が記録されます。
    * 特に `base_objects_accessed` 列には、アクセスされたオブジェクトの情報が JSON 配列で格納されています。
2.  **`TAG_REFERENCES`**:
    * 「どのオブジェクトに、どんなタグ（名前と値）が付いているか」が記録されます。



これらを `QUERY_HISTORY`（クエリ履歴）と組み合わせることで、「特定のタグが付いたオブジェクトにアクセスしたクエリとロール」を特定できます。


## 実践：ハンズオン

それでは、Snowsight でやっていきましょう。

**注意:**
`ACCOUNT_USAGE` のデータ反映には、最大で **2〜3時間** の遅延が発生します。
Step 3（アクセス実行）まで進めたら、一度休憩を入れる必要があります。

### Step 0: コンテキストの設定

```sql
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;

CREATE SCHEMA IF NOT EXISTS WEEK_007;
USE SCHEMA WEEK_007;
```

### Step 1: テーブル作成とデータ投入

3つのテーブル（悪役リスト、モンスターリスト、武器保管場所）を作成し、データを投入します。

```sql
-- 1. 悪役リスト (Villain Information)
create or replace table week7_villain_information (
    id INT, first_name VARCHAR(50), last_name VARCHAR(50), email VARCHAR(50), Alter_Ego VARCHAR(50)
);
insert into week7_villain_information (id, first_name, last_name, email, Alter_Ego) values 
(1, 'Chrissy', 'Riches', 'criches0@ning.com', 'Waterbuck, defassa'),
-- (中略: サンプルデータを数件投入)
(2, 'Libbie', 'Fargher', 'lfargher1@vistaprint.com', 'Ibis, puna');

-- 2. モンスターリスト (Monster Information)
create or replace table week7_monster_information (
    id INT, monster VARCHAR(50), hideout_location VARCHAR(50)
);
insert into week7_monster_information (id, monster, hideout_location) values 
(1, 'Northern elephant seal', 'Huangban'),
(2, 'Paddy heron (unidentified)', 'Várzea Paulista');

-- 3. 武器保管場所 (Weapon Storage Location)
create or replace table week7_weapon_storage_location (
    id INT, created_by VARCHAR(50), location VARCHAR(50), catch_phrase VARCHAR(50), weapon VARCHAR(50)
);
insert into week7_weapon_storage_location (id, created_by, location, catch_phrase, weapon) values 
(1, 'Ullrich-Gerhold', 'Mazatenango', 'Assimilated object-oriented extranet', 'Fintone'),
(2, 'Olson-Lindgren', 'Dvorichna', 'Switchable demand-driven knowledge user', 'Andalax');
```

### Step 2: タグの作成と適用

「機密レベル」を表すタグを作成し、各テーブルに設定します。



```sql
-- タグの作成
create or replace tag security_class comment = 'sensitive data';

-- タグの適用
-- Villain と Weapon テーブルには「超機密 (Super Secret)」タグを付ける
alter table week7_villain_information set tag security_class = 'Level Super Secret A+++++++';
alter table week7_weapon_storage_location set tag security_class = 'Level Super Secret A+++++++';

-- Monster テーブルには「レベルB」タグを付ける
alter table week7_monster_information set tag security_class = 'Level B';
```

### Step 3: ロール作成とアクセス（証拠作り）

犯人役となる3つのロール（User1, User2, User3）を作成し、それぞれ別のテーブルにアクセスさせます。

```sql
-- (SECURITYADMIN または ACCOUNTADMIN で実行)
USE ROLE SECURITYADMIN;

-- ロールの作成
create or replace role user1;
create or replace role user2;
create or replace role user3;

-- 自分(Current User)にロールを付与してスイッチできるようにする
grant role user1 to role accountadmin;
grant role user2 to role accountadmin;
grant role user3 to role accountadmin;

-- 権限付与 (SYSADMINに戻って実行してもOK)
-- ※各ロールに WH, DB, SCHEMA, TABLE へのアクセス権限を付与します
-- (コード省略: ハンズオンでは全テーブルへのSELECT権限を付与してください)

-- ★ ここでアクセスログを生成します！ ★
USE ROLE user1;
select * from week7_villain_information; 
-- => Tag: 'Level Super Secret...' (検知されるべき)

USE ROLE user2;
select * from week7_monster_information; 
-- => Tag: 'Level B' (検知されない)

USE ROLE user3;
select * from week7_weapon_storage_location; 
-- => Tag: 'Level Super Secret...' (検知されるべき)
```

**ここでコーヒーブレイク**
`ACCOUNT_USAGE` への反映には時間がかかります（通常45分〜3時間）。
焦らず待ちましょう。


### Step 4: アクセス履歴の監査 (Solution)

時間が経過したら、いよいよ犯人捜しです。
`ACCOUNTADMIN` ロールで、`SNOWFLAKE` データベースのビューを結合して検索します。

**クエリのポイント:**
1.  `ACCESS_HISTORY` の `base_objects_accessed`（配列）を `FLATTEN` して、アクセスされたテーブルID (`objectId`) を取り出す。
2.  そのIDを使って `TAG_REFERENCES` と結合し、付与されていたタグを確認する。
3.  `QUERY_HISTORY` と結合して、実行したロール名 (`ROLE_NAME`) を取得する。



```sql
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
    -- 自分が実行したクエリ（直近）に絞る
    AND qh.START_TIME > DATEADD('hour', -4, CURRENT_TIMESTAMP())
ORDER BY 
    qh.START_TIME DESC;
```

### 結果確認

実行結果の `ROLE_NAME` 列を確認してください。

* **`USER1`**: `week7_villain_information` (Super Secret) を見たので**検知**。
* **`USER3`**: `week7_weapon_storage_location` (Super Secret) を見たので**検知**。
* **`USER2`**: `week7_monster_information` (Level B) しか見ていないので、結果には**含まれない**。

正しく抽出できていれば成功です。


## 学びとポイント

1.  **タグによるデータ分類**
    * テーブル名やスキーマ名だけでなく、「タグ」というメタデータを付与することで、横断的なセキュリティ管理が可能になります。
2.  **ACCESS_HISTORY の強力さ**
    * 誰がどのデータに触れたかを、列レベル（今回はテーブルレベルですが）で追跡できる強力なビューです。
    * `FLATTEN` 関数を使って JSON 配列を展開するテクニックがここでも活きてきます。
3.  **ACCOUNT_USAGE の遅延**
    * 監査ログはリアルタイムではありません。インシデント対応などで直近のログを見たい場合は、タイムラグがあることを考慮する必要があります。


## 次回予告

次回は **Week 8** に挑戦します。
テーマは再び **「Stream」** ですが、今度は `TASKS` と組み合わせたパイプライン構築になりそうです。


## 参考資料
* [Frosty Friday Week 7](https://frostyfriday.org/blog/2022/07/29/week-7-intermediate/)
* [Snowflake Docs: オブジェクトのタグ付け (Object Tagging)](https://docs.snowflake.com/ja/user-guide/object-tagging)
* [Snowflake Docs: ACCESS_HISTORY ビュー](https://docs.snowflake.com/ja/sql-reference/account-usage/access_history)
* [Snowflake Docs: TAG_REFERENCES ビュー](https://docs.snowflake.com/ja/sql-reference/account-usage/tag_references)