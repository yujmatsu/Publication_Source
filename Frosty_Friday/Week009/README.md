# 【Snowflake】Frosty Friday Week 9 やってみた：タグベースのマスキングポリシーで「管理テーブル」での権限制御
# Zenn:https://zenn.dev/yujmatsu/articles/20251208_frostyfriday_009

## はじめに

Snowflake の実践的なスキルを磨くためのコミュニティ課題、「Frosty Friday」。

今回の **Week 9** は、難易度 "Intermediate"（中級）。
テーマは **「タグベースのマスキングポリシー (Tag-based Masking Policy)」** です。

一般的な動的データマスキングでは、テーブルの「列」に対して直接ポリシーを適用します。
しかし、テーブルが100個あったら？ 列が1000個あったら？ いちいちポリシーを適用するのは大変ですよね。

そこで登場するのが **「タグにポリシーを紐付ける」** 機能です。
「この列には `機密レベル: 高` のタグを貼る」だけで、自動的にマスキングがかかるようになります。



さらに今回は、ポリシーの中に `CASE WHEN CURRENT_ROLE() = 'FOO' ...` のようにロール名を直接書く（ハードコーディング）のではなく、**権限管理テーブル**を参照して動的にマスクするかどうかを決める、高度な設計パターンにも挑戦します。

> **前提：Enterprise Edition 以上**
> タグベースのマスキングポリシーは、**Snowflake Enterprise Edition 以上**で利用可能な機能です。

## 今週の課題：Week 9 - Intermediate

課題の詳細は公式サイトで確認できます。
[Week 9 – Intermediate – Tag-based Masking Policies](https://frostyfriday.org/blog/2022/08/12/week-9-intermediate/)

### 課題のストーリー
悪の組織にも「スーパーヒーロー」のデータ漏洩対策が必要です。
`data_to_be_masked` テーブルにある `first_name` と `last_name` をマスクしたいのですが、以下の要件があります。

### 要件の要約
1.  **タグベース:** 列に直接ポリシーを適用するのではなく、タグを介して適用すること。
2.  **動的な権限管理:** マスキングポリシーの中にロール名をハードコーディングしないこと。
3.  **ロールごとの見え方:**
    * **デフォルトユーザー:** `hero_name` のみ見える（名前はマスク）。
    * **Role `foo1`:** `hero_name` と `first_name` が見える。
    * **Role `foo2`:** 全てのデータが見える。


> **攻略のアプローチについて**
> Frosty Friday の要件には「ポリシー内で `CURRENT_ROLE()` などのロールチェック機能を使わないこと」という記述があります（理想的にはロール自体にもタグを付け、タグ同士を比較するのが最も抽象度が高い方法です）。
>
> 今回は以下の2つのパターンを紹介します。
> * **パターン1 (実務的):** マッピングテーブルを作成し、`CURRENT_ROLE()` をキーにして参照する。
> * **パターン2 (発展的):** ロールにもタグを付け、タグ同士のレベルを比較する（マッピングテーブル不要）。


## 知識：タグベースのマスキングポリシー

従来のマスキングと何が違うのでしょうか？

* **従来 (Column-level):**
    * ポリシーを作る → 各テーブルの各列に `SET MASKING POLICY` する。
    * 管理が大変。適用漏れのリスクがある。
* **今回 (Tag-based):**
    * ポリシーを作る → **「タグ」にポリシーを紐付ける** (`ALTER TAG ... SET MASKING POLICY`)。
    * 各列には「タグ」を付けるだけでOK。
    * **タグが付いた列は、自動的にそのポリシーで保護される。**

これに加えて、今回はポリシーの中身で **マッピングテーブル（権限台帳）** を参照させることで、SQLを修正することなく権限変更ができるようにします。


## 実践：ハンズオン

それでは、Snowsight でやっていきましょう。

## パターン1：マッピングテーブルによる制御

まずは、権限管理テーブル（マッピングテーブル）を使って制御する方法です。
SQL（ポリシー）を変更することなく、テーブルのレコード更新だけで権限管理ができるのがメリットです。

### Step 0: コンテキストと変数の準備

```sql
USE ROLE SYSADMIN;
USE WAREHOUSE TEMP_WH;
USE DATABASE FROSTY_FRIDAY;

CREATE SCHEMA IF NOT EXISTS WEEK_009;
USE SCHEMA WEEK_009;

-- 権限付与のために、現在のユーザー名を変数に格納します
SET my_current_user = CURRENT_USER();
```

### Step 1: データとロールの準備 (Start Up Code)

課題で提供されているデータと、検証用のロール `foo1`, `foo2` を作成します。
自分自身にこれらのロールを付与して、切り替えられるようにしておきます。

```sql
-- 1-1. データの作成
CREATE OR REPLACE TABLE data_to_be_masked(first_name varchar, last_name varchar,hero_name varchar);
INSERT INTO data_to_be_masked (first_name, last_name, hero_name) VALUES 
('Eveleen', 'Danzelman','The Quiet Antman'),
('Harlie', 'Filipowicz','The Yellow Vulture'),
-- ... (中略) ...
('Ware', 'Ledstone','Optimo');

-- 1-2. ロールの作成とユーザーへの紐付け (SECURITYADMIN)
USE ROLE SECURITYADMIN;
CREATE OR REPLACE ROLE foo1;
CREATE OR REPLACE ROLE foo2;

GRANT ROLE foo1 TO USER IDENTIFIER($my_current_user);
GRANT ROLE foo2 TO USER IDENTIFIER($my_current_user);

-- 1-3. ロールへのアクセス権限付与 (SYSADMIN)
USE ROLE SYSADMIN;
GRANT USAGE ON DATABASE FROSTY_FRIDAY TO ROLE foo1;
GRANT USAGE ON SCHEMA WEEK_009 TO ROLE foo1;
GRANT USAGE ON WAREHOUSE TEMP_WH TO ROLE foo1;
GRANT SELECT ON TABLE data_to_be_masked TO ROLE foo1;

GRANT USAGE ON DATABASE FROSTY_FRIDAY TO ROLE foo2;
GRANT USAGE ON SCHEMA WEEK_009 TO ROLE foo2;
GRANT USAGE ON WAREHOUSE TEMP_WH TO ROLE foo2;
GRANT SELECT ON TABLE data_to_be_masked TO ROLE foo2;
```

### Step 2: マッピングテーブル（権限管理簿）の作成

ここが今回のポイントです。
「どのロールが、どのタグ（機密レベル）を見れるか」を管理するテーブルを作ります。

> **注意点: マッピングテーブルの保護**
> このマッピングテーブル (`auth_matrix`) 自体にタグを付けてマスクしてしまうと、循環参照や予期せぬエラーの原因になります。マッピングテーブルはタグ付け対象から外すか、アクセス権限を適切に管理してください。

```sql
-- 権限マトリクス: Role -> Allowed Tag Value
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
```

### Step 3: タグとマスキングポリシーの作成・適用

ここからの操作は強力な権限が必要なため、**`ACCOUNTADMIN`** ロールで実行します。
（※タグ作成者とポリシー適用者が異なると権限エラーになりやすいため、まとめて強い権限で行うのがハンズオンでは確実です。）
ポリシーの中で `SYSTEM$GET_TAG_ON_CURRENT_COLUMN` 関数を使い、**「今アクセスしようとしている列に付いているタグの値」** を取得するのがキモです。



```sql
USE ROLE ACCOUNTADMIN;
USE SCHEMA FROSTY_FRIDAY.WEEK_009;

-- 【重要】ACCOUNTADMIN が auth_matrix を参照できるように権限を付与
-- マスキングポリシーの所有者(ACCOUNTADMIN)は、内部で参照するテーブルの権限を持っている必要があります
--また、この GRANT が権限不足になる場合は、auth_matrix の所有者ロール（例: SYSADMIN）で実行してください
GRANT SELECT ON TABLE auth_matrix TO ROLE ACCOUNTADMIN;

-- 1. タグの作成
-- ALLOWED_VALUES を設定しておくと、入力ミスを防げます
CREATE OR REPLACE TAG sensitive_data_tag 
    ALLOWED_VALUES 'LEVEL_1', 'LEVEL_2'
    COMMENT = 'Controls visibility of sensitive data';

-- 2. マスキングポリシーの作成
CREATE OR REPLACE MASKING POLICY dynamic_tag_policy AS (val string) RETURNS string ->
    CASE
        -- 現在のロール(CURRENT_ROLE)が、
        -- 現在のカラムのタグ値(SYSTEM$GET_TAG_ON_CURRENT_COLUMN)に対して
        -- アクセス権を持っているかをマッピングテーブルでチェック
        WHEN EXISTS (
            SELECT 1 
            FROM auth_matrix 
            WHERE role_name = CURRENT_ROLE()
              -- タグ名は完全修飾名で指定するのが安全です
              AND allowed_tag_value = SYSTEM$GET_TAG_ON_CURRENT_COLUMN('FROSTY_FRIDAY.WEEK_009.sensitive_data_tag')
        ) THEN val
        ELSE '*********' -- 権限がない場合はマスク (実務ではNULL考慮などを行うことも多いです)
    END;

-- 3. タグにマスキングポリシーを紐付け
-- これにより、このタグが付いた列には自動的にポリシーが適用されます
ALTER TAG sensitive_data_tag 
SET MASKING POLICY dynamic_tag_policy;

-- 4. 【重要】SYSADMINにタグの利用権限を付与
-- タグの所有者はACCOUNTADMINなので、SYSADMINがテーブルにタグを付けられるよう権限を与えます
GRANT APPLY ON TAG sensitive_data_tag TO ROLE SYSADMIN;

USE ROLE SYSADMIN; -- 作業が終わったら戻る
```

> **TIPS: サブクエリと権限**
> ポリシー内でテーブルを参照する場合、**ポリシーの所有者（作成したロール）** がそのテーブルへの `SELECT` 権限を持っている必要があります。
> もし `Insufficient privileges to operate on table AUTH_MATRIX` というエラーが出た場合は、`auth_matrix` への権限付与を確認するか、テーブルとポリシーの所有者を揃えるようにしてください。

### Step 4: テーブルの列にタグを適用

最後に、テーブルの各列にタグ（と値）を設定します。
この操作はオブジェクトの所有者（今回は `SYSADMIN` で作成しました）で行います。

```sql
USE ROLE SYSADMIN;

-- First Name には 'LEVEL_1' (foo1, foo2が見れる)
ALTER TABLE data_to_be_masked 
MODIFY COLUMN first_name 
SET TAG sensitive_data_tag = 'LEVEL_1';

-- Last Name には 'LEVEL_2' (foo2だけが見れる)
ALTER TABLE data_to_be_masked 
MODIFY COLUMN last_name 
SET TAG sensitive_data_tag = 'LEVEL_2';
```

### Step 5: 結果の確認

各ロールになりきって、データを見てみましょう。

**1. SYSADMIN (デフォルトユーザー)**
マッピングテーブルに定義がないため、タグ付き列はすべてマスクされるはずです。

```sql
USE ROLE SYSADMIN;
SELECT * FROM data_to_be_masked LIMIT 5;
-- 結果: first_name=***, last_name=***, hero_name=見える
```

**2. FOO1 (LEVEL_1 のみ許可)**
`first_name` だけが見えるはずです。

```sql
USE ROLE foo1;
SELECT * FROM data_to_be_masked LIMIT 5;
-- 結果: first_name=見える, last_name=***, hero_name=見える
```

**3. FOO2 (LEVEL_1, LEVEL_2 両方許可)**
すべて見えるはずです。

```sql
USE ROLE foo2;
SELECT * FROM data_to_be_masked LIMIT 5;
-- 結果: first_name=見える, last_name=見える, hero_name=見える
```

完璧です。これでパターン1の攻略は完了です。

## 発展：パターン2（タグ vs タグ）による権限制御

先ほどの手順では「マッピングテーブル」を使いましたが、実は Snowflake の機能を最大限に活かすと、**マッピングテーブルすら不要** にできます。

それは、「列」だけでなく **「ロール」にもタグを付け、タグの値同士（レベル）を比較する** という方法です。



* **列:** `sensitive_level_tag` = 'LEVEL_1'
* **ロール:** `role_clearance_tag` = 'LEVEL_2'
* **ポリシー:** `ロールのレベル >= 列のレベル` なら表示

これなら、ロール名が変わろうがテーブルが変わろうが、タグの値さえ適切ならポリシーを変更する必要は一切ありません。

> **Frosty Friday の要件について**
> この解法でも `CURRENT_ROLE()` を使用しますが、これはロール名でハードコードして分岐するためではなく、**ロールに付与されたタグ値を取得するための“参照キー”** として使用しています。これにより「ロール名に依存しない」という課題の意図を完全に満たします。

### 実装例（SQL）

> **重要：パターン1の設定解除**
> このパターン2を試す前に、先ほどテーブル列に設定したパターン1のタグを外してください。1つの列に複数のマスキングポリシーが適用される状態（競合）を防ぐためです。
>
> ```sql
> USE ROLE ACCOUNTADMIN;
> ALTER TABLE data_to_be_masked MODIFY COLUMN first_name UNSET TAG sensitive_data_tag;
> ALTER TABLE data_to_be_masked MODIFY COLUMN last_name UNSET TAG sensitive_data_tag;
> ```

**1. 2つのタグを作成**

`ALLOWED_VALUES` はあえて設定せず、柔軟性を持たせます。その代わり、運用ルールとして**値は必ず `LEVEL_<数字>` 形式にする** ことを徹底します。

```sql
USE ROLE ACCOUNTADMIN;
USE SCHEMA FROSTY_FRIDAY.WEEK_009;

-- (A) 列に付ける「機密レベル」タグ
CREATE OR REPLACE TAG sensitive_level_tag
  COMMENT = 'Column sensitivity (LEVEL_1/LEVEL_2)';

-- (B) ロールに付ける「クリアランス（権限）レベル」タグ
CREATE OR REPLACE TAG role_clearance_tag
  COMMENT = 'Role clearance (LEVEL_0/LEVEL_1/LEVEL_2)';
```

**2. レベル比較を行うマスキングポリシーの作成**

`SYSTEM$GET_TAG` 関数を使って、実行中のロール (`CURRENT_ROLE()`) に付いているタグの値を取得し、列のタグ値と比較します。
今回は `CASE` 文で分岐するのではなく、`LEVEL_2` のような文字列から数字部分（`2`）を抽出して比較することで、将来 `LEVEL_3` ができてもポリシー修正が不要なロジックにします。

```sql
CREATE OR REPLACE MASKING POLICY mask_by_tag_level AS (val STRING) RETURNS STRING ->
  CASE
    WHEN val IS NULL THEN NULL
    WHEN
      /* ロール側のタグ値から数値を抽出 (例: 'LEVEL_2' -> 2) */
      COALESCE(
        TRY_TO_NUMBER(
          SPLIT_PART(
            SYSTEM$GET_TAG('FROSTY_FRIDAY.WEEK_009.role_clearance_tag', CURRENT_ROLE(), 'ROLE'),
            '_',
            2
          )
        ),
        0 -- タグが無い、または形式違いならレベル0（権限なし）
      )
      >=
      /* 列側のタグ値から数値を抽出 */
      COALESCE(
        TRY_TO_NUMBER(
          SPLIT_PART(
            SYSTEM$GET_TAG_ON_CURRENT_COLUMN('FROSTY_FRIDAY.WEEK_009.sensitive_level_tag'),
            '_',
            2
          )
        ),
        999 -- タグが無い、または形式違いなら最高機密扱い（安全側に倒す）
      )
    THEN val
    ELSE '*********'
  END;

-- ポリシーを列用タグに紐付け
ALTER TAG sensitive_level_tag SET MASKING POLICY mask_by_tag_level;
```

**3. ロールと列にタグを付与**

この方式では、「誰がロールにタグを付けられるか」がセキュリティの肝になります。

> **セキュリティ上の最重要ポイント**
> この方式では、**タグが権限そのもの**になります。もし誰かが勝手に自分のロールに `LEVEL_99` を設定できたら、全てのデータが見えてしまいます。
> 実務では、このタグの `APPLY` 権限は、極少数のガバナンス担当ロールのみに限定してください。
> ロールにタグを付けるには、以下の権限が必要です。
>
> 1.  対象ロール（例: `foo1`）に対する権限（オーナー or 上位ロール）
> 2.  タグ（`role_clearance_tag`）に対する `APPLY` 権限
> 3.  タグが存在するスキーマへの `USAGE` 権限

```sql
-- ロールにクリアランスレベルを設定
-- (※ACCOUNTADMIN で実行するか、上記権限を持つロールで行ってください)
ALTER ROLE foo1 SET TAG FROSTY_FRIDAY.WEEK_009.role_clearance_tag = 'LEVEL_1';
ALTER ROLE foo2 SET TAG FROSTY_FRIDAY.WEEK_009.role_clearance_tag = 'LEVEL_2';
-- SYSADMIN は設定しない (= LEVEL_0 扱い)

-- 列に機密レベルを設定
ALTER TABLE data_to_be_masked MODIFY COLUMN first_name SET TAG sensitive_level_tag = 'LEVEL_1';
ALTER TABLE data_to_be_masked MODIFY COLUMN last_name  SET TAG sensitive_level_tag = 'LEVEL_2';
```

これで、マッピングテーブルを管理することなく、タグの付け替えだけで権限管理が完結するシステムが構築できました。


## 学びとポイント

今回の構成は、大規模なデータガバナンスにおいて非常に強力です。

1.  **タグベースの適用**
    * カラムが増えても「タグを付けるだけ」で済み、ポリシーの適用漏れを防げます。
2.  **設定とロジックの分離**
    * ポリシーの中に `CASE WHEN ROLE = 'FOO1'` と書いてしまうと、ロールが増えるたびにポリシーを書き換える（`ALTER MASKING POLICY`）必要があります。
    * 今回のようにマッピングテーブルを使えば、**`INSERT` / `DELETE` だけで権限管理が可能**になり、運用負荷が激減します。
3.  **実務での「最小権限」**
    * 今回のハンズオンでは簡単のために `ACCOUNTADMIN` を使用しましたが、実務では**タグ管理専用のロール**（`APPLY MASKING POLICY` 権限などを持つロール）を作成し、そこでタグやポリシーを管理するのが安全なベストプラクティスです。


## 次回予告

次回は **Week 10** に挑戦予定です。
記念すべき10回目は、**「ストアドプロシージャ」** がテーマのようです。
ダイナミックにテーブルを作成したりデータをロードしたりする自動化処理を学びます。

## 参考資料
* [Frosty Friday Week 9](https://frostyfriday.org/blog/2022/08/12/week-9-intermediate/)
* [Snowflake Docs: タグベースのマスキングポリシー](https://docs.snowflake.com/ja/user-guide/tag-based-masking-policies)
* [Snowflake Docs: SYSTEM$GET_TAG_ON_CURRENT_COLUMN](https://docs.snowflake.com/ja/sql-reference/functions/system_get_tag_on_current_column)
