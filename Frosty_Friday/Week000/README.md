# Snowflakeの実践力向上！「Frosty Friday」に挑戦するための準備と環境構築
# Zenn:https://zenn.dev/yujmatsu/articles/20251120_frostyfriday_000

## はじめに

Snowflake の学習を進める中で、こんな悩みを持つことはありませんか？

* 「ドキュメントは読んだけど、実際に手を動かす機会が少ない」
* 「業務で使う機能は偏っていて、Snowflake の全容を把握できていない気がする」

学習リソースとしては、Snowflake 公式が提供している **Quickstarts** や **Badge プログラム** も非常に充実しています。これらは新機能を体系的に学ぶのに最適で、まさに「教科書」として素晴らしい教材です。

一方で、公式ハンズオンは「手順通りに進めれば正解にたどり着ける」ように丁寧に設計されているため、**「要件だけを与えられて、自分でゼロから解法を考える」** という、現場で求められる応用力を鍛えるには、少し物足りなさを感じることもありました。

そんな中、とある事情で **「Frosty Friday」** というコミュニティの存在を知り、触れる機会をいただきました。

そのため、その機会を最大限に活かして自身のスキルアップを図るべく、Frosty Friday に継続的に挑戦することに決めました。
その解法や学びを記事としてアウトプットしていくことで、同じようにステップアップを目指す方々の参考になれば幸いです。

今回はその記念すべき第0回として、**Frosty Friday とは何か？** そして **挑戦するためのアカウント登録やGitHub準備** についてまとめます。


## Frosty Friday とは？

[Frosty Friday](https://frostyfriday.org/) は、Snowflake に関するコーディング課題が金曜日に公開されるコミュニティサイトです。
（2025/11/20時点ですでに「WEEK150」まで問題が出ています。）

### 特徴
* **実践的な課題:** 単なる SQL の抽出だけでなく、データロード、半構造化データ（JSON/XML）、GeoSpatial（地理空間情報）、ストリーム＆タスク、Java UDF など、Snowflake の機能をフル活用する課題が出題されます。
* **レベル分け:** `Basic`（初級）、`Intermediate`（中級）、`Hard`（上級）と難易度が分かれており、段階的にスキルアップできます。
* **コミュニティ:** 世界中の Snowflake ユーザーが挑戦しており、解法をシェアし合う文化があります。

単に「動けばいい」だけでなく、「Snowflake らしい書き方（ベストプラクティス）」を学ぶのに最適な教材です。


## 挑戦環境のセットアップ

それでは、実際に挑戦するための環境を整えていきましょう。

### Step 0: Frosty Friday へのユーザー登録
課題を見るだけなら登録は不要ですが、自分の解答を提出したり、リーダーボード（順位表）に参加したりするためにはユーザー登録が必要です。

**提出フロー:**
1.  [Frosty Friday 公式サイト](https://frostyfriday.org/) でアカウント登録する。
2.  課題を解き、コードを自分の **GitHub** (Gist等でも可) に公開する。
3.  課題ページのコメント欄に、その **GitHub URL を投稿して提出**する。

### Step 1: Snowflake 環境の準備
既存の検証環境があればそれを使いますが、本番環境とは切り離すことを強くお勧めします。

> **学習用アカウントの推奨**
> Frosty Friday の課題には、`ACCOUNTADMIN` 権限が必要になるものも多く含まれています。
> セキュリティやコストの観点から、会社の本番環境ではなく、**個人の学習用アカウント**や **[30日間の無料トライアル](https://signup.snowflake.com/)** を利用することを強く推奨します。

**専用の Database / Schema / Warehouse の作成**
課題ごとにオブジェクトが散乱しないよう、専用の箱を用意しておきます。
また、コスト事故を防ぐためにリソースモニターも設定しておくと安心です。

```sql
-- 管理用ロールになる (SYSADMIN推奨)
USE ROLE SYSADMIN;

-- 1. 専用ウェアハウスの作成 (X-SMALLで十分)
CREATE OR REPLACE WAREHOUSE FF_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60   -- 60秒アイドル状態が続くと自動停止（コスト節約）
  AUTO_RESUME = TRUE; -- クエリが投げられたら自動再開

-- 2. 安全策：リソースモニターの作成と割り当て (任意ですが推奨)
-- (ACCOUNTADMIN権限が必要です)
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR FF_MONITOR
  WITH CREDIT_QUOTA = 5 -- 月間5クレジットで停止
  TRIGGERS ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE FF_WH SET RESOURCE_MONITOR = FF_MONITOR;
USE ROLE SYSADMIN; -- ロールを戻す

-- 3. 専用データベースの作成
CREATE DATABASE IF NOT EXISTS FROSTY_FRIDAY;

-- 4. 課題ごとにスキーマを切る運用にする予定
-- 課題は100週以上あるため、ゼロ埋め (WEEK_001) にしておくとソートしやすく便利です
CREATE SCHEMA IF NOT EXISTS FROSTY_FRIDAY.WEEK_001;

-- (任意) チームで取り組む場合などは専用ロールを作成して権限を付与
-- CREATE ROLE IF NOT EXISTS FF_ROLE;
-- GRANT USAGE ON WAREHOUSE FF_WH TO ROLE FF_ROLE;
-- GRANT USAGE ON DATABASE FROSTY_FRIDAY TO ROLE FF_ROLE;
-- GRANT USAGE ON SCHEMA FROSTY_FRIDAY.WEEK_01 TO ROLE FF_ROLE;
```

> **TIPS: ワークシートのコンテキスト設定**
> SQLを実行する際は、意図しない場所（DB/Schema）にテーブルを作らないよう、ワークシートの先頭でコンテキストを明示する癖をつけると良いでしょう。
>
> ```sql
> USE ROLE SYSADMIN;
> USE WAREHOUSE FF_WH;
> USE DATABASE FROSTY_FRIDAY;
> USE SCHEMA WEEK_001;
> ```

### Step 2: GitHub リポジトリの準備
書いた SQL コードは、Snowsight（SnowflakeのWeb UI）に残すだけでなく、**GitHub で管理**することをおすすめします。
提出時にURLが必要になるだけでなく、自身のポートフォリオとしても活用できます。

**ディレクトリ構成案**
以下イメージのようにWeek ごとにフォルダを分ける構成案としています。
各週のフォルダに `README.md` を置いて、学んだことや詰まったポイントをメモしておくと資産になります。

```text
my-frosty-friday-solutions/
├── README.md          # 全体の説明
├── week01/
│   ├── README.md      # Week1のメモ・解説
│   └── solution.sql   # 解答クエリ
├── week02/
│   ├── README.md
│   ├── setup.sql      # データ準備用クエリ
│   └── solution.sql
...
```

とりあえず私は以下のGitHubリポジトリを作成しました。
https://github.com/yujmatsu/Publication_Source


> **TIPS: Snowsight との Git 連携**
> Snowflake には **Git Integration** 機能があります。これを使うと、Snowsight 上から直接 Git リポジトリのファイルを参照したり、バージョン管理されたスクリプトを実行したりできます。
> 余裕があれば、この機能の検証も兼ねて設定してみると面白いかもしれません。
> （参考：[SnowflakeでのGitリポジトリの使用](https://docs.snowflake.com/ja/developer-guide/git/git-overview/)）


## 取り組み方とマイルール

ただ漫然と解くだけではもったいないので、以下のルールで取り組んでいきます。

1.  **公式ドキュメントを必ず参照する**
    * 解けたとしても「なぜその関数を使うのか」「他の方法はないか」をドキュメントで裏取りを可能な限りする。
2.  **コストを意識する**
    * 無駄なスキャンをしていないか、ウェアハウスのサイズは適切か、常に意識します。
3.  **アウトプットする**
    * 解法だけでなく、「どこで詰まったか」「何が学びだったか」を Zenn 記事として残します。


## 今後の予定

まずは [Week 1: Basic](https://frostyfriday.org/blog/2022/07/14/week-1/) から順に挑戦していきます。
外部ステージからのデータロードといった基礎的なところからスタートするようです。

せっかく技術系のブログも始めたばかりなので、記事を書き続けるモチベを保つという意味でも「やってみた」記事頑張ってやっていこうと思います。

## 参考リンク
* [Frosty Friday 公式サイト](https://frostyfriday.org/)
* [Snowflake Documentation](https://docs.snowflake.com/)