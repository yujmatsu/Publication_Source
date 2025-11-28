# 【Snowflake】Frosty Friday Week 4 やってみた：ネストされたJSONデータをFLATTENでフラットな表にする
# Zenn:https://zenn.dev/yujmatsu/articles/20251129_frostyfriday_004

## はじめに

Snowflake の実践的なスキルを磨くためのコミュニティ課題、「Frosty Friday」。

前回（[Week 3](https://zenn.dev/yujmatsu/articles/20251123_frostyfriday_003)）はメタデータ検索を扱いました。

今回の **Week 4** は、難易度が **"Hard"（上級）** に設定されています。
テーマは **「JSONデータの解析 (JSON Parsing)」** です。

「JSONデータが S3 にあるけど、配列の中に配列が入っていて（ネストしていて）、どうやってテーブルにすればいいかわからない…」
「`FLATTEN` 関数って聞いたことあるけど、使い方がいまいちピンとこない」

そんな方も多いのではないでしょうか？私も学習して知っていたもののそんな状態でした。

Snowflake は **半構造化データ（JSON, Avro, Parquetなど）の扱いが非常に得意** です。

今回は、複雑な階層構造を持つ JSON データを、まるで玉ねぎの皮をむくように一段ずつ展開し、きれいなテーブル形式に変換する方法を解説します。
もちろん、今回も **「コストとベストプラクティス」** を意識したアプローチで攻略していきます。

## 今週の課題：Week 4 - Hard

課題の詳細は公式サイトで確認できます。
[Week 4 – Hard – JSON Parsing](https://frostyfriday.org/blog/2022/07/15/week-4-hard/)

### 課題のストーリー
Frosty Friday コンサルティング社は、Frost大学の歴史学部から依頼を受けました。
彼らはデータウェアハウスで「君主（Monarchs）」のデータを分析したいと考えています。
提供された JSON ファイルをロードし、特定のフォーマットのテーブルに変換してください。


### 要件の要約
1.  **JSONファイルのロード:** 指定された場所にあるファイルをデータウェアハウスに取り込む。
2.  **データの解析とフラット化:** 階層化されたデータをテーブル形式に変換する。
3.  **特定のカラムを作成する:**
    * `ID`: 年代順のID（誕生順）
    * `Inter-House ID`: 家系内での登場順
    * `Nicknames`: あだ名（最大3つまで列を分ける）
    * `Consorts`: 配偶者（最大3人まで列を分ける）
    * その他、出生地、在位期間、埋葬地などの詳細情報
4.  **行の欠損なし:** 処理の過程で行を失わないこと（最終的に **26行** になるはず）。

要するに、**「ネストされたJSONを、配列のインデックスなども活用しながら、分析しやすい完全にフラットなテーブルに変換してね」** という課題です。


## 知識：FLATTEN 関数とは？

JSONの配列（`[...]`）を行（レコード）に展開するための関数です。
Snowflake で JSON を扱う上で、**最も重要で頻出する関数** です。

* **入力:** `[A, B, C]` という1つの配列データ
* **出力:**
    * 行1: A
    * 行2: B
    * 行3: C
    * （3つの行に「展開」される）

また、`FLATTEN` すると `VALUE`（中身）だけでなく、**`INDEX`（配列の何番目か）** や `KEY`（キー名）なども取得できるのがポイントです。今回の課題ではこの `INDEX` が重要になります。


## 実践：ハンズオン

それでは、Snowsight でやっていきましょう。

### Step 0: コンテキストの設定

```sql
-- コンテキストの設定
USE ROLE SYSADMIN;
USE WAREHOUSE FF_WH;
USE DATABASE FROSTY_FRIDAY;

-- Week 4 用のスキーマを作成
CREATE SCHEMA IF NOT EXISTS WEEK_004;
USE SCHEMA WEEK_004;
```

### Step 1: 外部ステージとファイルフォーマットの作成

まずはデータが置かれている S3 バケットへのステージを作成します。
今回もパブリックバケットです。

```sql
-- 外部ステージの作成
CREATE OR REPLACE TEMPORARY STAGE WEEK4_STAGE
  URL = 's3://frostyfridaychallenges/challenge_4/';

-- JSON用のファイルフォーマット作成
-- STRIP_OUTER_ARRAY = TRUE で外側の[]を削除してロード
CREATE OR REPLACE FILE FORMAT FF_JSON_FORMAT
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE; -- 外側の大括弧 [] を取り除くオプション
```

> **TIPS: STRIP_OUTER_ARRAY の重要性**
> JSONデータ全体が `[{...}, {...}]` のように配列で囲まれている場合、このオプションを `TRUE` にすると、Snowflake は配列の中身を1つずつのレコードとしてロードしてくれます。
> により、後続のクエリで `FLATTEN` する回数を1回減らせるだけでなく、データのパーティショニング（分割）が最適化され、**クエリパフォーマンス向上（＝コスト削減）** につながります。

### Step 2: 生データのロード (ELTの "E" と "L")

まずは、データを `VARIANT` 型のカラムとして一時テーブルに取り込みます。

```sql
-- 生データを格納するテーブルの作成とデータロード
CREATE OR REPLACE TABLE WEEK4_RAW_DATA AS
SELECT $1 AS json_data
FROM @WEEK4_STAGE
(FILE_FORMAT => 'FF_JSON_FORMAT');

-- 中身を確認
SELECT * FROM WEEK4_RAW_DATA;
```

> **コスト意識ポイント：なぜ一度テーブルに入れるのか？**
> ステージ上のファイルに対して直接 `SELECT` を投げることも可能ですが、解析クエリを何度も試行錯誤する場合、その都度 S3 へのアクセスとファイルスキャンが発生し、課金対象になります。
> 一度 Snowflake 内のテーブル（今回は Temporary Table）にロードしてしまえば、以降のアクセスは高速かつ低コストになります。

**データの構造（イメージ）:**
1行のレコードに、以下のような階層データが入っていることがわかります。

```json
{
  "Era": "Reyes Católicos",
  "Houses": [
    {
      "House": "Trastámara",
      "Monarchs": [
        {
          "Name": "Isabella I",
          "Nicknames": ["The Catholic"],
          "Consorts": ["Ferdinand II"],
          "Birth": "1451-04-22",
          "Place of Birth": "Madrigal de las Altas Torres",
          ...
        },
        ...
      ]
    }
  ]
}
```

### Step 3: 階層の展開（FLATTENの活用）

このJSONは「Era（時代）」の中に「Houses（家系）」の配列があり、さらにその中に「Monarchs（君主）」の配列があるという **3階層** 構造です。
これをフラットにするために、`LATERAL FLATTEN` を2回使います。

### Step 4: IDの生成とテーブル作成 (ELTの "T")

要件に合わせて全てのカラムを抽出し、`ROW_NUMBER()` でIDを付与してテーブルを作成します。
`Consorts`（配偶者）だけでなく、`Nicknames`（あだ名）も配列になっているため、インデックス指定で取り出します。また、キー名にスペースが含まれる場合（例: `"Place of Birth"`）は、ダブルクォートで囲んで指定します。

```sql
CREATE OR REPLACE TABLE WEEK4_OUTPUT AS
SELECT 
    -- 誕生順にIDを振る
    ROW_NUMBER() OVER (ORDER BY m.value:Birth::DATE) AS ID,
    
    -- 家系内での順番 (配列のインデックス + 1)
    m.index + 1 AS INTER_HOUSE_ID,
    
    -- 第1階層 (Era)
    root.json_data:Era::STRING AS ERA,
    
    -- 第2階層 (House)
    h.value:House::STRING AS HOUSE,
    
    -- 第3階層 (Monarch)
    m.value:Name::STRING AS NAME,
    
    -- Nickname (配列)
    m.value:Nickname[0]::STRING AS NICKNAME_1,
    m.value:Nickname[1]::STRING AS NICKNAME_2,
    m.value:Nickname[2]::STRING AS NICKNAME_3,
    
    -- 詳細情報
    m.value:Birth::DATE AS BIRTH,
    m.value:"Place of Birth"::STRING AS PLACE_OF_BIRTH,
    m.value:"Start of Reign"::DATE AS START_OF_REIGN,
    
    -- Consort (配列)
    -- エスケープ文字(\/)を利用してキーを指定
    m.value:"Consort\/Queen Consort"[0]::STRING AS QUEEN_OR_QUEEN_CONSORT_1,
    m.value:"Consort\/Queen Consort"[1]::STRING AS QUEEN_OR_QUEEN_CONSORT_2,
    m.value:"Consort\/Queen Consort"[2]::STRING AS QUEEN_OR_QUEEN_CONSORT_3,
    
    -- その他の詳細情報
    m.value:"End of Reign"::DATE AS END_OF_REIGN,
    m.value:Duration::STRING AS DURATION,
    m.value:Death::DATE AS DEATH,
    m.value:"Age at Time of Death"::STRING AS AGE_AT_TIME_OF_DEATH_YEARS,
    m.value:"Place of Death"::STRING AS PLACE_OF_DEATH,
    m.value:"Burial Place"::STRING AS BURIAL_PLACE

FROM WEEK4_RAW_DATA root,
LATERAL FLATTEN(input => root.json_data:Houses) h,
LATERAL FLATTEN(input => h.value:Monarchs) m

ORDER BY ID;
```

> **ヒント: "Don't lose any rows"**
> 課題のヒントにあった「行を失わないように」ですが、今回はデータが必ず存在するためデフォルト（`OUTER => FALSE`）で問題ありませんでした。もし配列が空でも親の情報を残したい場合は `LATERAL FLATTEN(..., OUTER => TRUE)` を使います（Left Joinのような挙動になります）。

### Step 5: 結果の確認

作成されたテーブルを確認します。要件通り **26行** になっていれば成功です。

```sql
SELECT * FROM WEEK4_OUTPUT;
```

**実行結果イメージ:**

| ID | INTER_HOUSE_ID | ERA | HOUSE | NAME | NICKNAME_1 | NICKNAME_2 | NICKNAME_3 | BIRTH | PLACE_OF_BIRTH | ... |
|---:|---:|---|---|---|---|---|---|---|---|---|
| 1 | 1 | Pre-Transition | Trastámara | Isabel I de Castilla | NULL | NULL | NULL | 1451-04-22 | Madrigal de las Altas Torres | ... |
| 2 | 3 | Pre-Transition | Trastámara | Fernando II de Aragon | NULL | NULL | NULL | 1452-03-10 | Sos | ... |
| 3 | 2 | Pre-Transition | Trastámara | Fernando V de Castilla | NULL | NULL | NULL | 1452-03-10 | Sos | ... |
| 4 | 6 | Pre-Transition | Trastámara | Felipe I de Castilla | NULL | NULL | NULL | 1478-06-22 | Brujas | ... |
| ... | ... | ... | ... | ... | ... | ... | ... | ... | ... | ... |

IDがきれいに連番になり、NicknameやConsortも列に分かれ、詳細情報も含んだリッチなテーブルができあがりました。
※ただ出来上がった表と課題の最終アウトプットのイメージとを確認したら多少内容が違いました。
　おそらく格納されているJSONファイルが変わったか、最終アプトプットイメージが作成し間違えた・・・？
　確認した限りやっていることは間違っていなさそうなのでこれで問題なしとします。


## 今回の「コスト意識」ポイント

一見すると「SQLのパズル」のような回でしたが、実は **「JSONデータの扱い方」は、Snowflakeのコスト効率に大きく影響する** 重要なポイントです。

1.  **Schema-on-Read vs Schema-on-Write**
    * **Schema-on-Read (読み取り時に解析):** 生のJSON (`VARIANT`型) をそのまま保存し、クエリするたびに `FLATTEN` や `::` で解析する方法。柔軟ですが、毎回 CPU リソースを使うため、頻繁に集計するデータには向きません。
    * **Schema-on-Write (書き込み時に解析):** 今回の Step 4 のように、ロード時（または直後）にフラットなテーブルに変換して保存する方法。ストレージ容量は増えますが、その後のクエリは通常のテーブルスキャンになるため、**検索が高速で、ウェアハウスの稼働時間を短縮（＝コスト削減）**できます。
    * **結論:** 分析で頻繁に使う項目は、今回のようにフラット化して保存するのがベストプラクティスです。

2.  **爆発的な行数の増加に注意**
    * `FLATTEN` は配列の要素数分だけ行を増やします。ネストが深く、要素数が多いデータに対して不用意に `FLATTEN` を繰り返すと、数百万行が一瞬で数億行に膨れ上がり、ウェアハウスのメモリ不足（Spill）や長時間稼働を引き起こすリスクがあります。必要なデータだけに絞って展開することが大切です。


## 次回予告

次回は **Week 5** に挑戦予定です。
テーマは **「UDF (ユーザー定義関数)」** で、UDFは SQL だけでは難しい処理を、Python や Java を使って解決する、Snowflake の拡張機能に触れていく予定です。

## 参考資料
* [Frosty Friday Week 4](https://frostyfriday.org/blog/2022/07/15/week-4-hard/)
* [Snowflake Docs: 半構造化データのクエリ (JSON)](https://docs.snowflake.com/ja/user-guide/querying-semistructured)
* [Snowflake Docs: FLATTEN 関数](https://docs.snowflake.com/ja/sql-reference/functions/flatten)