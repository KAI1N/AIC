-- Databricks notebook source
-- MAGIC %md
-- MAGIC # データエンジニアリング入門
-- MAGIC ## 第３回　データ活用実践初級編 データの可視化と活用

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 講師
-- MAGIC - B3 勝又
-- MAGIC ### TA
-- MAGIC - M2 西川
-- MAGIC - M2 豊原
-- MAGIC - B2 好田

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 1．データ可視化とは？

-- COMMAND ----------

-- MAGIC %md
-- MAGIC [第3回講義資料](https://docs.google.com/presentation/d/1yX7VNcpU8Nwdexzzct0TfdQPduSypNapGeYnmxOP8tw/edit?usp=sharing)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2．Databricks SQL の使い方
-- MAGIC - DashBoard
-- MAGIC - SQL ウェアハウス
-- MAGIC - Databricks Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 3．SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.1 【復習】 SQLとは？
-- MAGIC Structured Query Languageの略で、RDB（構造化データ）を操作するための言語．<br>
-- MAGIC SQLには様々な機能があるが、本講習会では「データを分析する」という部分に着目して紹介する．
-- MAGIC
-- MAGIC <b>基本文法<b>
-- MAGIC 1. <b>select</b>　⇒　列(カラム)を指定
-- MAGIC 1. <b>from</b>　⇒　テーブルを指定
-- MAGIC 1. <b>where</b>　⇒　絞り込み条件の指定
-- MAGIC 1. <b>group by</b>　⇒　グループ条件の指定  
-- MAGIC
-- MAGIC
-- MAGIC 上記の基本文法について忘れてしまっていたら第二回授業をチェック！

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.2 JOIN 
-- MAGIC テーブル同士を結合することです。
-- MAGIC データベースからレコードを取得する際、テーブル1とテーブル2のレコードを同時に取得したいなぁ...と思う時や、1つのテーブルだけでは情報が足りないから他のテーブルからも情報を引っ張ってこよう！と思うことがあります。そんな時に使うのがJOINです。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2.1 JOINの種類
-- MAGIC テーブルの結合は大きく分けると内部結合と外部結合に分かれています。<br>どのようにテーブルを結合するかによって、SQLでの記載方法が異なります。<br>また、下記の表のように省略形で記載することもできます。
-- MAGIC | 結合方法 | SQLでの記載方法 | 省略形 |
-- MAGIC | --- | --- | --- |
-- MAGIC | 内部結合 | INNER JOIN | JOIN |
-- MAGIC | 外部結合| LEFT OUTER JOIN | LEFT JOIN |
-- MAGIC | 外部結合 | RIGHT OUTER JOIN | RIGHT JOIN |

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Person テーブル
-- MAGIC | id  |   name    |
-- MAGIC |:---:|:---------:|
-- MAGIC |  1  | toyohara  |
-- MAGIC |  2  | nishikawa |
-- MAGIC |  3  |   kohda   |
-- MAGIC |  4  | katsumata |
-- MAGIC |null |    ito    |
-- MAGIC
-- MAGIC ### total テーブル
-- MAGIC
-- MAGIC | person_id | num |
-- MAGIC |:---------:|:---:|
-- MAGIC |     1     |  1  |
-- MAGIC |     2     |  6  |
-- MAGIC |     3     |  2  |
-- MAGIC |     4     |  2  |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2.2 INNER JOIN
-- MAGIC - <b>INNER JOIN</b>  
-- MAGIC テーブル同士を内部結合した場合、指定した条件に合致したレコードのみを取り出します

-- COMMAND ----------

SELECT name, num
  FROM hive_metastore.default.person as p-- 結合元の左テーブル
  JOIN hive_metastore.default.total as t--　結合先の右テーブル
    ON p.id = t.person_id -- 左テーブルのidと右テーブルのperson_idが一致するもの

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2.3 OUTER JOIN
-- MAGIC 外部結合では、内部結合のように条件に一致させた状態で結合してくれるのに加え、どちらかのテーブルに存在しないもの、NULLのものに関しても強制的に取得してくれます。
-- MAGIC - <b>LEFT JOIN</b>  
-- MAGIC 左外部結合のことで、左のテーブルは全て表示します。
-- MAGIC - <b>RIGHT JOIN</b>  
-- MAGIC 右外部結合のことで、右のテーブルは全て表示します。

-- COMMAND ----------

SELECT name, num
  FROM hive_metastore.default.person as p-- 結合元の左テーブル
  LEFT JOIN hive_metastore.default.total as t--　結合先の右テーブル
    ON p.id = t.person_id -- 左テーブルのidと右テーブルのperson_idが一致するもの

-- COMMAND ----------

SELECT name, num
  FROM hive_metastore.default.person as p-- 結合元の左テーブル
  RIGHT JOIN hive_metastore.default.total as t--　結合先の右テーブル
    ON p.id = t.person_id -- 左テーブルのidと右テーブルのperson_idが一致するもの

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.3 ORDER BY
-- MAGIC - <b>ORDER BY</b> <br>
-- MAGIC テーブルからSELECTでデータを取得するとき,「ORDER BY」を使うと、指定されたカラムを基準に並べ替えることができます。<br>
-- MAGIC 昇順（ASC）または降順（DESC）二つのソート方法があります。

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   name,
-- MAGIC   count(*) as cnt 
-- MAGIC FROM 
-- MAGIC   main.fruits.fruits_table
-- MAGIC GROUP BY
-- MAGIC   name
-- MAGIC ORDER BY
-- MAGIC   cnt  
-- MAGIC   -- 指定しないと昇順 (ASCと一緒)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   name,
-- MAGIC   count(*) as cnt 
-- MAGIC FROM 
-- MAGIC   main.fruits.fruits_table
-- MAGIC GROUP BY
-- MAGIC   name
-- MAGIC ORDER BY
-- MAGIC   cnt ASC
-- MAGIC   -- 指定しない場合と一緒

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   name,
-- MAGIC   count(*) as cnt 
-- MAGIC FROM 
-- MAGIC   main.fruits.fruits_table
-- MAGIC GROUP BY
-- MAGIC   name
-- MAGIC ORDER BY
-- MAGIC   cnt DESC
-- MAGIC   -- 降順

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.4 Having
-- MAGIC - <b>Having</b> <br>
-- MAGIC GROUP BYを使う時、特定な条件をかけたい場合があります。その時、使うのがHAVING句となります。<br>
-- MAGIC 簡単に言うと、GROUP BYに使うWHERE文と言えるかもしれません。

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   name,
-- MAGIC   count(*) as cnt 
-- MAGIC FROM 
-- MAGIC   main.fruits.fruits_table
-- MAGIC GROUP BY
-- MAGIC   name
-- MAGIC HAVING 
-- MAGIC   cnt > 3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.5 limit
-- MAGIC - <b>limit</b> <br>
-- MAGIC SELECT文でレコードを取得する際の行数を制限するために使用されます。 <br>
-- MAGIC 特に大きなテーブルからデータを取得する際に、結果をページングしたり、サンプリングしたりするのに役立つ文法です。

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   name,
-- MAGIC   count(*) as cnt 
-- MAGIC FROM 
-- MAGIC   main.fruits.fruits_table
-- MAGIC GROUP BY
-- MAGIC   name
-- MAGIC ORDER BY
-- MAGIC   cnt DESC
-- MAGIC limit 2 --個数を制限

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3.6 【復習】 SQLの実行順番について
-- MAGIC SQLの文法は順番が決まっています．
-- MAGIC 1. select　⇒　列(カラム)を指定
-- MAGIC 1. from　⇒　テーブルを指定
-- MAGIC 1. join　⇒　複数テーブルの結合処理
-- MAGIC 1. where　⇒　絞り込み条件の指定
-- MAGIC 1. group by　⇒　グループ条件の指定
-- MAGIC 1. having　⇒　グループ化後の絞り込み条件の指定（今回は扱いません、whereと似た処理）
-- MAGIC 1. order by　⇒　ソート順の指定
-- MAGIC 1. limit　⇒　取得する行数の指定

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 4． Covid19 のデータで可視化に挑戦

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4.1  今回の目標
-- MAGIC <img src="https://drive.google.com/uc?export=view&id=19JWF-QORCg_L0hhdsIr3HNRIhvOl7z9I" width="80%"> <br>
-- MAGIC [講師が作成したDashBoard](https://dbc-cb16f346-8fc9.cloud.databricks.com/sql/dashboards/ed32e912-9dca-4266-9307-5ba7313557a8?o=2793874229953371)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### チャレンジ1

-- COMMAND ----------

--  main.covid19.daily_cases テーブルからデータを10個表示
-- ヒント from where limitを使います
SELECT

FROM
  main.covid19.daily_cases


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### チャレンジ2

-- COMMAND ----------

-- 地域別の感染者数を取得してみよう！
-- Prefecture_codeとcasesがあればOK
-- ヒント: SELECTとFROMとSUM、GROUP BYを使うよ！
SELECT 

FROM 

GROUP BY  



-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### チャレンジ3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 参考 DATE_TRUNC 関数
-- MAGIC DATE_TRUNC 関数は、指定した日付部分 (時、日、月など) に基づいてタイムスタンプの式またはリテラルを切り捨てます。
-- MAGIC #### 構文
-- MAGIC DATE_TRUNC('datepart', timestamp)

-- COMMAND ----------

--daily_inpatient_careから月毎の入院患者数,退院者数,未確認者数を取得したい
--まずはdaily_inpatient_careの中身を見てみよう。！

-- 以下クエリ記述
  

-- COMMAND ----------

--じゃあ実際に書いてみよう!


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### チャレンジ4
-- MAGIC
-- MAGIC Databricks SQLを用いて可視化をしてみよう！

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 5． Amazon Redshiftで可視化/集計をやってみよう(時間があったら)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## スキーマ紹介
-- MAGIC
-- MAGIC <img src="https://docs.aws.amazon.com/ja_jp/redshift/latest/dg/images/tutorial-optimize-tables-ssb-data-model.png">
-- MAGIC <br>【出典】https://docs.aws.amazon.com/ja_jp/redshift/latest/dg/tutorial-loading-data.html
-- MAGIC

-- COMMAND ----------

-- 【例1】 月毎の売り上げを可視化



-- COMMAND ----------

-- 【例2】 最も高い収益を上げた製品は何かを抽出してみる


-- COMMAND ----------

-- 【例3】地域ごとの年間売上



-- COMMAND ----------


