# BigQuery + jupyterで機械学習を行う(その1: データセットの作成)

## これなに?

Big Queryでデータセットのロード、前処理、特徴量生成、モデルの学習、評価を行う方法についての記事です  
jupyter上で実行することで、よりシームレスにbqに接続し、クエリを実行すると共に、データを可視化し、モデルのブラッシュアップを行っていきます

まとめてると、割と長くなったので、この記事の中では、
- jupyter上でマジックコマンドを実行する方法
- bq上にあるdatasetの概要の確認方法

について説明します  

特徴量生成、MLモデルの作成についてはまた別の記事にまとめます  
なお、jupyterや機械学習自体の説明については基本せず、また、pythonでの処理は行わずすべてSQLで分析していきます

# 機械学習でBigQueryを使用するメリット

機械学習でBQを使う際にメリットだと個人的に感じるのは、

- 大量のログであっても容易にすばやくクレンジングや特徴量の作成が行える
- 一度作成したデータセットを簡単に保存しておける
- MLモデルを作る際にパラメータ調整やスケーリングなどの面倒な部分をよしなにやってくれる

ところです  
さらにjupyter上でクエリを実行することで、

- シームレスにbqに接続し、クエリを実行できる
- bqからの返り値を`pandas.DataFrame`として受け取ることで、sqlで表現できないような複雑な処理や可視化が可能
- 実行結果をまとめて、そのままレポートにできる(この記事もjupyterで書きました)

などのメリットがあります

# 分析対象

例としてなにか良いものがないか、`bigquery-public-data` から探してみます


```python
!bq ls --project_id bigquery-public-data
```

`google_analytics_sample`という、`GA`のトランザクションの`dataset`があったので、これで、ユーザーが特定の商品をクリックする確率(isClick)を予測してみようと思います

# 準備

## datasetを生成


```python
!bq mk --location=US google_analytics
```

datasetを作成します。データを読み込んだりする都合上今回使用する `bigquery-public-data.google_analytics_sample` と同じregionにする必要があるため、locationを `US`に指定します(デフォルトでもUSになりますが、一応指定します)

## magic commandの認証

せっかくjupyterを使うので、magic commandを使用してbqに接続できるようにします。


```python
!pip install google-cloud-bigquery
```

tokenのファイルを読み込ませます  
詳細については[こちら](https://google-auth.readthedocs.io/en/latest/user-guide.html#obtaining-credentials)のドキュメントをご覧ください


```python
from google.cloud.bigquery import magics
from google.oauth2 import service_account
credentials = (service_account.Credentials.from_service_account_file('[/path/to/key.json]'))
magics.context.credentials = credentials
```

これで準備は完了です

# %%bigqueryの使い方について

まず、magic commandを使えるようにするために読み込みます


```python
%load_ext google.cloud.bigquery
```

magic comannd `%%bigquery` を使用すると、cell内でbqのクエリを実行でき、更に返り値を`pandas.DataFrame`オブジェクトで受け取れるので、jupyter上で可視化などを行う際に非常に便利です。  
使い方としては以下のような形です

```
%%bigquery [返り値を格納する変数名]　--project [project_id] --param [クエリ内で使いたい変数を格納したdictまたはjson]  

[実行するクエリ]
```

具体的な実行はこのようになります


```python
PARAMS = {'LIMIT' : 5}
PROJECT ='[自身の作成したプロジェクトID]'
```


```python
%%bigquery data --project $PROJECT  --params $PARAMS
SELECT * FROM  `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` LIMIT @LIMIT
```


```python
data
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>visitorId</th>
      <th>visitNumber</th>
      <th>visitId</th>
      <th>visitStartTime</th>
      <th>date</th>
      <th>totals</th>
      <th>trafficSource</th>
      <th>device</th>
      <th>geoNetwork</th>
      <th>customDimensions</th>
      <th>hits</th>
      <th>fullVisitorId</th>
      <th>userId</th>
      <th>channelGrouping</th>
      <th>socialEngagementType</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>None</td>
      <td>1</td>
      <td>1470117657</td>
      <td>1470117657</td>
      <td>20160801</td>
      <td>{'visits': 1, 'hits': 3, 'pageviews': 3, 'time...</td>
      <td>{'referralPath': '/yt/about/', 'campaign': '(n...</td>
      <td>{'browser': 'Internet Explorer', 'browserVersi...</td>
      <td>{'continent': 'Americas', 'subContinent': 'Nor...</td>
      <td>[{'index': 4, 'value': 'North America'}]</td>
      <td>[{'hitNumber': 1, 'time': 0, 'hour': 23, 'minu...</td>
      <td>7194065619159478122</td>
      <td>None</td>
      <td>Social</td>
      <td>Not Socially Engaged</td>
    </tr>
    <tr>
      <th>1</th>
      <td>None</td>
      <td>151</td>
      <td>1470083489</td>
      <td>1470083489</td>
      <td>20160801</td>
      <td>{'visits': 1, 'hits': 3, 'pageviews': 3, 'time...</td>
      <td>{'referralPath': '/yt/about/', 'campaign': '(n...</td>
      <td>{'browser': 'Chrome', 'browserVersion': 'not a...</td>
      <td>{'continent': 'Americas', 'subContinent': 'Nor...</td>
      <td>[{'index': 4, 'value': 'North America'}]</td>
      <td>[{'hitNumber': 1, 'time': 0, 'hour': 13, 'minu...</td>
      <td>8159312408158297118</td>
      <td>None</td>
      <td>Social</td>
      <td>Not Socially Engaged</td>
    </tr>
  </tbody>
</table>
</div>



基本はこれだけで良いと思いますが、より詳細なオプションは[公式ドキュメント](https://google-cloud.readthedocs.io/en/latest/bigquery/generated/google.cloud.bigquery.magics.html)を参考にしてください

# tableの概要を確認

columnの名前と型を確認します


```python
%%bigquery --project $PROJECT 
SELECT table_name, column_name, data_type  FROM `bigquery-public-data.google_analytics_sample.INFORMATION_SCHEMA.COLUMNS` 
WHERE table_name ='ga_sessions_20160801'
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>table_name</th>
      <th>column_name</th>
      <th>data_type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ga_sessions_20160801</td>
      <td>visitorId</td>
      <td>INT64</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ga_sessions_20160801</td>
      <td>visitNumber</td>
      <td>INT64</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ga_sessions_20160801</td>
      <td>visitId</td>
      <td>INT64</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ga_sessions_20160801</td>
      <td>visitStartTime</td>
      <td>INT64</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ga_sessions_20160801</td>
      <td>date</td>
      <td>STRING</td>
    </tr>
    <tr>
      <th>5</th>
      <td>ga_sessions_20160801</td>
      <td>totals</td>
      <td>STRUCT&lt;visits INT64, hits INT64, pageviews INT...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>ga_sessions_20160801</td>
      <td>trafficSource</td>
      <td>STRUCT&lt;referralPath STRING, campaign STRING, s...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>ga_sessions_20160801</td>
      <td>device</td>
      <td>STRUCT&lt;browser STRING, browserVersion STRING, ...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ga_sessions_20160801</td>
      <td>geoNetwork</td>
      <td>STRUCT&lt;continent STRING, subContinent STRING, ...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>ga_sessions_20160801</td>
      <td>customDimensions</td>
      <td>ARRAY&lt;STRUCT&lt;index INT64, value STRING&gt;&gt;</td>
    </tr>
    <tr>
      <th>10</th>
      <td>ga_sessions_20160801</td>
      <td>hits</td>
      <td>ARRAY&lt;STRUCT&lt;hitNumber INT64, time INT64, hour...</td>
    </tr>
    <tr>
      <th>11</th>
      <td>ga_sessions_20160801</td>
      <td>fullVisitorId</td>
      <td>STRING</td>
    </tr>
    <tr>
      <th>12</th>
      <td>ga_sessions_20160801</td>
      <td>userId</td>
      <td>STRING</td>
    </tr>
    <tr>
      <th>13</th>
      <td>ga_sessions_20160801</td>
      <td>channelGrouping</td>
      <td>STRING</td>
    </tr>
    <tr>
      <th>14</th>
      <td>ga_sessions_20160801</td>
      <td>socialEngagementType</td>
      <td>STRING</td>
    </tr>
  </tbody>
</table>
</div>



`STRUCT`の中身がよくわからないので、


```python
bq show  --project_id bigquery-public-data google_analytics_sample.ga_sessions_20160801
```

で確認してみます  
目的変数である`isClick` はかなり入れ込んでるので、少し面倒そうですね

```
   Last modified                             Schema                             Total Rows   Total Bytes   Expiration   Time Partitioning   Clustered Fields   Labels
 ----------------- ----------------------------------------------------------- ------------ ------------- ------------ ------------------- ------------------ --------
  23 Oct 22:12:27   |- visitorId: integer                                       1711         19920125
                    |- visitNumber: integer
                    |- visitId: integer
                    |- visitStartTime: integer
                    |- date: string
                    +- totals: record
 ...
```

予測のための説明変数は

```
hits.hour
hits.product.productPrice
device.isMObile
device.browser
geoNetwork.country
```


としましょう  
細かいcolumnの説明は[こちら](https://support.google.com/analytics/answer/3437719?hl=ja)にあるので、気になる人は御覧ください

# 学習に使用する変数のみを取り出す

下記のクエリで必要なもののみを取り出します


```python
%%bigquery --project $PROJECT 
-- 使用する変数が入っている要素のみを抽出
WITH data AS(
SELECT hits, device, geoNetwork
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` 
),
data2 AS(
SELECT hit.hour, hit.product, device.browser, device.isMobile, geoNetwork.country  FROM data, UNNEST(hits) AS hit
),
data3 AS(
SELECT hour, browser, isMobile, country, product.productPrice, product.isClick FROM data2, UNNEST(product) AS product 
)

SELECT  * FROM data3 LIMIT 5

```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>hour</th>
      <th>browser</th>
      <th>isMobile</th>
      <th>country</th>
      <th>productPrice</th>
      <th>isClick</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>23</td>
      <td>Internet Explorer</td>
      <td>False</td>
      <td>United States</td>
      <td>25000000</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>23</td>
      <td>Internet Explorer</td>
      <td>False</td>
      <td>United States</td>
      <td>50000000</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>23</td>
      <td>Internet Explorer</td>
      <td>False</td>
      <td>United States</td>
      <td>100000000</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>23</td>
      <td>Internet Explorer</td>
      <td>False</td>
      <td>United States</td>
      <td>250000000</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>13</td>
      <td>Chrome</td>
      <td>False</td>
      <td>United States</td>
      <td>16990000</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



いちいち上のクエリを叩くのも面倒なので、最初に作成したdatasetに新たなtableを作成し、そこに取得した結果を保存します


```python
%%bigquery --project $PROJECT 
CREATE  OR REPLACE TABLE google_analytics.ga_dataset AS

-- 以下は上と同様のクエリ
WITH data AS(
SELECT hits, device, geoNetwork
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20160801` 
),
data2 AS(
SELECT hit.hour, hit.product, device.browser, device.isMobile, geoNetwork.country  FROM data, UNNEST(hits) AS hit
),
data3 AS(
SELECT hour, browser, isMobile, country, product.productPrice, product.isClick FROM data2, UNNEST(product) AS product 
)

SELECT  * FROM data3 
```


```python
%%bigquery --project $PROJECT 
-- 確認
SELECT * FROM `google_analytics.ga_dataset` LIMIT 5
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>hour</th>
      <th>browser</th>
      <th>isMobile</th>
      <th>country</th>
      <th>productPrice</th>
      <th>isClick</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>13</td>
      <td>Chrome</td>
      <td>False</td>
      <td>United States</td>
      <td>20990000</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>13</td>
      <td>Chrome</td>
      <td>False</td>
      <td>United States</td>
      <td>990000</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>7</td>
      <td>Firefox</td>
      <td>False</td>
      <td>Canada</td>
      <td>16990000</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>7</td>
      <td>Firefox</td>
      <td>False</td>
      <td>Canada</td>
      <td>16990000</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>13</td>
      <td>Chrome</td>
      <td>False</td>
      <td>United States</td>
      <td>12990000</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



一旦ここまでとして、次は学習に使用するdatasetの前処理と、特徴量生成を行います

# 参考文献 

- [スケーラブルデータサイエンス データエンジニアのための実践Google Cloud Platform](https://www.amazon.co.jp/%E3%82%B9%E3%82%B1%E3%83%BC%E3%83%A9%E3%83%96%E3%83%AB%E3%83%87%E3%83%BC%E3%82%BF%E3%82%B5%E3%82%A4%E3%82%A8%E3%83%B3%E3%82%B9-%E3%83%87%E3%83%BC%E3%82%BF%E3%82%A8%E3%83%B3%E3%82%B8%E3%83%8B%E3%82%A2%E3%81%AE%E3%81%9F%E3%82%81%E3%81%AE%E5%AE%9F%E8%B7%B5Google-Platform-Valliappa-Lakshmanan-ebook/dp/B07R39RLSQ/ref=sr_1_1?__mk_ja_JP=%E3%82%AB%E3%82%BF%E3%82%AB%E3%83%8A&keywords=%E3%82%B9%E3%82%B1%E3%83%BC%E3%83%A9%E3%83%96%E3%83%AB+GCP&qid=1563756392&s=digital-text&sr=1-1)
    - bqのTech Leadである[Valliappa Lakshmanan](https://www.oreilly.com/pub/au/7304)さんの本。日本語版が最近出た
    
- [Google BigQuery: The Definitive Guide](https://www.amazon.com/Google-BigQuery-Definitive-Warehousing-Analytics/dp/1492044466)
    - 同じくValliappa Lakshmananさんの本。ただし、発売は来年

- [ビッグデータ分析・活用のためのSQLレシピ](https://www.amazon.co.jp/%E3%83%93%E3%83%83%E3%82%B0%E3%83%87%E3%83%BC%E3%82%BF%E5%88%86%E6%9E%90%E3%83%BB%E6%B4%BB%E7%94%A8%E3%81%AE%E3%81%9F%E3%82%81%E3%81%AESQL%E3%83%AC%E3%82%B7%E3%83%94-%E5%8A%A0%E5%B5%9C-%E9%95%B7%E9%96%80/dp/4839961263/ref=sr_1_4?__mk_ja_JP=%E3%82%AB%E3%82%BF%E3%82%AB%E3%83%8A&keywords=sql&qid=1563756274&s=books&sr=1-4)
    - ここまでできるのかというレベルにSQLの黒魔術がいろいろのってます


