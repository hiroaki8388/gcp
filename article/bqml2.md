# BigQuery + jupyterで機械学習を行う(その2: 前処理、EDA、特徴量生成)

# これなに?
BQMLを利用して、機械学習を行うために有用そうな方法をまとめた記事です 
この記事の中では  

- データセットの概観を確認し、探索的に分析
- 分析した結果から精度向上に役立ちそうな特徴量を生成します

使用するデータセットやjupyterとの接続方法については、[前記事](https://qiita.com/hiroaki_hase/items/3d7da2166b5bc495879f)にまとめてあります  
わりと長くなったので、BQMLの説明は次の記事に回します

## 使用するデータセット

前回の記事で作成したgoogle_analyticsのdatasetから、特定の商品をclickする確率を予測します(データセットはgoogleAnalyticsを使用します)

前回と同様にmagic commandを利用します


```python
from google.cloud.bigquery import magics
from google.oauth2 import service_account
credentials = (service_account.Credentials.from_service_account_file('[/path/to/key.json]'))
magics.context.credentials = credentials
```


```python
%load_ext google.cloud.bigquery
```


```python
PROJECT ='[自身の作成したプロジェクトID]'
```


```python
%%bigquery --project $PROJECT 
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



# データセットの特徴を調べる

まず、`isClick`がどのようになっているか確認します


```python
%%bigquery --project $PROJECT 
SELECT 
    isClick, COUNT(1) AS numIsClick 
FROM `google_analytics.ga_dataset`
GROUP BY isClick
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
      <th>isClick</th>
      <th>numIsClick</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>None</td>
      <td>90973</td>
    </tr>
    <tr>
      <th>1</th>
      <td>True</td>
      <td>2034</td>
    </tr>
  </tbody>
</table>
</div>



この結果を見る限り、どうやらclickしなかった場合、は`False`ではなく、`null`が入っているようです  
今後の分析のことを考えると、1/0で二値化したほうが楽なので、変換します  
また、`productPrice`の値も極端に大きいので10の6乗で割ります


```python
%%bigquery --project $PROJECT 
-- 新たなdatasetを定義
CREATE OR REPLACE TABLE google_analytics.ga_dataset_cleansed AS
SELECT 
    *EXCEPT(isClick, productPrice),
    IF(isClick, 1, 0) AS isClick,
    productPrice/1e6 AS productPrice
FROM `google_analytics.ga_dataset`
```






```python
%%bigquery --project $PROJECT 
SELECT * FROM `google_analytics.ga_dataset_cleansed` LIMIT 5
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
      <th>isClick</th>
      <th>productPrice</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>Chrome</td>
      <td>False</td>
      <td>United States</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0</td>
      <td>Chrome</td>
      <td>False</td>
      <td>United States</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>Chrome</td>
      <td>False</td>
      <td>United States</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>Chrome</td>
      <td>False</td>
      <td>United States</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>Chrome</td>
      <td>False</td>
      <td>United States</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>



# pandasを利用して統計的な特徴を確認

次に、pandasを利用して、データセットの概観を確認します。ただし、全部とってくると当然メモリに乗らない(乗る量ならそもそもBigQueryを使わなくてもよい)ので、`RAND()`関数で適当にsamplingしてきます


```python
%%bigquery data_sampled --project $PROJECT 
-- 10%だけサンプリング
SELECT 
    * FROM `google_analytics.ga_dataset_cleansed` 
WHERE  RAND() < 0.1
```

概観を確認します


```python
data_sampled.describe()
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
      <th>isClick</th>
      <th>productPrice</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>9230.000000</td>
      <td>9230.000000</td>
      <td>9230.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>12.747996</td>
      <td>0.021993</td>
      <td>23.121088</td>
    </tr>
    <tr>
      <th>std</th>
      <td>5.346544</td>
      <td>0.146670</td>
      <td>25.548049</td>
    </tr>
    <tr>
      <th>min</th>
      <td>0.000000</td>
      <td>0.000000</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>9.000000</td>
      <td>0.000000</td>
      <td>7.967500</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>12.000000</td>
      <td>0.000000</td>
      <td>16.990000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>17.000000</td>
      <td>0.000000</td>
      <td>21.990000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>23.000000</td>
      <td>1.000000</td>
      <td>575.700000</td>
    </tr>
  </tbody>
</table>
</div>



大まかな値段の分布やclickする確率がわかります。CTRは0.02程度で、`productPrice`の分布は右に歪んでそうですね

# よりよい特徴量を探索

よりよい説明変数を見つけたり、逆に役に立たない変数を削ったりするために、特徴量ごとに分布や特性を分析、可視化してきます

## hour特徴量

`hour`ごとに見たときになにか特徴がないかを確認します


```python
%%bigquery --project $PROJECT 
-- hourとtransactionの数の相関係数
WITH data AS(
SELECT 
    hour, COUNT(1) AS numTrans 
FROM `google_analytics.ga_dataset_cleansed` GROUP BY hour
)
SELECT CORR(hour, numTrans) AS corr FROM data

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
      <th>corr</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0.32829</td>
    </tr>
  </tbody>
</table>
</div>



transction数と時間にはかなり相関がありますね。加えて、おそらく`hour`の値は国ごとに共通なので、時差のことを踏まえると、国別に見るとより相関が高いかもしれません


```python
%%bigquery --project $PROJECT 
-- hourとtransactionの数の相関係数
WITH data AS(
SELECT 
    country, hour, COUNT(1) AS numTrans 
FROM `google_analytics.ga_dataset_cleansed` GROUP BY country, hour
)
-- 国ごと集計
SELECT 
    country, SUM(numTrans) AS numTrans, CORR(hour, numTrans) AS corr 
FROM data GROUP BY country
-- transction数が多い国のみ表示
ORDER BY numTrans DESC LIMIT 10 
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
      <th>country</th>
      <th>numTrans</th>
      <th>corr</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>United States</td>
      <td>71621</td>
      <td>0.378118</td>
    </tr>
    <tr>
      <th>1</th>
      <td>United Kingdom</td>
      <td>2502</td>
      <td>-0.377689</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Canada</td>
      <td>2067</td>
      <td>0.377426</td>
    </tr>
    <tr>
      <th>3</th>
      <td>India</td>
      <td>2020</td>
      <td>-0.086196</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Brazil</td>
      <td>1438</td>
      <td>0.140260</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Mexico</td>
      <td>971</td>
      <td>0.446485</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Taiwan</td>
      <td>901</td>
      <td>0.563291</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Japan</td>
      <td>891</td>
      <td>-0.219272</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Australia</td>
      <td>643</td>
      <td>-0.066862</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Spain</td>
      <td>570</td>
      <td>0.884690</td>
    </tr>
  </tbody>
</table>
</div>



国ごとに見たほうがtransaction数と時間にかなり相関がありそうです。同じ時間でも国ごとに特徴が違うならば、`hour`特徴量はそのまま使うよりも`country`との交差特徴量として生成したほうがより有用そうです  
交差項の作成のために、bqには`ML.FEATURE_CROSS`という関数が用意されているので、これで交差特徴量を作成します


```python
%%bigquery --project $PROJECT 
SELECT 
    country,
    hour,
    ML.FEATURE_CROSS(
    STRUCT(
            country,
-- 数値型の場合はサポートされていないので、CASTする
            CAST(hour AS STRING) AS hour
    )) AS countryHour
FROM `google_analytics.ga_dataset_cleansed`  LIMIT 5
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
      <th>country</th>
      <th>hour</th>
      <th>country_hour</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>United States</td>
      <td>0</td>
      <td>{'country_hour': 'United States_0'}</td>
    </tr>
    <tr>
      <th>1</th>
      <td>United States</td>
      <td>0</td>
      <td>{'country_hour': 'United States_0'}</td>
    </tr>
    <tr>
      <th>2</th>
      <td>United States</td>
      <td>1</td>
      <td>{'country_hour': 'United States_1'}</td>
    </tr>
    <tr>
      <th>3</th>
      <td>United States</td>
      <td>1</td>
      <td>{'country_hour': 'United States_1'}</td>
    </tr>
    <tr>
      <th>4</th>
      <td>United States</td>
      <td>1</td>
      <td>{'country_hour': 'United States_1'}</td>
    </tr>
  </tbody>
</table>
</div>



## productPrice特徴量

商品の値段分布にどのような特徴があるかを見てみます  
まず、観測数を10分割したときに区切りとなる値を`APPROX_QUANTIELS`関数で見てみます


```python
%%bigquery --project $PROJECT 
-- hourとtransactionの数の総関係数
WITH data AS(
SELECT 
        APPROX_QUANTILES(productPrice, 10) AS q
FROM `google_analytics.ga_dataset_cleansed` 
)
SELECT 
    quantile
FROM data, UNNEST(q) AS quantile
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
      <th>quantile</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0.00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1.99</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4.99</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10.99</td>
    </tr>
    <tr>
      <th>4</th>
      <td>16.99</td>
    </tr>
    <tr>
      <th>5</th>
      <td>16.99</td>
    </tr>
    <tr>
      <th>6</th>
      <td>17.99</td>
    </tr>
    <tr>
      <th>7</th>
      <td>20.99</td>
    </tr>
    <tr>
      <th>8</th>
      <td>25.00</td>
    </tr>
    <tr>
      <th>9</th>
      <td>59.99</td>
    </tr>
    <tr>
      <th>10</th>
      <td>575.70</td>
    </tr>
  </tbody>
</table>
</div>



25以上の値の数は少なく、また、17あたりに値が集中しているようです  
ここで、値段ごとにctrがどれ位変わるかを見るために、`ML.BUCKETIZE`関数で、指定した境界値ごとの値に区切ってみます


```python
%%bigquery --project $PROJECT 
SELECT 
    productPrice,
    ML.BUCKETIZE(
        productPrice,
        [2, 5, 16, 17, 20, 50]
    ) AS priceBin
FROM `google_analytics.ga_dataset_cleansed` 
LIMIT 5
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
      <th>productPrice</th>
      <th>priceBin</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0.0</td>
      <td>bin_1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0.0</td>
      <td>bin_1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0.0</td>
      <td>bin_1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0.0</td>
      <td>bin_1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0.0</td>
      <td>bin_1</td>
    </tr>
  </tbody>
</table>
</div>



値段ごとにctrがどれ位変わるかを確認してみます


```python
%%bigquery --project $PROJECT 

WITH data AS(
SELECT
    isClick,
    productPrice,
    ML.BUCKETIZE(
        productPrice,
        [2, 5, 16, 17, 20, 50]
    ) AS priceBin
FROM `google_analytics.ga_dataset_cleansed` 
) 

SELECT 
    priceBin,
    MIN(productPrice)　AS minPrice,
    MAX(productPrice)　AS maxPrice,
    COUNT(1)　AS numTrans,
    SUM(isClick) AS click,
    SUM(isClick)/COUNT(1) AS CTR
FROM data
GROUP BY priceBin
ORDER BY priceBin
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
      <th>priceBin</th>
      <th>minPrice</th>
      <th>maxPrice</th>
      <th>numTrans</th>
      <th>click</th>
      <th>CTR</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>bin_1</td>
      <td>0.00</td>
      <td>1.99</td>
      <td>9247</td>
      <td>168</td>
      <td>0.018168</td>
    </tr>
    <tr>
      <th>1</th>
      <td>bin_2</td>
      <td>2.00</td>
      <td>4.99</td>
      <td>11340</td>
      <td>306</td>
      <td>0.026984</td>
    </tr>
    <tr>
      <th>2</th>
      <td>bin_3</td>
      <td>5.60</td>
      <td>15.99</td>
      <td>15928</td>
      <td>365</td>
      <td>0.022916</td>
    </tr>
    <tr>
      <th>3</th>
      <td>bin_4</td>
      <td>16.79</td>
      <td>16.99</td>
      <td>17176</td>
      <td>285</td>
      <td>0.016593</td>
    </tr>
    <tr>
      <th>4</th>
      <td>bin_5</td>
      <td>17.50</td>
      <td>19.99</td>
      <td>10084</td>
      <td>140</td>
      <td>0.013883</td>
    </tr>
    <tr>
      <th>5</th>
      <td>bin_6</td>
      <td>20.00</td>
      <td>49.95</td>
      <td>17630</td>
      <td>322</td>
      <td>0.018264</td>
    </tr>
    <tr>
      <th>6</th>
      <td>bin_7</td>
      <td>50.00</td>
      <td>575.70</td>
      <td>11602</td>
      <td>448</td>
      <td>0.038614</td>
    </tr>
  </tbody>
</table>
</div>



値段が高くなると、CTRが下がるような傾向は見られず、また、`16.8`まわりに値段が極端に集中してます  
なので、`productPrice`はそのまま数値としてではなく、binで区切り、カテゴリ特徴量に変換し、モデルに入力します

# カテゴリ系の特徴量

ブラウザごと、あるいは携帯からのアクセスかどうかで、CTRに違いが現れるかを確認します


```python
%%bigquery --project $PROJECT 
SELECT
    browser,
    isMobile,
    COUNT(1) AS numTrans,
    SUM(isClick) AS numClick,
    SUM(isClick)/COUNT(1) AS CTR
FROM `google_analytics.ga_dataset_cleansed` 
GROUP BY browser, isMobile
ORDER BY browser, isMobile DESC
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
      <th>browser</th>
      <th>isMobile</th>
      <th>numTrans</th>
      <th>numClick</th>
      <th>CTR</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Amazon Silk</td>
      <td>True</td>
      <td>26</td>
      <td>0</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Chrome</td>
      <td>True</td>
      <td>9271</td>
      <td>159</td>
      <td>0.017150</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Chrome</td>
      <td>False</td>
      <td>67104</td>
      <td>1574</td>
      <td>0.023456</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Edge</td>
      <td>False</td>
      <td>870</td>
      <td>5</td>
      <td>0.005747</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Firefox</td>
      <td>False</td>
      <td>3699</td>
      <td>55</td>
      <td>0.014869</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Internet Explorer</td>
      <td>True</td>
      <td>34</td>
      <td>0</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Internet Explorer</td>
      <td>False</td>
      <td>1500</td>
      <td>13</td>
      <td>0.008667</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Opera</td>
      <td>False</td>
      <td>140</td>
      <td>2</td>
      <td>0.014286</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Safari</td>
      <td>True</td>
      <td>7207</td>
      <td>146</td>
      <td>0.020258</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Safari</td>
      <td>False</td>
      <td>2570</td>
      <td>71</td>
      <td>0.027626</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Safari (in-app)</td>
      <td>True</td>
      <td>215</td>
      <td>9</td>
      <td>0.041860</td>
    </tr>
    <tr>
      <th>11</th>
      <td>UC Browser</td>
      <td>True</td>
      <td>68</td>
      <td>0</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>12</th>
      <td>YaBrowser</td>
      <td>False</td>
      <td>303</td>
      <td>0</td>
      <td>0.000000</td>
    </tr>
  </tbody>
</table>
</div>



同じブラウザでも、携帯からのアクセスかどうかで異なるようなので、この2つも交差特徴量として入力します

# 機械学習に入力するデータセットを作成する

以上をまとめて、機械学習に入力するデータセットを作成します


```python
%%bigquery --project $PROJECT 
CREATE OR REPLACE TABLE google_analytics.ga_dataset_feature AS
SELECT
    ML.FEATURE_CROSS(
    STRUCT(
            country,
            CAST(hour AS STRING) AS hour
    )) 
    AS countryHour,
    ML.FEATURE_CROSS(
    STRUCT(
            browser,
            isMobile
    )) 
    AS browserMobile,
    ML.BUCKETIZE(
    productPrice,
    [2, 5, 16, 17, 20, 50]
    ) AS priceBin,
    isClick
FROM  `google_analytics.ga_dataset_cleansed`  
```






```python
%%bigquery --project $PROJECT 

SELECT * FROM  `google_analytics.ga_dataset_feature`  LIMIT 5 
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
      <th>countryHour</th>
      <th>browserMobile</th>
      <th>priceBin</th>
      <th>isClick</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>{'country_hour': 'Peru_9'}</td>
      <td>{'browser_isMobile': 'Chrome_false'}</td>
      <td>bin_1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>{'country_hour': 'Peru_9'}</td>
      <td>{'browser_isMobile': 'Chrome_false'}</td>
      <td>bin_1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>{'country_hour': 'Peru_9'}</td>
      <td>{'browser_isMobile': 'Chrome_false'}</td>
      <td>bin_1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>{'country_hour': 'Peru_9'}</td>
      <td>{'browser_isMobile': 'Chrome_false'}</td>
      <td>bin_1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>{'country_hour': 'Peru_9'}</td>
      <td>{'browser_isMobile': 'Chrome_false'}</td>
      <td>bin_1</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



次に、このデータセットをモデルを作成し入力します(また今度書きます)

# 参考文献

- [スケーラブルデータサイエンス データエンジニアのための実践Google Cloud Platform](https://www.amazon.co.jp/%E3%82%B9%E3%82%B1%E3%83%BC%E3%83%A9%E3%83%96%E3%83%AB%E3%83%87%E3%83%BC%E3%82%BF%E3%82%B5%E3%82%A4%E3%82%A8%E3%83%B3%E3%82%B9-%E3%83%87%E3%83%BC%E3%82%BF%E3%82%A8%E3%83%B3%E3%82%B8%E3%83%8B%E3%82%A2%E3%81%AE%E3%81%9F%E3%82%81%E3%81%AE%E5%AE%9F%E8%B7%B5Google-Platform-Valliappa-Lakshmanan-ebook/dp/B07R39RLSQ/ref=sr_1_1?__mk_ja_JP=%E3%82%AB%E3%82%BF%E3%82%AB%E3%83%8A&keywords=%E3%82%B9%E3%82%B1%E3%83%BC%E3%83%A9%E3%83%96%E3%83%AB+GCP&qid=1563756392&s=digital-text&sr=1-1)
    - bqのTech Leadである[Valliappa Lakshmanan](https://www.oreilly.com/pub/au/7304)さんの本。日本語版が最近出た
    
- [Google BigQuery: The Definitive Guide](https://www.amazon.com/Google-BigQuery-Definitive-Warehousing-Analytics/dp/1492044466)
    - 同じくValliappa Lakshmananさんの本。ただし、発売は来年

- [ビッグデータ分析・活用のためのSQLレシピ](https://www.amazon.co.jp/%E3%83%93%E3%83%83%E3%82%B0%E3%83%87%E3%83%BC%E3%82%BF%E5%88%86%E6%9E%90%E3%83%BB%E6%B4%BB%E7%94%A8%E3%81%AE%E3%81%9F%E3%82%81%E3%81%AESQL%E3%83%AC%E3%82%B7%E3%83%94-%E5%8A%A0%E5%B5%9C-%E9%95%B7%E9%96%80/dp/4839961263/ref=sr_1_4?__mk_ja_JP=%E3%82%AB%E3%82%BF%E3%82%AB%E3%83%8A&keywords=sql&qid=1563756274&s=books&sr=1-4)
    - ここまでできるのかというレベルでのSQLの黒魔術がいろいろのってます
