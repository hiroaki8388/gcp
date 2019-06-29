# /bin/bash
INSTANCE_NAME='flights'
LOCAL_IP=$(curl http://ipecho.net/plain)
MYSQL_IP=$(gcloud sql instances describe $INSTANCE_NAME --format="value(ipAddresses.ipAddress)") 
# instance生成
 gcloud sql instances create $INSTANCE_NAME --tier=db-n1-standard-1 --activation-policy=ALWAYS

# 使用している端末のipの権限を付与
# 原著だとwgetコマンドを使用しているが、mac環境のためcurlを使用
 gcloud sql instances patch $INSTANCE_NAME --authorized-networks $LOCAL_IP

# tableを定義
mysql --host=$MYSQL_IP --user root --verbose < create_table.sql

# data dirに入っているcsvをimport
mysqlimport --local --host $MYSQL_IP --user root --ignore-lines=1 --fields-terminated-by=',' bts data/flights.csv-*
