# /bin/bash
export INSTANCE_NAME='flight'
# instance生成
gcloud sql instances create $INSTANCE_NAME --tier=db-n1-standard-1 --activation-policy=ALWAYS

# 使用している端末のipの権限を付与
# 原著だとwgetコマンドを使用しているが、mac環境のためcurlを使用
gcloud sql instances patch $INSTANCE_NAME --authorized-networks $(curl http://ipecho.net/plain)
