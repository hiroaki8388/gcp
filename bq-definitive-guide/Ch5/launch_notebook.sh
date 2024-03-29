
#!/bin/bash

IMAGE=--image-family=tf-latest-cpu # cpuに特化したimage type
INSTANCE_NAME=dlvm # 任意
MAIL=hasepappagcp@gmail.com # 要変更

echo "Launching $INSTANCE_NAME"
gcloud compute instances create ${INSTANCE_NAME} \
      --machine-type=n1-standard-2 \
      --scopes=https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/userinfo.email \
      ${IMAGE} \
      --image-project=deeplearning-platform-release \
      --boot-disk-device-name=${INSTANCE_NAME} \
      --metadata="proxy-user-mail=${MAIL}"

echo "Looking for Jupyter URL on $INSTANCE_NAME"
while true; do
   proxy=$(gcloud compute instances describe ${INSTANCE_NAME} 2> /dev/null | grep dot-datalab-vm)
   if [ -z "$proxy" ]
   then
      echo -n "."
      sleep 1
   else
      echo "done!"
      echo "$proxy"
      break
   fi
done

# あとは同一のgoogle clientでproxyにブラウザでアクセスする
# gcloud compute ssh hase@dlvm --ssh-key-file ~/.ssh/id_rsa とすれば、consoleからもアクセス可能
