# On Airflow server to AIrflow application directory:
sudo cp /tmp/SwiftTrendSubscriptions-0.1.jar /opt/app/swift-subscriptions/track-alerts/
# copy *.jar to Airflow server
scp -i mykeygen ~/workspace/SwiftTrendSubscriptions/target/SwiftTrendSubscriptions-0.1.jar alexey.rudenko2002@205.209.184.35.bc.googleusercontent.com:/tmp/
# copy *.py (dag for mailer) to Airflow server
scp -i mykeygen ~/workspace/SwiftTrendSubscriptions/resources/track_alert_subscription.py alexey.rudenko2002@205.209.184.35.bc.googleusercontent.com:/tmp/
# copy Archive 3 files (operators and 2 dags) to Airflow
~/workspace/ArchiveCollection/src/ArchiveCollection/copy_code_airflow.sh
# docker os
docker ps
# ssh to a docker worker:
docker exec -it 43f1ca144b63 bash
# copy jar to local airflow-docker
sudo cp ~/workspace/SwiftTrendSubscriptions/target/SwiftTrendSubscriptions-0.1.jar /opt/app/swift-subscriptions/track-alerts/
# copy dag to local airflow-docker
sudo cp ~/workspace/SwiftTrendSubscriptions/resources/track_alert_subscription.py /opt/airflow/dags/
# run airflow-docker
docker-compose -f docker-compose-CeleryExecutor-version-1-8-1-custom.yml up -d
docker-compose -f docker-compose-CeleryExecutor-version-1-8-1-custom.yml down
docker-compose -f docker-compose-CeleryExecutor-version-1-8-1-custom.yml restart
# start bash for compute engine
https://console.cloud.google.com/compute/instances?project=umg-swift
# join new server (production)
from pwd=~/.ssh/
>ssh swift-airflow.umusic.net
# change to airflow user
>su - airflow
password: airflow
# ~/.ssh/config:
Host swift-airflow.umusic.net
Hostname 10.250.4.3
User alexey.rudenko2002
PubKeyAuthentication yes
IdentityFile ~/.ssh/mykeygen
# scp to new server
scp -i mykeygen ~/workspace/SwiftTrendSubscriptions/target/SwiftTrendSubscriptions-0.1.jar alexey.rudenko2002@swift-airflow.umusic.net:/tmp/
# resolve IP
nslookup 10.250.4.3