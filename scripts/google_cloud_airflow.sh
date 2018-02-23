#generate keygen
ssh-keygen -t rsa -C "alexey.rudenko2002@gmail.com"
#copy content of the mykeygen.pub file to GoogleEngine/Metadata/SSH Key/Edit/Add Items
#ssh to airflow-docker
ssh -i mykeygen alexey.rudenko2002@205.209.184.35.bc.googleusercontent.com
#scp files to Airflow server /tmp/
scp -i mykeygen ~/workspace/SwiftTrendSubscriptions/resources/track_alert_subscription.py alexey.rudenko2002@205.209.184.35.bc.googleusercontent.com:/tmp/
scp -i mykeygen ~/workspace/SwiftTrendSubscriptions/target/SwiftTrendSubscriptions-0.1.jar alexey.rudenko2002@205.209.184.35.bc.googleusercontent.com:/tmp/
# On Airflow server
sudo cp /tmp/track_alert_subscription.py /opt/airflow/dags/
sudo cp /tmp/SwiftTrendSubscriptions-0.1.jar /opt/app/swift-subscriptions/track-alerts/