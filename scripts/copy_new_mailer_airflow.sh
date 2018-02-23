# copy *.jar to dev/old Airflow server
#scp -i mykeygen ~/workspace/SwiftTrendSubscriptions/target/SwiftTrendSubscriptions-0.1.jar alexey.rudenko2002@205.209.184.35.bc.googleusercontent.com:/tmp/
if [ "$1" = 1 ]; then
scp -i mykeygen ~/workspace/SwiftAlertsHourly/target/SwiftAlertsHourly-0.1.jar alexey.rudenko2002@swift-airflow.umusic.net:/tmp/
fi
# copy *.py (dag for mailer) to Airflow server
#scp -i mykeygen ~/workspace/SwiftTrendSubscriptions/resources/track_alert_subscription.py alexey.rudenko2002@205.209.184.35.bc.googleusercontent.com:/tmp/
if [ "$2" = 2 ]; then
scp -i mykeygen ~/workspace/SwiftAlertsHourly/src/main/resources/track_alert_subscription_hourly.py alexey.rudenko2002@swift-airflow.umusic.net:/tmp/
fi
