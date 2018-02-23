# copy *.jar to dev/old Airflow server
scp -i mykeygen ~/workspace/SwiftTrendSubscriptions/target/SwiftTrendSubscriptions-0.1.jar alexey.rudenko2002@205.209.184.35.bc.googleusercontent.com:/tmp/
# copy *.py (dag for mailer) to Airflow server
scp -i mykeygen ~/workspace/SwiftTrendSubscriptions/resources/track_alert_subscription.py alexey.rudenko2002@205.209.184.35.bc.googleusercontent.com:/tmp/
