"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import date,datetime, timedelta
import os
import getpass

from airflow.operators import WaitGCSOperator
from airflow.operators import WaitQueryOperator

default_args = {
    'owner': 'alexey.rudenko2002@umusic.com',
    'depends_on_past': False,
    #'start_date': datetime(2017, 9, 26),
    #'start_date': datetime.now(),
    #'email': ['airflow@airflow.com'],
    'email': ['arudenko2002@yahoo.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    #'schedule_interval': '30,*,*,*,*',
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

project = '{{var.value.project}}'
runner = '{{var.value.runner}}'
from_mongodb_users = '{{var.value.from_mongodb_users}}'
artist_track_images = '{{var.value.artist_track_images}}'
playlist_geography = '{{var.value.playlist_geography}}'
playlist_track_action = '{{var.value.playlist_track_action}}'
product = '{{var.value.product}}'
playlist_track_history = '{{var.value.playlist_track_history}}'
playlist_history = '{{var.value.playlist_history}}'
streams = '{{var.value.streams}}'
canopus_resource = '{{var.value.canopus_resource}}'
canopus_name = '{{var.value.canopus_name}}'
mail_alerts_output = '{{var.value.mail_alerts_output}}'
temp_directory = '{{var.value.temp_directory}}'
whom = '{{var.value.whom}}'
gmail = '{{var.value.gmail}}'
mongoDB = '{{var.value.mongodb_hourly}}'
outputfile = '{{var.value.outputfile}}'
outputfile_hourly = '{{var.value.outputfile_hourly}}'
inputfile = '{{var.value.inputfile}}'
inputfile_hourly = '{{var.value.inputfile_hourly}}'
destination_table = '{{var.value.destination_table}}'
environment='{{var.value.environment}}'
#executionDate='{{var.value.executionDate}}'
schedule = '0 */2 * * *'
sleep = '{{var.value.sleep}}'
alsome = '{{var.value.alsome}}'
mongodb = '{{var.value.step_mongodb}}'
enrichment = '{{var.value.step_enrichment}}'
major_sql = '{{var.value.step_major_sql}}'
hourly = '{{var.value.hourly}}'
schema = '{{var.value.schema}}'


execute_fillTrackHistory = '{{var.value.execute_fillTrackHistory}}'
execute_loadGCStoBQ = '{{var.value.execute_loadGCStoBQ}}'

if environment=="prod":
    schedule='0 */2 * * *'

if environment=="dev":
    schedule='0 */2 * * *'

executionDateToday=str(datetime.now())
print "executionDateTimeNow="+executionDateToday
actualDate = (str(datetime.now()+timedelta(hours=-2))[:13]+":00:00")
print ("actualDateTime="+actualDate)
partition = actualDate[2:4]+actualDate[11:13]+actualDate[4:10]+" 00:00:00"

fillTrackHistory_beginning="if [ True = "+execute_fillTrackHistory+" ]; then "
loadGCStoBQ_beginning="if [ True = "+execute_loadGCStoBQ+" ]; then "
t1nn_ending = "; fi"

fillTrackHistory = 'java -cp /opt/app/swift-subscriptions/track-alerts/SwiftAlertsHourly-0.1.jar TrackAction.FillTrackHistory' \
                   + ' --executionDate "'+executionDateToday + '"'\
                   + ' --project ' + project \
                   + ' --runner ' + runner \
                   + ' --streams ' + streams \
                   + ' --outputfile ' + outputfile_hourly \
                   + ' --temp_directory '+temp_directory \
                   + ' --hourly '+hourly

print fillTrackHistory
fillTrackHistory = fillTrackHistory_beginning+fillTrackHistory+t1nn_ending
print fillTrackHistory

sleep_pause = 'sleep '+str(sleep)
#sleep_pause = "echo "+sleep_pause
sleep_pause = fillTrackHistory_beginning+sleep_pause+t1nn_ending
print sleep_pause



loadGCStoBQ = 'java -cp /opt/app/swift-subscriptions/track-alerts/SwiftAlertsHourly-0.1.jar TrackAction.LoadGCStoBQ' \
              + ' --executionDate "'+executionDateToday + '"'\
              + ' --project ' + project \
              + ' --runner DirectRunner ' \
              + ' --destination_table ' + destination_table \
              + ' --input_file ' + inputfile_hourly \
              + ' --temp_directory '+temp_directory \
              + ' --schema '+schema \
              + ' --hourly '+hourly

#loadGCStoBQ = "echo loadGCStoBQ is running"

loadGCStoBQ = loadGCStoBQ_beginning+loadGCStoBQ+t1nn_ending
print loadGCStoBQ

cmd='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftAlertsHourly-0.1.jar TrackAction.TrackActionSubscription ' \
    + ' --executionDate "'+executionDateToday +'"' \
    + ' --project ' + project + ' --runner ' + runner + ' --from_mongodb_users ' + from_mongodb_users + ' --artist_track_images ' + artist_track_images \
    + ' --playlist_geography ' + playlist_geography + ' --playlist_track_action ' + playlist_track_action \
    + ' --product '+product+' --playlist_track_history '+playlist_track_history+' --playlist_history '+playlist_history + ' --streams ' + streams \
    + ' --canopus_resource '+canopus_resource+' --canopus_name '+canopus_name \
    + ' --mail_alerts_output '+mail_alerts_output+' --temp_directory '+temp_directory+' --gmail ' + gmail + ' --mongoDB ' + mongoDB + ' --hourly ' + hourly

step1=cmd+' '+mongodb
print step1
step2=cmd+' '+enrichment
print step2
step3=cmd+' '+major_sql
print step3
sendEmail = 'java -Xmx10g -cp /opt/app/swift-subscriptions/track-alerts/SwiftAlertsHourly-0.1.jar TrackAction.SaveBQTableAsJson ' \
            + ' --executionDate "'+executionDateToday +'"'\
            + ' --project ' + project + ' --runner ' + runner + ' --whom ' + whom + ' --playlist_track_action ' + playlist_track_action \
            + ' --temp_directory ' + temp_directory + ' --mail_alerts_output ' +  mail_alerts_output + ' --gmail ' + gmail + ' --alsome ' + alsome \
            + ' --hourly ' + hourly
print sendEmail

bucket="umg-dev"
prefix = "swift_alerts/trackHistoryAPI_"+actualDate.replace(":","-").replace(" ","_")+"_Hourly/"
print (prefix)

dagTask = DAG(
    'swift_trends_subscriptions_hourly', default_args=default_args
    ,schedule_interval=schedule
    ,start_date=datetime(2018, 01, 11, 2, 0, 0)
    #,schedule_interval=timedelta(days=1)
)
dagTask.catchup=False
# t1,t2 and t3 are tasks created by instantiating operators

t1 = BashOperator(
    task_id='gcs_track_history_hourly',
    bash_command=fillTrackHistory,
    dag=dagTask)

#t2 = BashOperator(
#    task_id='sleep_hourly',
#    bash_command=sleep_pause,
#    dag=dagTask)

GCS_Files = WaitGCSOperator(
    task_id='GCS_Files',
    bucket=bucket,
    prefix=prefix,
    number="20",
    google_cloud_storage_conn_id="google_cloud_default",
    dag=dagTask
)

t3 = BashOperator(
    task_id='bq_track_history_hourly',
    bash_command=loadGCStoBQ,
    dag=dagTask)

task_wait_bq = WaitQueryOperator(
    task_id="wait_for_query",
    sql = "SELECT * FROM `"+destination_table+"_hourly` WHERE _PARTITIONTIME='"+partition+"' LIMIT 2",
    dag=dagTask
)

t11 = BashOperator(
    task_id='build_email_table_mongodb_hourly',
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.TrackActionSubscription --executionDateTest 2017-09-21 --project umg-dev --runner DataflowRunner --mongodb',
    bash_command=step1,
    dag=dagTask
)

t12 = BashOperator(
    task_id='build_email_table_enrichment_hourly',
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.TrackActionSubscription --executionDateTest 2017-09-21 --project umg-dev --runner DataflowRunner --enrichment',
    bash_command=step2,
    dag=dagTask)

t13 = BashOperator(
    task_id='build_email_table_major_sql_hourly',
    bash_command=step3,
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.TrackActionSubscription --executionDateTest 2017-09-21 --project umg-dev --runner DataflowRunner --major_sql',
    dag=dagTask)

t20 = BashOperator(
    task_id='readBQ_send_email_hourly',
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.SaveBQTableAsJson --project umg-dev --runner DataflowRunner --executionDateTest 2017-09-21 --whom toteam',
    bash_command=sendEmail,
    dag=dagTask)


t1>>GCS_Files>>t3>>task_wait_bq>>t11>>t12>>t13>>t20
