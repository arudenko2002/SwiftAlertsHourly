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
mongoDB = '{{var.value.mongodb}}'
outputfile = '{{var.value.outputfile}}'
inputfile = '{{var.value.inputfile}}'
destination_table = '{{var.value.destination_table}}'
environment='{{var.value.environment}}'
#executionDate='{{var.value.executionDate}}'
schedule = '0 14 * * *'
sleep = '{{var.value.sleep}}'
alsome = '{{var.value.alsome}}'
mongodb = '{{var.value.step_mongodb}}'
enrichment = '{{var.value.step_enrichment}}'
major_sql = '{{var.value.step_major_sql}}'

execute_fillTrackHistory = '{{var.value.execute_fillTrackHistory}}'
execute_loadGCStoBQ = '{{var.value.execute_loadGCStoBQ}}'

if environment=="prod":
    schedule='0 14 * * *'

if environment=="dev":
    schedule='0 17 * * *'

#schedule = '{{var.value.schedule_interval}}'
#if executionDate is "today":
executionDate=str(datetime.now())[:10]

print "executionDate="+executionDate

cmd='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.TrackActionSubscription '  \
    + ' --executionDate '+executionDate +"" \
    + ' --project ' + project + ' --runner ' + runner + ' --from_mongodb_users ' + from_mongodb_users + ' --artist_track_images ' + artist_track_images +  ''  \
    + ' --playlist_geography ' + playlist_geography + ' --playlist_track_action ' + playlist_track_action +  '' \
    + ' --product '+product+' --playlist_track_history '+playlist_track_history+' --playlist_history '+playlist_history + ' --streams ' + streams + '' \
    + ' --canopus_resource '+canopus_resource+' --canopus_name '+canopus_name+'' \
    + ' --mail_alerts_output '+mail_alerts_output+' --temp_directory '+temp_directory+' --gmail ' + gmail + ' --mongoDB ' + mongoDB

step1=cmd+' '+mongodb
print step1
step2=cmd+' '+enrichment
print step2
step3=cmd+' '+major_sql
print step3
sendEmail = 'java -Xmx10g -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.SaveBQTableAsJson ' \
    + ' --executionDate '+executionDate +"" \
    + ' --project ' + project + ' --runner ' + runner + ' --whom ' + whom + ' --playlist_track_action ' + playlist_track_action + '' \
    + ' --temp_directory ' + temp_directory + ' --mail_alerts_output ' +  mail_alerts_output + ' --gmail ' + gmail + ' --alsome ' + alsome
print sendEmail

fillTrackArtistImage = 'java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.FillArtistTrackImage' \
    + ' --executionDate '+executionDate +"" \
    + ' --project ' + project + ' --runner ' + runner + ' --from_mongodb_users ' + from_mongodb_users + ' --artist_track_images ' + artist_track_images +  '' \
    + ' --playlist_geography ' + playlist_geography + ' --playlist_track_action ' + playlist_track_action +  '' \
    + ' --product '+product+' --playlist_track_history '+playlist_track_history+' --playlist_history '+playlist_history + ' --streams ' + streams + '' \
    + ' --canopus_resource '+canopus_resource+' --canopus_name '+canopus_name+'' \
    + ' --outputfile gs://umg-dev/swift_alerts --temp_directory '+temp_directory
print fillTrackArtistImage

executionDateToday=str(datetime.now())[:10]
actualDate = str(datetime.now()+timedelta(days=-1))[:10]

fillTrackHistory_beginning="if [ True = "+execute_fillTrackHistory+" ]; then "
loadGCStoBQ_beginning="if [ True = "+execute_loadGCStoBQ+" ]; then "
t1nn_ending = "; fi"

fillTrackHistory = 'java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.FillTrackHistory' \
    + ' --executionDate '+executionDateToday +"" \
    + ' --project ' + project + ' --runner ' + runner \
    + ' --streams ' + streams + '' \
    + ' --outputfile ' + outputfile + ' --temp_directory '+temp_directory

#fillTrackHistory = "echo fillTrackHistory is running"


fillTrackHistory = fillTrackHistory_beginning+fillTrackHistory+t1nn_ending
print fillTrackHistory

sleep_pause = 'sleep '+str(sleep)
#sleep_pause = "echo "+sleep_pause
sleep_pause = fillTrackHistory_beginning+sleep_pause+t1nn_ending
print sleep_pause



loadGCStoBQ = 'java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.LoadGCStoBQ' \
    + ' --executionDate '+executionDateToday +"" \
    + ' --project ' + project + "" \
    + ' --runner ' + runner +"" \
    + ' --destination_table ' + destination_table + '' \
    + ' --input_file ' + inputfile + "" \
    + ' --temp_directory '+temp_directory +'' \
    + ' --schema track_history_schema.txt'

#loadGCStoBQ = "echo loadGCStoBQ is running"

loadGCStoBQ = loadGCStoBQ_beginning+loadGCStoBQ+t1nn_ending
print loadGCStoBQ

bucket="umg-dev"
prefix = "swift_alerts/trackHistoryAPI_"+actualDate+"/"
print("prefix"+prefix)

dagTask = DAG(
   'swift_trends_subscriptions', default_args=default_args
             ,schedule_interval=schedule
             ,start_date=datetime(2017, 11, 01, 0, 0, 0)
             #,schedule_interval=timedelta(days=1)
             )
dagTask.catchup=False
# t1,t2 and t3 are tasks created by instantiating operators
t11 = BashOperator(
   task_id='build_email_table_mongodb',
   #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.TrackActionSubscription --executionDateTest 2017-09-21 --project umg-dev --runner DataflowRunner --mongodb',
   bash_command=step1,
   dag=dagTask)

t12 = BashOperator(
    task_id='build_email_table_enrichment',
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.TrackActionSubscription --executionDateTest 2017-09-21 --project umg-dev --runner DataflowRunner --enrichment',
    bash_command=step2,
    dag=dagTask)

t13 = BashOperator(
    task_id='build_email_table_major_sql',
    bash_command=step3,
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.TrackActionSubscription --executionDateTest 2017-09-21 --project umg-dev --runner DataflowRunner --major_sql',
    dag=dagTask)

# t20
t2 = BashOperator(
    task_id='readBQ_send_email',
    #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.SaveBQTableAsJson --project umg-dev --runner DataflowRunner --executionDateTest 2017-09-21 --whom toteam',
    bash_command=sendEmail,
    dag=dagTask)

t12.set_upstream(t11)
t13.set_upstream(t12)
t2.set_upstream(t13)
##t3.set_upstream(t1)

dagTask10 = DAG(
   'artist_track_images', default_args=default_args,
             #schedule_interval='30 * * * *',
             start_date=datetime.now(),
             schedule_interval='0 7 * * 0')

# t10
t10 = BashOperator(
   task_id='build_image_table',
   #bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.FillArtistTrackImage --executionDate 2017-11-05 --project umg-dev --runner DataflowRunner --temp_directory gs://umg-dev/temp --outputfile gs://umg-dev/swift_alerts',
   bash_command=fillTrackArtistImage,
   dag=dagTask10)

dagTask100 = DAG(
    'track_history_api', default_args=default_args,
    start_date=datetime(2017, 12, 01, 0, 0, 0),
    schedule_interval='0 12 * * *')
dagTask100.catchup=False

# t100

t100 = BashOperator(
    task_id='gcs_track_history',
    bash_command=fillTrackHistory,
    dag=dagTask100)

#t110 = BashOperator(
#    task_id='sleep',
#    bash_command=sleep_pause,
#    dag=dagTask100)


GCS_Files = WaitGCSOperator(
    task_id='GCS_Files',
    bucket=bucket,
    prefix=prefix,
    number="20",
    google_cloud_storage_conn_id="google_cloud_default",
    dag=dagTask
)

t111 = BashOperator(
    task_id='bq_track_history',
    bash_command=loadGCStoBQ,
    dag=dagTask100)

t100>>GCS_Files>>t111

# dagTask20 = DAG(
#       'readBQ_Pipe', default_args=default_args,
#                 #schedule_interval='30 * * * *',
#                 start_date=datetime(2017, 9, 21),
#                 schedule_interval=None)
#
# # t20
# t20 = BashOperator(
#    task_id='readBQ_send_email',
#    bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.SaveBQTableAsJson --executionDate 2017-09-21',
#    #bash_command='echo AAAAAAAAAAAAAAAAAAAAAAAAAAAA',
#    dag=dagTask20)
# t21 = BashOperator(
#    task_id='readFile_sendEmail',
#    bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.ReadJSONEmailJSON --executionDate 2017-09-21',
#    dag=dagTask20)
# t21.set_upstream(t20)
#
# # TEST
# test = DAG(
#       'Test_readBQ_Pipe', default_args=default_args,
#                 #schedule_interval='30 * * * *',
#                 start_date=datetime(2017, 10, 14),
#                 schedule_interval=None)
#
# test1=BashOperator(
# task_id='test1_readBQ_send_email',
#    bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.SaveBQTableAsJson --executionDate 2017-09-21',
#    #bash_command='echo AAAAAAAAAAAAAAAA',
#    dag=test
# )
#
# test2=BashOperator(
# task_id='test2_readBQ_send_email',
#    bash_command='java -cp /opt/app/swift-subscriptions/track-alerts/SwiftTrendSubscriptions-0.1.jar TrackAction.SendTrackAction --executionDate 2017-09-21  --json --email justtome',
#    dag=test
# )
#
# test2.set_upstream(test1)
