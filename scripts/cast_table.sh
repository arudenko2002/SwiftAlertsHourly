#COPY TABLE FROM SOURCE USING SQL QUERY
#bq --sync query --batch --allow_large_results --replace --nouse_legacy_sql --destination_table umg-dev:swift_trends_alerts.artist_track_images "$(cat sql/cast_table.sql)"
#load files to the table FILLARTISTTRACK
version=421
echo $version
bq load -F '{' swift_trends_alerts.artist_track_images$version gs://umg-dev/swift_alerts/imagesAPI_2018-01-03/images_2018-01-03_$version.csv* canopus_id:int64,artist_name:string,isrc:string,track_uri:string,artist_uri:string,track_image:string,artist_image:string,last_update:timestamp
##bq load swift_trends_alerts.artist_track_images$version /Users/rudenka/Documents/images_2017-12-20_$version.csv* canopus_id:int64,artist_name:string,isrc:string,track_uri:string,artist_uri:string,track_image:string,artist_image:string,last_update:timestamp
#bq load swift_trends_alerts.artist_track_images         gs://umg-dev/swift_alerts/imagesAPI_2017-12-20/images_2017-12-20_$version.csv* canopus_id:int64,artist_name:string,isrc:string,track_uri:string,artist_uri:string,track_image:string,artist_image:string,last_update:timestamp
bq load -F '{' swift_alerts.artist_track_images                gs://umg-dev/swift_alerts/imagesAPI_2018-01-03/images_2018-01-03_$version.csv* canopus_id:int64,artist_name:string,isrc:string,track_uri:string,artist_uri:string,track_image:string,artist_image:string,last_update:timestamp