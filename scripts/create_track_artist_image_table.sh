bq load swift_trends_alerts.track_artist_image gs://umg-dev/swift_alerts/imagesAPI_2017-11-04/images_2017-11-04_033.csv* canopus_id:string,isrc,track_uri:string,artist_uri:string,track_image:string,artist_image:string,last_update:timestamp

# Add all your results to swift_trends_alerts.track_artist_image1
bq load swift_trends_alerts.track_artist_image1 gs://umg-dev/swift_alerts/imagesAPI_2017-11-04/images_2017-11-04_780.csv* canopus_id:string,isrc,track_uri:string,artist_uri:string,track_image:string,artist_image:string,last_update:timestamp

# Then filter union of swift_trends_alerts.track_artist_image1 and swift_trends_alerts.track_artist_image and save it to swift_trends_alerts.track_artist_image
bq --sync query --batch --allow_large_results --replace --nouse_legacy_sql --destination_table umg-dev:swift_trends_alerts.track_artist_image "$(cat sql/fillArtistTrack.sql)"

# Copy resut table to the table that is used by processes
bq cp umg-dev:swift_trends_alerts.track_artist_image umg-dev:swift_alerts.artist_track_images