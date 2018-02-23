SELECT canopus_id,isrc,track_uri,artist_uri,track_image,artist_image,MAX(last_update) as last_update FROM `umg-dev.swift_trends_alerts.track_artist_image1`
WHERE artist_uri is not null
and track_image is not null
--and artist_image<>"various artists"
--and track_image<>"no track/album image uri"
group by 1,2,3,4,5,6