WITH get_canopus_artist_image AS (
  select canopus_id,artist_uri,artist_image,count(1) as images
  from `umg-dev.swift_trends_alerts.artist_track_images`
  WHERE artist_image<>"various artists"
  GROUP BY 1,2,3
),
get_canopus AS (
  select canopus_id,max(images) as max_images
  from get_canopus_artist_image
  WHERE artist_image<>"various artists"
  GROUP BY 1
),
get_artist_image AS (
select a.canopus_id,artist_uri,artist_image,images from get_canopus_artist_image a
join get_canopus b
on a.canopus_id=b.canopus_id  and images=max_images
),
get_track_uri_image AS (
select track_uri, track_image,count(1) as images
from `umg-dev.swift_trends_alerts.artist_track_images`
group by 1,2
),
get_track_uri AS (
select track_uri, max(images) as max_images
from get_track_uri_image
group by 1
),
get_track_image AS (
select a.track_uri,track_image,images from get_track_uri_image a
join get_track_uri b
on a.track_uri=b.track_uri  and images=max_images
),
get_canopus_isrc_track_uri AS (
select canopus_id, isrc, track_uri from `umg-dev.swift_trends_alerts.artist_track_images`
group by 1,2,3
),
get_canopus_isrc_artist_uri_artist_image AS (
select a.canopus_id, isrc,artist_uri,artist_image from get_artist_image a
join get_canopus_isrc_track_uri b
on a.canopus_id = b.canopus_id
),
get_canopus_isrc_track_uri_track_image AS (
select canopus_id,isrc,a.track_uri,a.track_image from get_track_image a
join get_canopus_isrc_track_uri b
on a.track_uri = b.track_uri
),
get_canopus_isrc_track_image_artist_image AS (
select b.canopus_id,b.isrc,a.track_uri,a.track_image,c.artist_uri,c.artist_image
from get_track_image a
join get_canopus_isrc_track_uri b on a.track_uri = b.track_uri
join get_artist_image c on b.canopus_id = c.canopus_id
)
--select * FROM get_canopus_isrc_artist_uri_artist_image
--select * FROM get_canopus_isrc_track_uri_track_image
select * FROM get_canopus_isrc_track_image_artist_image
LIMIT 1000