WITH get_canopus_artist_image AS (
  select canopus_id,isrc,artist_image,count(1) as images
  from `umg-dev.swift_alerts.artist_track_images`
  WHERE artist_image<>"various artists"
  GROUP BY 1,2,3

),
get_canopus AS (
  select canopus_id,isrc,max(images) as max_images
  from get_canopus_artist_image
  WHERE artist_image<>"various artists"
  GROUP BY 1,2
),
get_artist_image AS (
select a.canopus_id,a.isrc,artist_image,images from get_canopus_artist_image a
join get_canopus b
on a.canopus_id=b.canopus_id  and a.isrc=b.isrc and images=max_images
)

--select * from get_canopus_artist_image limit 1000
--select * from get_canopus limit 1000
select * FROM get_artist_image
where images>8
LIMIT 1000