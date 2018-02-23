WITH  getToday AS (
  --SELECT CURRENT_DATE()
  SELECT TIMESTAMP("{ExecutionDate}") as today
),getTodayMinusOne AS (
  --SELECT CURRENT_DATE()
  SELECT TIMESTAMP("{ExecutionDateMinusOne}") as todayminusone
),getTodayMinusTWO AS (
  --SELECT CURRENT_DATE()
  SELECT TIMESTAMP("{ExecutionDateMinusTwo}") as todayminustwo
),
 get_canopus_artist_image AS (
    select canopus_id,artist_uri,artist_image,count(1) as images
    from `{artist_track_images}`
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
      from `{artist_track_images}`
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
      select canopus_id, isrc, track_uri from `{artist_track_images}`
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
  ),
get_user_products_trackers  AS(
  SELECT
    c.pt,
    a.*,
    b.release_artist_id,b.isrc,
    b.product_title,
    --CONCAT(b.r2_release_title,"-",b.r2_release_version_title) as product_title,
    --CASE WHEN bb.formatted_title is not NULL THEN bb.formatted_title ELSE b.product_title END as product_title,
    --bb.artist_name,
    c.playlist_uri,
    c.playlist_id,
    c.name,
    c.owner_id,
    c.owner_uri,
    c.followers,
    "" as description,
    c.report_date,
    c.position,
    c.added_at,
    c.added_by,
    c.track_uri,
    c.track_name,
    c.artist_uri,
    c.artist_name,
    c.album_uri,
    c.album_name
  FROM
    (
    --SELECT * FROM `umg-dev.swift_alerts.from_mongodb_users`
    SELECT * FROM `{from_mongodb_users}`
    --where
    ---- conopus_id=10057501 or
    --conopus_id=10064799
    --or conopus_id=10238158
    --or conopus_id=10072176
    --or conopus_id=10001057
    --or conopus_id=10042171
    ) a
  JOIN
    `umg-swift.metadata.product` b
    --ON a.conopus_id=b.master_artist_id
    ON a.conopus_id=b.release_artist_id
  join
  (
    select resource.*,name.default_name as artist_name from `{canopus_resource}` resource
    join `{canopus_name}` name
    on resource.canopus_id=name.canopus_id
  )
  bb
    on b.isrc=bb.isrc
  JOIN
    (
    SELECT
    _PARTITIONTIME as pt,
    playlist_uri,
    playlist_id,
    playlist_date as report_date,
    playlist_name as name,
    playlist_owner as owner_id,
    playlist_owner_uri as owner_uri,
    --followers,
    --c.position,
    track_add_by as added_by,
    track_uri,
    track_name,
    isrc,
    artist_uri,
    artist_name,
    album_uri,
    album_name,
    max(followers) as followers,
    max(position) as position,
    max(load_timestamp) as added_at

    FROM
    --`umg-partner.spotify.playlist_track_history`
    --`umg-dev.swift_alerts.spotify_track_history_details`
    `{playlist_track_history}`
    WHERE
    LENGTH(isrc)>0
    --AND _PARTITIONTIME>="2017-08-01 00:00:00"
    --AND _PARTITIONTIME>=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 2 DAY))
    --AND _PARTITIONTIME>=TIMESTAMP(SELECT todayminustwo FROM getTodayMinusTwo)
    --AND _PARTITIONTIME<"2017-08-03 00:00:00"
    --AND _PARTITIONTIME<TIMESTAMP((SELECT today FROM getToday))
    AND (
        _PARTITIONTIME=(SELECT todayminusone FROM getTodayMinusOne)
        OR
        _PARTITIONTIME=(SELECT todayminustwo FROM getTodayMinusTwo)
    )
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    ) c ON b.isrc=c.isrc
),
getJOIN AS (
SELECT *
FROM (
SELECT
CASE WHEN (a.userid is not null) THEN a.userid ELSE b.userid END as userId,
--CASE WHEN (a.firstname is not null) THEN a.firstname ELSE b.firstname END as firstname,
CASE WHEN (a.lastname is not null) THEN a.lastname ELSE b.lastname END as lastname,
CASE WHEN (a.album_uri is not null) THEN a.album_uri ELSE b.album_uri END as album_uri,
CASE WHEN (a.album_name is not null) THEN a.album_name ELSE b.album_name END as album_name,
CASE WHEN (a.track_name is not null) THEN a.track_name ELSE b.track_name END as track_name,

CASE WHEN (a.release_artist_id is not null) THEN a.release_artist_id  ELSE b.release_artist_id  END as release_artist_id ,
CASE WHEN (a.product_title is not null) THEN a.product_title ELSE b.product_title END as product_title,

CASE WHEN (a.playlist_id is not null) THEN a.playlist_id ELSE b.playlist_id END as playlist_id,
CASE WHEN (a.name is not null) THEN a.name ELSE b.name END as name,
"" as description,
CASE WHEN (a.added_at is not null) THEN a.added_at ELSE b.added_at END as added_at,
CASE WHEN (a.conopus_id is not null) THEN a.conopus_id ELSE b.conopus_id END as conopus_id,
CASE WHEN (a.artist_uri is not null) THEN a.artist_uri ELSE b.artist_uri END as artist_uri,
CASE WHEN (a.artist_name is not null) THEN a.artist_name ELSE b.artist_name END as artist_name,
CASE WHEN (a.owner_id is not null) THEN a.owner_id ELSE b.owner_id END as owner_id,
CASE WHEN (a.owner_uri is not null) THEN a.owner_uri ELSE b.owner_id END as owner_uri,
CASE WHEN (a.followers is not null) THEN a.followers ELSE b.followers END as followers,
--CASE WHEN (a.playlist_id is not null) THEN a.playlist_id ELSE b.playlist_id END as playlist_id2,
CASE WHEN (a.playlist_uri is not null) THEN a.playlist_uri ELSE b.playlist_uri END as playlist_uri,
--CASE WHEN (a.name is not null) THEN a.name ELSE b.name END as name,
CASE WHEN (a.isrc is not null) THEN a.isrc ELSE b.isrc END as isrc,
CASE WHEN (a.report_date is not null) THEN TIMESTAMP_ADD(a.report_date, INTERVAL 2 HOUR) ELSE b.report_date END as report_date,
CASE WHEN (a.email is not null) THEN a.email ELSE b.email END as email,
CASE WHEN (a.firstname is not null) THEN a.firstname ELSE b.firstname END as firstname,
CASE WHEN (a.track_uri is not null) THEN a.track_uri ELSE b.track_uri END as track_uri,
CASE WHEN (a.position is not null) THEN a.position ELSE b.position END as position,
CASE WHEN (
          a.report_date is not null and b.report_date is null
       ) THEN "DROP"
     WHEN (
          a.report_date is null and b.report_date is not null
       ) THEN "ADD"
     WHEN  (
         a.report_date is not null
         OR
         b.report_date is not null
       ) THEN "NOACTION"
END as action_type

FROM (
SELECT release_artist_id,product_title,userid,firstname,lastname,email,playlist_uri,isrc,report_date,track_uri,track_name,
    playlist_id,
    name,
    owner_id,
    owner_uri,
    followers,
    artist_uri,
    artist_name,
    album_uri,
    album_name,
    added_at,
    conopus_id,
    position
from
get_user_products_trackers
where
--report_date=DATE_SUB((SELECT today FROM getToday), INTERVAL 2 DAY)
--report_date=(SELECT todayminustwo FROM getTodayMinusTwo)
pt=(SELECT todayminustwo FROM getTodayMinusTwo)

) a
full join
(
SELECT release_artist_id,product_title,userid,firstname,lastname,email,playlist_uri,isrc,report_date,track_uri,track_name,
    playlist_id,
    name,
    owner_id,
    owner_uri,
    followers,
    artist_uri,
    artist_name,
    album_uri,
    album_name,
    added_at,
    conopus_id,
    position
from
--get_user_products_trackers where report_date="2017-08-02"
get_user_products_trackers
where
--report_date=DATE_SUB((SELECT today FROM getToday), INTERVAL 1 DAY)
--report_date=(SELECT todayminusone FROM getTodayMinusOne)
pt=(SELECT todayminusone FROM getTodayMinusOne)
) b
on a.playlist_uri=b.playlist_uri and a.isrc=b.isrc and a.email=b.email
) c
where action_type <>"NOACTION"
),
get_top_playlists AS (
SELECT  source_uri as playlist_uri,count(1) as visits FROM `{streams}`
WHERE
_PARTITIONTIME >= TIMESTAMP_SUB((SELECT today FROM getToday), INTERVAL 14 DAY)
AND _PARTITIONTIME <(SELECT today FROM getToday)
AND length(source_uri)>0
GROUP BY 1
ORDER BY visits DESC
LIMIT 2000
),
get_top_playlists10000 AS (
SELECT  source_uri as playlist_uri,count(1) as visits FROM `{streams}`
WHERE
_PARTITIONTIME >= TIMESTAMP_SUB((SELECT today FROM getToday), INTERVAL 28 DAY)
AND _PARTITIONTIME <(SELECT today FROM getToday)
AND length(source_uri)>0
GROUP BY 1
ORDER BY visits DESC
LIMIT 10000
),
get_top_track_uris AS (
    SELECT partner_track_uri as track_uri,isrc,count(1) as visits FROM `{streams}`
    WHERE
        _PARTITIONTIME >=TIMESTAMP_SUB((SELECT today FROM getToday), INTERVAL 90 DAY)
        AND LENGTH(partner_track_uri)>0
    GROUP BY 1,2
    ORDER BY visits DESC
    -- LIMIT 100000
),
get_top_track_uris1 AS (
    SELECT partner_track_uri as track_uri,count(1) as visits FROM `{streams}`
    WHERE
        _PARTITIONTIME >=TIMESTAMP_SUB((SELECT today FROM getToday), INTERVAL 1500 DAY)
        AND LENGTH(partner_track_uri)>0
    GROUP BY 1
    ORDER BY visits
--    LIMIT 100000
),
get_user_products_trackers_details AS (
  SELECT c.*,
    case c.action_type
			when 'DROP' then c.report_date
			when 'ADD' then coalesce(TIMESTAMP(STRING(added_at)),c.report_date)
	  end as action_date
	  ,case c.action_type
			when 'DROP' then c.report_date
			when 'ADD' then coalesce(TIMESTAMP(STRING(added_at)),c.report_date)
	  end as action_timestamp,
	CASE WHEN f.country IS NULL THEN "Unknown" ELSE f.country END as country
  FROM getJOIN c
  JOIN get_top_playlists e
  ON c.playlist_uri=e.playlist_uri
  LEFT JOIN `umg-dev.swift_alerts.playlist_geography` f
  ON c.playlist_uri=f.playlist_uri
),
get_track_artist_image AS (
    SELECT
    track_uri,artist_uri,track_image,artist_image
    FROM
    --`{project}.swift_alerts.artist_track_images`
    get_canopus_isrc_track_image_artist_image
    GROUP BY 1,2,3,4
),
get_track_artist_image_fill AS (
    SELECT track_uri,artist_uri,track_image,artist_image FROM `{artist_track_images}`
    GROUP BY 1,2,3,4
  ),
get_details_with_artist_track_images2 AS (
  select a.*,b.artist_image,b.track_image from
  get_user_products_trackers_details a
  left join get_track_artist_image b
  ON a.track_uri=b.track_uri
),
fill_tracks_with_artist_track_images AS (
   select a.conopus_id as canopus_id,
   case when canopus.default_name is not null then canopus.default_name else a.artist_name end as artist_name,
   a.isrc,
   a.track_uri,
   a.artist_uri,
   b.artist_image
   from get_user_products_trackers_details a
   left join `{canopus_name}` canopus
      on a.conopus_id=canopus.canopus_id
   left join get_track_artist_image b
      ON a.track_uri=b.track_uri

   WHERE b.artist_image IS NULL
   GROUP BY 1,2,3,4,5,6
),
get_canopus_info AS (
 --SELECT canopus_id,isrc FROM  `umg-dev.swift_alerts.canopus_resource`
  --GROUP BY 1,2
  select resource.canopus_id,resource.isrc,name.default_name as default_name,name.artist_name as artist_name
      from `{canopus_resource}` resource
      join `{canopus_name}` name
      on resource.canopus_id=name.canopus_id
      GROUP BY 1,2,3,4
),
fill_all_tracks_with_artist_track_images AS (
     select canopus.canopus_id as canopus_id,
       case when canopus.default_name is not null then canopus.default_name else canopus.artist_name end as artist_name,
       --canopus.artist_name as various_artist_name,
       visits,
       a.isrc as isrc,
       a.track_uri as track_uri,
       b.artist_image as artist_image
     from get_top_track_uris a
     left join get_canopus_info canopus
     ON a.isrc=canopus.isrc
     left join get_track_artist_image_fill b
     ON a.track_uri=b.track_uri
     --join `umg-dev.swift_alerts.artist_image_duplication` c
     --on canopus.canopus_id=c.canopus_id
     WHERE b.artist_image IS NULL
     AND canopus.canopus_id IS NOT NULL
     AND LENGTH(canopus.artist_name)>0
     AND LENGTH(a.isrc)>0
     GROUP BY 1,2,3,4,5,6
     ORDER BY visits DESC
     LIMIT 200000
),
get_canopused_images AS (
     select canopus_id,
     isrc,
     track_uri,
     artist_name,
     --various_artist_name,
     --visits,
     artist_image
     from fill_all_tracks_with_artist_track_images
     --GROUP BY 1,2,3,4,5
),
count_lines_yesterday_before_yesterday AS (
    SELECT count(*)
    FROM `{playlist_track_history}`
    WHERE
    --_PARTITIONTIME=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 2 DAY))
    _PARTITIONTIME=(SELECT todayminustwo FROM getTodayMinusTwo)
    --GROUP BY 1
    UNION ALL
    SELECT count(*)
    FROM `{playlist_track_history}`
    WHERE
    --_PARTITIONTIME=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 1 DAY))
    _PARTITIONTIME=(SELECT todayminusone FROM getTodayMinusOne)
    --GROUP BY 1
),
getTrackCount as (
select track_uri,track_image,count(*) as tracks
from `{artist_track_images}`
group by 1,2
),
getTrackImage as (
select track_uri,track_image,max(tracks)
from getTrackCount
group by 1,2
),
getArtistCount2 as (
select canopus_id,artist_uri,artist_image,count(1) as artists
from `umg-dev.swift_alerts.artist_track_images`
group by 1,2,3
),
getArtistImage2 as (
    select a.* from getArtistCount2 a
    join
    (
    select canopus_id,max(artists) as max_images
        from getArtistCount2
        WHERE artist_image<>"various artists"
        GROUP BY 1
    ) b
    on a.canopus_id=b.canopus_id  and artists=max_images
    --where b.canopus_id=10020469
),
get_details_with_artist_track_images as (
select a.*,b.track_image,c.artist_image from get_user_products_trackers_details a
left join getTrackImage b
on a.track_uri=b.track_uri
--left join getArtistImage c
--on a.artist_uri=c.artist_uri
left join getArtistImage2 c
on a.conopus_id=c.canopus_id
)
----call
--select * from get_user_products_trackers_details