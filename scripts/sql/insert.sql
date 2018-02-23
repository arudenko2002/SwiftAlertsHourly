WITH  getToday AS (
  --SELECT CURRENT_DATE()
  SELECT DATE("{ExecutionDate}") as today
),
 get_canopus_artist_image AS (
    select canopus_id,artist_uri,artist_image,count(1) as images
    from `umg-dev.swift_alerts.artist_track_images`
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
      from `umg-dev.swift_alerts.artist_track_images`
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
      select canopus_id, isrc, track_uri from `umg-dev.swift_alerts.artist_track_images`
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
    a.*,
    b.release_artist_id,b.isrc,b.product_title,
    c.playlist_uri,
    c.report_date,
    c.position,
    c.added_at,
    c.added_by,
    c.track_uri,
    c.track_name,
    --isrc AAA,
    c.artist_uri,
    c.artist_name,
    -- c.album_type,
    c.album_uri,
    c.album_name
  FROM
    (
    SELECT * FROM `{project}.swift_alerts.from_mongodb_users`
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
  JOIN
    (
    SELECT
    playlist_uri,
    playlist_date as report_date,
    --c.position,
    track_add_by as added_by,
    track_uri,
    track_title as track_name,
    isrc,
    artist_uri,
    track_artist as artist_name,
    -- album_type,
    album_uri,
    album_title as album_name,
    max(track_position) as position,
    max(load_timestamp) as added_at
    FROM
    `umg-partner.spotify.playlist_track_history`
    WHERE
    LENGTH(isrc)>0
    --AND _PARTITIONTIME>="2017-08-01 00:00:00"
    AND _PARTITIONTIME>=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 2 DAY))
    --AND _PARTITIONTIME<"2017-08-03 00:00:00"
    AND _PARTITIONTIME<TIMESTAMP((SELECT today FROM getToday))
    --AND playlist_uri="spotify:user:22z5y2tny4iq43jtouhrh76pi:playlist:0mZdLcVBcH6vax0TCmYQXL"
    GROUP BY 1,2,3,4,5,6,7,8,9,10
    ) c ON b.isrc=c.isrc
),
get_details AS (
  SELECT playlist_uri
  ,playlist_id
  ,playlist_owner as owner_id
  ,playlist_owner_uri as owner_uri
  --,type
  --,report_date
  ,playlist_name as name,
  max(playlist_date) as report_date,
  max(follower_count) as followers,
  count(playlist_uri),
  max(playlist_description) as description
  FROM `umg-partner.spotify.playlist_history`
  WHERE
  --_PARTITIONTIME>='2017-08-01 00:00:00'
  _PARTITIONTIME>=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 2 DAY))
  --AND _PARTITIONTIME<'2017-08-03 00:00:00'
  AND _PARTITIONTIME<TIMESTAMP((SELECT today FROM getToday))
  GROUP BY 1,2,3,4,5
  --,6
  --,8
),
getJOIN AS (
--select * from get_user_products_trackers_details  -- where report_date="2017-08-01";
SELECT d.*,c.action_type FROM (
SELECT
CASE WHEN (a.playlist_uri is not null) THEN a.playlist_uri ELSE b.playlist_uri END as playlist_uri,
CASE WHEN (a.isrc is not null) THEN a.isrc ELSE b.isrc END as isrc,
CASE WHEN (a.report_date is not null) THEN a.report_date ELSE b.report_date END as report_date,
CASE WHEN (a.email is not null) THEN a.email ELSE b.email END as email,
CASE WHEN (a.track_uri is not null) THEN a.track_uri ELSE b.track_uri END as track_uri,
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
SELECT email,playlist_uri,isrc,report_date,track_uri
from
--get_user_products_trackers where report_date="2017-08-01"
get_user_products_trackers
where
report_date=DATE_SUB((SELECT today FROM getToday), INTERVAL 2 DAY)
) a
full join
(
SELECT email,playlist_uri,isrc,report_date,track_uri
from
--get_user_products_trackers where report_date="2017-08-02"
get_user_products_trackers
where report_date=DATE_SUB((SELECT today FROM getToday), INTERVAL 1 DAY)
) b
on a.playlist_uri=b.playlist_uri and a.isrc=b.isrc and a.email=b.email
) c
join
get_user_products_trackers d
on c.playlist_uri=d.playlist_uri and c.isrc=d.isrc and c.report_date=d.report_date and c.email=d.email and c.track_uri=d.track_uri
where action_type <>"NOACTION"
),
get_top_playlists AS (
SELECT  source_uri as playlist_uri,count(1) as visits FROM `umg-partner.spotify.streams`
WHERE
_PARTITIONTIME >= TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 28 DAY))
AND _PARTITIONTIME <TIMESTAMP((SELECT today FROM getToday))
AND length(source_uri)>0
GROUP BY 1
ORDER BY visits DESC
LIMIT 2000
),
get_top_track_uris AS (
    SELECT partner_track_uri as track_uri,isrc,count(1) as visits FROM `umg-partner.spotify.streams`
    WHERE
        _PARTITIONTIME >=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 90 DAY))
        AND LENGTH(partner_track_uri)>0
    GROUP BY 1,2
    ORDER BY visits
    -- LIMIT 100000
),
get_top_track_uris1 AS (
    SELECT partner_track_uri as track_uri,count(1) as visits FROM `umg-partner.spotify.streams`
    WHERE
        _PARTITIONTIME >=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 1500 DAY))
        AND LENGTH(partner_track_uri)>0
    GROUP BY 1
    ORDER BY visits
--    LIMIT 100000
),
-- PROJECTIONS ARE TEMP. DOSCONNECTED
--get_projections AS (
--    SELECT playlist_uri,track_uri,sum(streams_total) as streams,sum(streams_position_predicted) as estimated_streams
--    FROM `umg-data-science.projections.playlist_track_projections`
--    WHERE
--    _PARTITIONTIME >=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 28 DAY))
--    and
--    _PARTITIONTIME < TIMESTAMP((SELECT today FROM getToday))
--    GROUP BY 1,2
--),
get_user_products_trackers_details AS (
  SELECT c.*,
    d.playlist_id,
    d.name,
    d.description,
    d.owner_id,
    d.owner_uri,
    d.followers
    ,case c.action_type
			when 'DROP' then c.report_date
			when 'ADD' then coalesce(DATE(added_at),c.report_date)
	end as action_date
	,case c.action_type
			when 'DROP' then TIMESTAMP(c.report_date)
			when 'ADD' then coalesce(added_at,TIMESTAMP(c.report_date))
	end as action_timestamp,
--	g.streams,
--	g.estimated_streams,
	CASE WHEN f.country IS NULL THEN "Unknown" ELSE f.country END as country
	--"USA" as country
  --FROM get_user_products_trackers c
  FROM getJOIN c
  JOIN get_details d
  ON c.playlist_uri=d.playlist_uri
  --AND c.report_date=d.report_date
  --  TODO Find the way around here.  Seems like it causes problems either way: commented out or not commented
  JOIN get_top_playlists e
  ON c.playlist_uri=e.playlist_uri
  LEFT JOIN `umg-dev.swift_alerts.playlist_geography` f
  ON c.playlist_uri=f.playlist_uri
--  LEFT JOIN get_projections g
--    ON c.playlist_uri=g.playlist_uri
--    AND c.track_uri=g.track_uri
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
    SELECT track_uri,artist_uri,track_image,artist_image FROM `{project}.swift_alerts.artist_track_images`
    GROUP BY 1,2,3,4
  ),
get_details_with_artist_track_images AS (
  select a.*,b.artist_image,b.track_image from
  get_user_products_trackers_details a
  left join get_track_artist_image b
  ON a.track_uri=b.track_uri
),
fill_tracks_with_artist_track_images AS (
   select a.conopus_id as canopus_id,a.isrc,a.track_uri,a.artist_uri,b.artist_image from
   get_user_products_trackers_details a
   left join get_track_artist_image b
   ON a.track_uri=b.track_uri

   WHERE b.artist_image IS NULL
   GROUP BY 1,2,3,4,5
),
get_canopus_info AS (
  SELECT canopus_id,isrc FROM  `umg-dev.swift_alerts.canopus_resource`
  GROUP BY 1,2
),
fill_all_tracks_with_artist_track_images AS (
     select canopus.canopus_id as canopus_id,a.isrc as isrc,a.track_uri as track_uri,b.artist_image as artist_image
     from get_top_track_uris a
     left join get_canopus_info canopus
     ON a.isrc=canopus.isrc
     left join get_track_artist_image_fill b
     ON a.track_uri=b.track_uri
     WHERE b.artist_image IS NULL
     AND canopus_id IS NOT NULL
     AND LENGTH(a.isrc)>0
     GROUP BY 1,2,3,4
     LIMIT 100000
),
get_canopused_images AS (
     select canopus_id,
     isrc,
     track_uri,
     artist_image
     from fill_all_tracks_with_artist_track_images
     GROUP BY 1,2,3,4
),
count_lines_yesterday_before_yesterday AS (
    SELECT count(*)
    FROM `umg-partner.spotify.playlist_track_history`
    WHERE _PARTITIONTIME=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 2 DAY))
    --GROUP BY 1
    UNION ALL
    SELECT count(*)
    FROM `umg-partner.spotify.playlist_track_history`
    WHERE _PARTITIONTIME=TIMESTAMP(DATE_SUB((SELECT today FROM getToday), INTERVAL 1 DAY))
    --GROUP BY 1
)
----call
--select * from get_user_products_trackers_details