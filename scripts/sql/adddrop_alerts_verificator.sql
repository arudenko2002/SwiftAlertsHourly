WITH AAA AS (
    SELECT _PARTITIONTIME as pt, EXTRACT(DAY FROM _PARTITIONTIME) as days, count(*) FROM `umg-dev.swift_alerts.playlist_track_action_hourly` WHERE _PARTITIONTIME >= "1700-01-14 00:00:00"
    group by 1,2
    order by days,pt
    LIMIT 1000
),
BBB AS (
    select _PARTITIONTIME as pt, EXTRACT(DAY FROM _PARTITIONTIME) as days,count(*) from `umg-dev.swift_alerts.spotify_track_history_details_hourly`
    where _PARTITIONTIME = "1814-01-23 00:00:00"
    group by 1,2
    order by days,pt
    limit 1000
),
CCC AS (
    SELECT _PARTITIONTIME as pt, EXTRACT(DAY FROM _PARTITIONTIME) as days, count(*) FROM `umg-dev.swift_alerts.playlist_track_action` WHERE _PARTITIONTIME >= "1700-01-14 00:00:00"
    group by 1,2
    order by days,pt
    LIMIT 1000
),
DDD AS (
    select _PARTITIONTIME as pt, EXTRACT(DAY FROM _PARTITIONTIME) as days,count(*) from `umg-dev.swift_alerts.spotify_track_history_details`
    where _PARTITIONTIME = "1814-01-23 00:00:00"
    group by 1,2
    order by days,pt
    limit 1000
)

SELECT * FROM DDD