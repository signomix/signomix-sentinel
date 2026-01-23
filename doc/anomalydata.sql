-- Anomaly data for DKHSROOM206
SELECT
  time_bucket_gapfill('15 minutes', tstamp) as day,
  interpolate(avg(d1)) AS status,
  avg(d2) AS temperature,
  avg(d3) AS temp_target,
  avg(d6) AS temp_req
FROM
  analyticdata
WHERE eui='DKHSROOM206' 
AND tstamp>now() - interval '12 hours'
AND tstamp<now()
GROUP BY day
ORDER BY day DESC;

