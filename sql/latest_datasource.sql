SELECT
    region,
    datasource,
    MAX(datetime) as latest_datetime
FROM
    raw_tripsdata
GROUP BY
        region, datasource
HAVING region IN (
  SELECT region
  FROM raw_tripsdata
  GROUP BY region
  ORDER BY COUNT(*) DESC
  LIMIT 2
)
ORDER BY COUNT(*) DESC, latest_datetime DESC
LIMIT 1
