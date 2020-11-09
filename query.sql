WITH
  first_open AS(
  SELECT
    user_pseudo_id,
    event_timestamp
  FROM
    `firebase-public-project.analytics_153293282.events_201807*`
  WHERE
    event_name='first_open' ),
  --------------------------------------------------------------------------
  conversion AS(
  SELECT
    user_pseudo_id,
    MIN(event_timestamp) event_timestamp,
    platform
  FROM
    `firebase-public-project.analytics_153293282.events_201807*`
  WHERE
    event_name='completed_5_levels'
  GROUP BY
    user_pseudo_id,
    platform),
  --------------------------------------------------------------------------
  tmp_tbl AS (
  SELECT
    conversion.user_pseudo_id usr_id,
    TIMESTAMP_DIFF(TIMESTAMP_MICROS(conversion.event_timestamp), TIMESTAMP_MICROS(first_open.event_timestamp), DAY) days_after_first_open,
    conversion.platform platform
  FROM ( first_open
    INNER JOIN
      conversion
    ON
      first_open.user_pseudo_id=conversion.user_pseudo_id ) ),
  --------------------------------------------------------------------------
  tmp_tbl2 AS (
  SELECT
    days_after_first_open,
    COUNT(days_after_first_open) total_conversions
  FROM
    tmp_tbl
  GROUP BY
    days_after_first_open
  ORDER BY
    total_conversions DESC
  LIMIT
    5),
  --------------------------------------------------------------------------
  tmp_tbl3 AS (
  SELECT
    platform,
    days_after_first_open,
    COUNT(days_after_first_open) total_conversions
  FROM
    tmp_tbl
  GROUP BY
    platform,
    days_after_first_open
  ORDER BY
    total_conversions DESC
  LIMIT
    100)
  ------------------------------------------------------------------------------
SELECT
  tmp_tbl2.days_after_first_open,
  tmp_tbl2.total_conversions,
  andr.total_conversions conversions_ANDROID,
  ios.total_conversions conversions_IOS
FROM
  tmp_tbl2
LEFT JOIN (
  SELECT
    *
  FROM
    tmp_tbl3
  WHERE
    platform='ANDROID') andr
ON
  tmp_tbl2.days_after_first_open=andr.days_after_first_open
LEFT JOIN (
  SELECT
    *
  FROM
    tmp_tbl3
  WHERE
    platform='IOS') ios
ON
  tmp_tbl2.days_after_first_open=ios.days_after_first_open
