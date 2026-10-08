CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS AUX;

CREATE OR REPLACE TABLE STAGING.GROUPS_CLEAN AS
SELECT
  TRY_TO_NUMBER(NULLIF(TRIM(g.group_id), ''))::NUMBER(38, 0) AS group_id,
  TRY_TO_NUMBER(NULLIF(TRIM(g.category_id), ''))::NUMBER(38, 0) AS category_id,
  COALESCE(NULLIF(TRIM(c.category_name), ''), NULLIF(TRIM(g."category.name"), ''))
    AS category_name,
  TRY_TO_NUMBER(NULLIF(TRIM(g.city_id), ''))::NUMBER(38, 0) AS city_id,
  COALESCE(NULLIF(TRIM(ci.city), ''), NULLIF(TRIM(g.city), '')) AS city_name,
  COALESCE(NULLIF(TRIM(ci.country), ''), NULLIF(TRIM(g.country), ''))
    AS country_code,
  COALESCE(NULLIF(TRIM(ci.state), ''), NULLIF(TRIM(g.state), ''))
    AS state_code,
  TRY_TO_TIMESTAMP_NTZ(NULLIF(TRIM(g.created), ''), 'YYYY-MM-DD HH24:MI:SS')
    AS created_at,
  TRY_TO_NUMBER(NULLIF(TRIM(g.members), ''))::NUMBER(38, 0) AS member_count,
  TRY_TO_DECIMAL(NULLIF(TRIM(g.rating), ''), 5, 2) AS rating,
  TRY_TO_DECIMAL(NULLIF(TRIM(g.lat), ''), 12, 8) AS latitude,
  TRY_TO_DECIMAL(NULLIF(TRIM(g.lon), ''), 12, 8) AS longitude,
  NULLIF(TRIM(g.group_name), '') AS group_name,
  NULLIF(TRIM(g.urlname), '') AS url_name
FROM RAW_DATA.GROUPS AS g
LEFT JOIN RAW_DATA.CATEGORIES AS c
  ON TRY_TO_NUMBER(NULLIF(TRIM(c.category_id), ''))
   = TRY_TO_NUMBER(NULLIF(TRIM(g.category_id), ''))
LEFT JOIN RAW_DATA.CITIES AS ci
  ON TRY_TO_NUMBER(NULLIF(TRIM(ci.city_id), ''))
   = TRY_TO_NUMBER(NULLIF(TRIM(g.city_id), ''))
WHERE TRY_TO_NUMBER(NULLIF(TRIM(g.group_id), '')) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY TRY_TO_NUMBER(NULLIF(TRIM(g.group_id), ''))
  ORDER BY TRY_TO_TIMESTAMP_NTZ(
    NULLIF(TRIM(g.created), ''),
    'YYYY-MM-DD HH24:MI:SS'
  ) DESC NULLS LAST
) = 1;

CREATE OR REPLACE TABLE STAGING.EVENTS_CLEAN AS
SELECT
  NULLIF(TRIM(event_id), '') AS event_id,
  TRY_TO_NUMBER(NULLIF(TRIM(group_id), ''))::NUMBER(38, 0) AS group_id,
  TRY_TO_TIMESTAMP_NTZ(NULLIF(TRIM(created), ''), 'YYYY-MM-DD HH24:MI:SS')
    AS created_at,
  TRY_TO_TIMESTAMP_NTZ(NULLIF(TRIM(event_time), ''), 'YYYY-MM-DD HH24:MI:SS')
    AS event_at,
  TRY_TO_TIMESTAMP_NTZ(NULLIF(TRIM(updated), ''), 'YYYY-MM-DD HH24:MI:SS')
    AS updated_at,
  NULLIF(TRIM(event_status), '') AS event_status,
  TRY_TO_NUMBER(NULLIF(TRIM(yes_rsvp_count), ''))::NUMBER(38, 0)
    AS yes_rsvp_count,
  TRY_TO_NUMBER(NULLIF(TRIM(maybe_rsvp_count), ''))::NUMBER(38, 0)
    AS maybe_rsvp_count,
  TRY_TO_DECIMAL(NULLIF(TRIM("rating.average"), ''), 5, 2) AS rating_average,
  TRY_TO_NUMBER(NULLIF(TRIM("rating.count"), ''))::NUMBER(38, 0)
    AS rating_count,
  NULLIF(TRIM(event_name), '') AS event_name
FROM RAW_DATA.EVENTS
WHERE NULLIF(TRIM(event_id), '') IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY NULLIF(TRIM(event_id), '')
  ORDER BY TRY_TO_TIMESTAMP_NTZ(
    NULLIF(TRIM(updated), ''),
    'YYYY-MM-DD HH24:MI:SS'
  ) DESC NULLS LAST
) = 1;

CREATE OR REPLACE TABLE AUX.GROUPS_BY_CITY_CATEGORY AS
WITH event_metrics AS (
  SELECT
    group_id,
    COUNT(*)::NUMBER(38, 0) AS event_count,
    SUM(COALESCE(yes_rsvp_count, 0))::NUMBER(38, 0)
      AS yes_rsvp_across_events,
    MAX(event_at) AS latest_event_at
  FROM STAGING.EVENTS_CLEAN
  WHERE group_id IS NOT NULL
  GROUP BY group_id
)
SELECT
  g.city_id,
  g.city_name,
  g.country_code,
  g.state_code,
  g.category_id,
  g.category_name,
  COUNT(*)::NUMBER(38, 0) AS group_count,
  SUM(COALESCE(g.member_count, 0))::NUMBER(38, 0)
    AS members_across_groups,
  ROUND(AVG(g.rating), 2)::NUMBER(5, 2) AS average_group_rating,
  SUM(COALESCE(e.event_count, 0))::NUMBER(38, 0) AS event_count,
  SUM(COALESCE(e.yes_rsvp_across_events, 0))::NUMBER(38, 0)
    AS yes_rsvp_across_events,
  MAX(e.latest_event_at) AS latest_event_at,
  CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS refreshed_at
FROM STAGING.GROUPS_CLEAN AS g
LEFT JOIN event_metrics AS e
  ON g.group_id = e.group_id
GROUP BY
  g.city_id,
  g.city_name,
  g.country_code,
  g.state_code,
  g.category_id,
  g.category_name;

SELECT 'STAGING.GROUPS_CLEAN' AS table_name, COUNT(*) AS row_count
FROM STAGING.GROUPS_CLEAN
UNION ALL
SELECT 'STAGING.EVENTS_CLEAN', COUNT(*) FROM STAGING.EVENTS_CLEAN
UNION ALL
SELECT 'AUX.GROUPS_BY_CITY_CATEGORY', COUNT(*) FROM AUX.GROUPS_BY_CITY_CATEGORY;
