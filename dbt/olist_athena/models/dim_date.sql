-- No depende de ninguna tabla: se genera sola. Debe dar 1,461 filas (2016-01-01 a 2019-12-31).
-- sequence() en Athena devuelve timestamps; se castea a date para que el CTAS a Parquet funcione.
WITH days AS (
    SELECT CAST(d AS date) AS d
    FROM UNNEST(sequence(DATE '2016-01-01', DATE '2019-12-31', INTERVAL '1' DAY)) AS t(d)
)

SELECT
    CAST(date_format(d, '%Y%m%d') AS integer) AS date_key,
    d                                          AS full_date,
    year(d)                                    AS year,
    quarter(d)                                 AS quarter,
    month(d)                                   AS month,
    day(d)                                     AS day,
    week(d)                                    AS week_of_year,   -- semana ISO, igual que EXTRACT(week) en Postgres
    day_of_week(d)                             AS day_of_week,    -- 1 = lunes ... 7 = domingo
    date_format(d, '%W')                       AS day_name,
    day_of_week(d) IN (6, 7)                   AS is_weekend
FROM days