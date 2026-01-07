/* ============================================================
   7. QUERIES (Car Market)
   Schema: car_market
   Data sources: normalized tables + partitioned fact table
   ============================================================ */

SET search_path TO car_market;


/* ============================================================
   Q1) Full listing overview (JOINs across multiple tables)
   Business insight: Build a dashboard-ready dataset that merges
   key attributes (price/year/make/specs/appearance/status).
   Techniques: INNER JOIN + LEFT JOIN, filtering, LIMIT
   ============================================================ */
   
SELECT
  c.listing_id,
  c.url,
  v.make,
  v.model,
  p.year,
  p.price,
  p.mileage,
  s.engine_type,
  s.transmission,
  a.body_type,
  a.color,
  st.cleared_customs,
  st.condition
FROM core AS c
JOIN pricing AS p
  ON p.listing_id = c.listing_id
LEFT JOIN vehicle AS v
  ON v.listing_id = c.listing_id
LEFT JOIN specs AS s
  ON s.listing_id = c.listing_id
LEFT JOIN appearance AS a
  ON a.listing_id = c.listing_id
LEFT JOIN status AS st
  ON st.listing_id = c.listing_id
WHERE p.price IS NOT NULL
ORDER BY p.price DESC
LIMIT 20;


/* ============================================================
   Q2) Top makes by listing count and average price (Aggregations)
   Business insight: Identify the most common brands and their
   typical price level to understand supply and market positioning.
   Techniques: GROUP BY, COUNT, AVG, HAVING, ORDER BY
   ============================================================ */
   
SELECT
  v.make,
  COUNT(*) AS listings,
  ROUND(AVG(p.price), 2) AS avg_price,
  MIN(p.price) AS min_price,
  MAX(p.price) AS max_price
FROM vehicle AS v
JOIN pricing AS p
  ON p.listing_id = v.listing_id
WHERE p.price IS NOT NULL
GROUP BY v.make
HAVING COUNT(*) >= 100
ORDER BY listings DESC, avg_price DESC
LIMIT 15;


/* ============================================================
   Q3) Price distribution buckets by year (CASE WHEN + GROUP BY)
   Business insight: Understand how listing prices are distributed
   across years (affordability segments).
   Techniques: CASE WHEN, GROUP BY, ORDER BY
   ============================================================ */
   
SELECT
  p.year,
  CASE
    WHEN p.price < 5000 THEN 'under_5k'
    WHEN p.price < 10000 THEN '5k_10k'
    WHEN p.price < 20000 THEN '10k_20k'
    WHEN p.price < 40000 THEN '20k_40k'
    ELSE '40k_plus'
  END AS price_bucket,
  COUNT(*) AS listings
FROM pricing AS p
WHERE p.price IS NOT NULL AND p.year IS NOT NULL
GROUP BY p.year, price_bucket
ORDER BY p.year, price_bucket;


/* ============================================================
   Q4) Advanced filtering examples (BETWEEN, IN, ILIKE)
   Business insight: Find "hot" segment: common makes within
   a year range and a price range.
   Techniques: BETWEEN, IN, ILIKE, ORDER BY, OFFSET
   ============================================================ */
   
SELECT
  v.make,
  v.model,
  p.year,
  p.price,
  p.mileage
FROM vehicle AS v
JOIN pricing AS p
  ON p.listing_id = v.listing_id
WHERE p.year BETWEEN 2015 AND 2020
  AND p.price BETWEEN 8000 AND 20000
  AND v.make IN ('Toyota', 'Hyundai', 'Kia', 'BMW', 'Mercedes-Benz')
  AND (v.model ILIKE '%corolla%' OR v.model ILIKE '%elantra%' OR v.model ILIKE '%rio%')
ORDER BY p.price ASC
OFFSET 0
LIMIT 50;


/* ============================================================
   Q5) NULL handling and cleaning logic check (COALESCE, NULLIF)
   Business insight: Create robust metrics when mileage is missing
   and avoid division-by-zero problems.
   Techniques: COALESCE, NULLIF
   ============================================================ */
   
SELECT
  v.make,
  COUNT(*) AS listings,
  ROUND(AVG(COALESCE(p.mileage, 0)), 2) AS avg_mileage_assuming_zero_if_null,
  ROUND(AVG(p.mileage), 2) AS avg_mileage_ignore_nulls,
  ROUND(AVG(p.price / NULLIF(p.mileage, 0)), 2) AS avg_price_per_km_excluding_zero_mileage
FROM vehicle AS v
JOIN pricing AS p
  ON p.listing_id = v.listing_id
WHERE p.price IS NOT NULL
GROUP BY v.make
HAVING COUNT(*) >= 200
ORDER BY listings DESC
LIMIT 10;


/* ============================================================
   Q6) Window functions: rank top expensive listings per make
   Business insight: For each make, identify top 3 most expensive
   models currently on the market.
   Techniques: ROW_NUMBER window function, PARTITION BY
   ============================================================ */
   
WITH ranked AS (
  SELECT
    v.make,
    v.model,
    p.year,
    p.price,
    ROW_NUMBER() OVER (
      PARTITION BY v.make
      ORDER BY p.price DESC NULLS LAST
    ) AS rn
  FROM vehicle AS v
  JOIN pricing AS p
    ON p.listing_id = v.listing_id
  WHERE p.price IS NOT NULL
)
SELECT
  make, model, year, price
FROM ranked
WHERE rn <= 3
ORDER BY make, price DESC;


/* ============================================================
   Q7) Window functions: year-to-year trend per make (LAG)
   Business insight: Detect price trend changes for each make by year.
   Techniques: CTE, AVG, LAG, ORDER BY
   ============================================================ */
   
WITH make_year AS (
  SELECT
    v.make,
    p.year,
    ROUND(AVG(p.price), 2) AS avg_price,
    COUNT(*) AS listings
  FROM vehicle AS v
  JOIN pricing AS p
    ON p.listing_id = v.listing_id
  WHERE p.price IS NOT NULL AND p.year IS NOT NULL
  GROUP BY v.make, p.year
  HAVING COUNT(*) >= 20
),
trend AS (
  SELECT
    make,
    year,
    avg_price,
    ROUND(LAG(avg_price) OVER (PARTITION BY make ORDER BY year), 2) AS prev_avg_price
  FROM make_year
)
SELECT
  make,
  year,
  avg_price,
  prev_avg_price,
  ROUND((avg_price - prev_avg_price), 2) AS delta_vs_prev_year
FROM trend
WHERE prev_avg_price IS NOT NULL
ORDER BY make, year;


/* ============================================================
   Q8) Subquery: makes with above-market average price
   Business insight: Identify premium brands relative to overall market.
   Techniques: scalar subquery, AVG
   ============================================================ */
   
SELECT
  v.make,
  ROUND(AVG(p.price), 2) AS make_avg_price
FROM vehicle AS v
JOIN pricing AS p
  ON p.listing_id = v.listing_id
WHERE p.price IS NOT NULL
GROUP BY v.make
HAVING AVG(p.price) > (
  SELECT AVG(price) FROM pricing WHERE price IS NOT NULL
)
ORDER BY make_avg_price DESC
LIMIT 15;


/* ============================================================
   Q9) Date/time functions (demonstration)
   Business insight: For reporting dashboards, use current timestamp
   and extract parts (useful for refresh metadata).
   Techniques: NOW(), EXTRACT, DATE_TRUNC
   ============================================================ */
   
SELECT
  NOW() AS refreshed_at,
  DATE_TRUNC('day', NOW()) AS refreshed_day,
  EXTRACT(YEAR FROM NOW()) AS current_year,
  EXTRACT(MONTH FROM NOW()) AS current_month;


/* ============================================================
   Q10) Partition query #1 (benefits from year partitioning)
   Business insight: Average price for a specific year range.
   Techniques: Partition pruning, WHERE year BETWEEN
   Best practice: EXPLAIN ANALYZE
   ============================================================ */
   
EXPLAIN ANALYZE
SELECT
  make,
  AVG(price) AS avg_price,
  COUNT(*) AS listings
FROM listing_fact_part
WHERE year BETWEEN 2015 AND 2019
  AND price IS NOT NULL
GROUP BY make
ORDER BY avg_price DESC
LIMIT 10;


/* ============================================================
   Q11) Partition query #2 (more selective range)
   Business insight: Compare mid-range prices for newer cars only.
   Techniques: Partition pruning + filtering + ORDER BY
   Best practice: EXPLAIN ANALYZE
   ============================================================ */
EXPLAIN ANALYZE
SELECT
  COUNT(*) AS listings,
  AVG(price) AS avg_price
FROM listing_fact_part
WHERE year BETWEEN 2020 AND 2022
  AND price BETWEEN 8000 AND 20000;


