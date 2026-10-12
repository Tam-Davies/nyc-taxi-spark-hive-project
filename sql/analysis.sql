-- 1. Total trips and revenue
SELECT
    SUM(total_trips) AS total_trips,
    ROUND(SUM(total_revenue), 2) AS total_revenue
FROM taxi.monthly_summary;

-- 2. Monthly performance
SELECT
    pickup_month,
    SUM(total_trips) AS total_trips,
    ROUND(SUM(total_revenue), 2) AS total_revenue
FROM taxi.monthly_summary
GROUP BY pickup_month
ORDER BY pickup_month;

-- 3. Vendor comparison
SELECT
    VendorID,
    SUM(total_trips) AS total_trips,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    ROUND(AVG(avg_fare), 2) AS avg_fare
FROM taxi.monthly_summary
GROUP BY VendorID
ORDER BY total_trips DESC;

-- 4. Highest-revenue month
SELECT
    pickup_month,
    ROUND(SUM(total_revenue), 2) AS total_revenue
FROM taxi.monthly_summary
GROUP BY pickup_month
ORDER BY total_revenue DESC
LIMIT 1;