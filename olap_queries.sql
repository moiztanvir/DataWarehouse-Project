USE metro_dw;
-- Q1. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
SELECT p.PRODUCT_NAME, SUM(ft.SALE) AS TOTAL_SALES
    FROM FACT_TRANSACTIONS ft
    JOIN PRODUCTS p ON ft.PRODUCT_ID = p.PRODUCT_ID
    WHERE YEAR(ft.ORDER_DATE) = 2019
    GROUP BY ft.PRODUCT_ID, p.PRODUCT_NAME
    ORDER BY TOTAL_SALES DESC
    LIMIT 5;


-- Q2. Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
WITH QuarterlySales AS (
    SELECT
        EXTRACT(YEAR FROM ORDER_DATE) * 100 + QUARTER(ORDER_DATE) AS sales_quarter, -- YYYYQ format
        STORE_NAME,
        SUM(SALE) AS total_revenue
    FROM FACT_TRANSACTIONS
    WHERE EXTRACT(YEAR FROM ORDER_DATE) = 2017
    GROUP BY 1, 2
),
RankedQuarterlySales AS (
  SELECT
    sales_quarter,
    STORE_NAME,
    total_revenue,
    LAG(total_revenue, 1, 0) OVER (PARTITION BY STORE_NAME ORDER BY sales_quarter) AS previous_quarter_revenue
  FROM QuarterlySales
)
SELECT
    sales_quarter,
    STORE_NAME,
    total_revenue,
    (total_revenue - previous_quarter_revenue) * 100.0 / previous_quarter_revenue AS growth_rate
FROM RankedQuarterlySales
WHERE previous_quarter_revenue <> 0
ORDER BY sales_quarter, STORE_NAME;


-- Q3. Detailed Supplier Sales Contribution by Store and Product Name (No change needed)
SELECT
    STORE_NAME,
    SUPPLIER_NAME,
    PRODUCT_NAME,
    SUM(SALE) AS total_sales
FROM FACT_TRANSACTIONS
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;


-- Q4. Seasonal Analysis of Product Sales Using Dynamic Drill-Down
WITH SeasonalSales AS (
    SELECT
        PRODUCT_NAME,
        CASE
            WHEN EXTRACT(MONTH FROM ORDER_DATE) BETWEEN 3 AND 5 THEN 'Spring'
            WHEN EXTRACT(MONTH FROM ORDER_DATE) BETWEEN 6 AND 8 THEN 'Summer'
            WHEN EXTRACT(MONTH FROM ORDER_DATE) BETWEEN 9 AND 11 THEN 'Fall'
            ELSE 'Winter'
        END AS season,
        SUM(SALE) AS total_sales
    FROM FACT_TRANSACTIONS
    GROUP BY 1, 2
)
SELECT
    PRODUCT_NAME,
    season,
    total_sales
FROM SeasonalSales
ORDER BY PRODUCT_NAME, season;


-- Q5. Store-Wise and Supplier-Wise Monthly Revenue Volatility
WITH MonthlyRevenue AS (
    SELECT
        EXTRACT(YEAR FROM ORDER_DATE) * 100 + EXTRACT(MONTH FROM ORDER_DATE) AS sales_month, -- YYYYMM format
        STORE_NAME,
        SUPPLIER_NAME,
        SUM(SALE) AS monthly_revenue
    FROM FACT_TRANSACTIONS
    GROUP BY 1, 2, 3
),
LaggedRevenue AS (
    SELECT
        sales_month,
        STORE_NAME,
        SUPPLIER_NAME,
        monthly_revenue,
        LAG(monthly_revenue, 1, 0) OVER (PARTITION BY STORE_NAME, SUPPLIER_NAME ORDER BY sales_month) AS previous_month_revenue
    FROM MonthlyRevenue
)
SELECT
    sales_month,
    STORE_NAME,
    SUPPLIER_NAME,
    monthly_revenue,
    (monthly_revenue - previous_month_revenue) * 100.0 / previous_month_revenue AS volatility
FROM LaggedRevenue
WHERE previous_month_revenue <> 0
ORDER BY STORE_NAME, SUPPLIER_NAME, sales_month;


-- Q6. Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)  (No change needed, still simplified)
WITH ProductPairs AS (
  SELECT
    ORDER_ID,
    GROUP_CONCAT(PRODUCT_NAME, ', ') AS product_combination
  FROM FACT_TRANSACTIONS
  GROUP BY ORDER_ID
  HAVING COUNT(*) > 1
),
PairCounts AS (
  SELECT
    product_combination,
    COUNT(*) AS pair_count
  FROM ProductPairs
  GROUP BY product_combination
)
SELECT
  product_combination,
  pair_count
FROM PairCounts
ORDER BY pair_count DESC
LIMIT 5;


-- Q7. Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
SELECT
    STORE_NAME,
    SUPPLIER_NAME,
    PRODUCT_NAME,
    SUM(SALE) AS total_revenue
FROM FACT_TRANSACTIONS
WHERE EXTRACT(YEAR FROM ORDER_DATE) = 2019 -- Replace '2023' with desired year
GROUP BY STORE_NAME, SUPPLIER_NAME, PRODUCT_NAME WITH ROLLUP
ORDER BY STORE_NAME, SUPPLIER_NAME, PRODUCT_NAME;


-- Q8. Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
WITH HalfYearlySales AS (
    SELECT
        PRODUCT_NAME,
        CASE
            WHEN EXTRACT(MONTH FROM ORDER_DATE) BETWEEN 1 AND 6 THEN 'H1'
            ELSE 'H2'
        END AS sales_half,
        SUM(SALE) AS total_revenue,
        SUM(QUANTITY) AS total_quantity
    FROM FACT_TRANSACTIONS
    GROUP BY 1, 2
),
YearlySales AS (
  SELECT
    PRODUCT_NAME,
    SUM(SALE) AS total_yearly_revenue,
    SUM(QUANTITY) AS total_yearly_quantity
  FROM FACT_TRANSACTIONS
  GROUP BY 1
)
SELECT
    hs.PRODUCT_NAME,
    hs.sales_half,
    hs.total_revenue,
    hs.total_quantity,
    ys.total_yearly_revenue,
    ys.total_yearly_quantity
FROM HalfYearlySales hs
JOIN YearlySales ys ON hs.PRODUCT_NAME = ys.PRODUCT_NAME
ORDER BY hs.PRODUCT_NAME, hs.sales_half;


-- Q9. Identify High Revenue Spikes in Product Sales and Highlight Outliers (No change needed)
WITH DailySales AS (
    SELECT
        PRODUCT_NAME,
        ORDER_DATE,
        SUM(SALE) AS daily_revenue
    FROM FACT_TRANSACTIONS
    GROUP BY 1, 2
),
AverageDailySales AS (
    SELECT
        PRODUCT_NAME,
        AVG(daily_revenue) AS avg_daily_revenue
    FROM DailySales
    GROUP BY 1
)
SELECT
    ds.PRODUCT_NAME,
    ds.ORDER_DATE,
    ds.daily_revenue,
    ads.avg_daily_revenue,
    CASE WHEN ds.daily_revenue > 2 * ads.avg_daily_revenue THEN 'Outlier' ELSE 'Normal' END AS outlier_flag
FROM DailySales ds
JOIN AverageDailySales ads ON ds.PRODUCT_NAME = ads.PRODUCT_NAME
ORDER BY ds.PRODUCT_NAME, ds.ORDER_DATE;


-- Q10. Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT
    STORE_NAME,
    EXTRACT(YEAR FROM ORDER_DATE) * 100 + QUARTER(ORDER_DATE) AS sales_quarter, -- YYYYQ format
    SUM(SALE) AS total_quarterly_sales
FROM FACT_TRANSACTIONS
GROUP BY 1, 2
ORDER BY 1;