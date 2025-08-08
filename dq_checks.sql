-- Negative checks
SELECT COUNT(*) AS neg_qty FROM fact_sales WHERE quantity < 0;
SELECT COUNT(*) AS neg_amt FROM fact_sales WHERE sales_amount < 0;

-- Orphans
SELECT COUNT(*) AS orphan_cust
FROM fact_sales f
LEFT JOIN dim_customer d ON f.customer_id=d.customer_id
WHERE d.customer_id IS NULL;

SELECT COUNT(*) AS orphan_prod
FROM fact_sales f
LEFT JOIN dim_product_scd2 p ON f.product_id=p.product_id
WHERE p.product_id IS NULL;

-- Aggregates
SELECT order_year, SUM(sales_amount) AS sales
FROM fact_sales
GROUP BY order_year
ORDER BY order_year;
