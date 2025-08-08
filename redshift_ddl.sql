-- Dimensions
CREATE TABLE IF NOT EXISTS dim_customer (
  customer_id BIGINT PRIMARY KEY,
  email VARCHAR(255),
  region VARCHAR(16),
  is_vip BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_product_scd2 (
  product_id BIGINT,
  category VARCHAR(64),
  unit_cost DECIMAL(12,2),
  is_current BOOLEAN,
  effective_date TIMESTAMP,
  end_date TIMESTAMP
) DISTSTYLE KEY DISTKEY(product_id) SORTKEY(product_id, effective_date);

-- Facts
CREATE TABLE IF NOT EXISTS fact_sales (
  order_id BIGINT,
  order_date DATE,
  customer_id BIGINT REFERENCES dim_customer(customer_id),
  product_id BIGINT REFERENCES dim_product_scd2(product_id),
  quantity INT,
  sales_amount DECIMAL(12,2),
  order_year INT,
  order_qtr INT
) DISTSTYLE KEY DISTKEY(order_id) SORTKEY(order_date);
