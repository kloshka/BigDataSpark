CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.report_sales_products (
    metric_type String,
    product_id Nullable(Int32),
    product_name Nullable(String),
    product_category Nullable(String),
    value_1 Float64,
    value_2 Float64,
    value_3 Float64,
    report_date Date
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_sales_customers (
    metric_type String,
    customer_id Nullable(Int32),
    customer_name Nullable(String),
    country Nullable(String),
    value_1 Float64,
    value_2 Float64,
    report_date Date
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_sales_time (
    metric_type String,
    period String,
    year Nullable(Int32),
    month Nullable(Int32),
    value_1 Float64,
    value_2 Float64,
    report_date Date
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_sales_stores (
    metric_type String,
    store_id Nullable(Int32),
    store_name Nullable(String),
    city Nullable(String),
    country Nullable(String),
    value_1 Float64,
    value_2 Float64,
    report_date Date
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_sales_suppliers (
    metric_type String,
    supplier_id Nullable(Int32),
    supplier_name Nullable(String),
    supplier_country Nullable(String),
    value_1 Float64,
    value_2 Float64,
    report_date Date
) ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS analytics.report_product_quality (
    metric_type String,
    product_id Nullable(Int32),
    product_name Nullable(String),
    value_1 Float64,
    value_2 Float64,
    report_date Date
) ENGINE = MergeTree()
ORDER BY tuple();
