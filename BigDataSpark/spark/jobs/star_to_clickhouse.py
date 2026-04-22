from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


POSTGRES_URL = "jdbc:postgresql://postgres:5432/bds2"
POSTGRES_PROPS = {
    "user": "bds_user",
    "password": "bds_pass",
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/analytics"
CLICKHOUSE_PROPS = {
    "user": "ch_user",
    "password": "ch_pass",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


def read_pg(spark: SparkSession, table_name: str):
    return spark.read.jdbc(
        url=POSTGRES_URL,
        table=table_name,
        properties=POSTGRES_PROPS,
    )


def write_clickhouse(df, table_name: str) -> None:
    fill_map = {}
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            fill_map[field.name] = ""
        elif isinstance(field.dataType, (T.IntegerType, T.LongType, T.ShortType, T.ByteType)):
            fill_map[field.name] = 0
        elif isinstance(field.dataType, (T.DoubleType, T.FloatType, T.DecimalType)):
            fill_map[field.name] = 0.0

    prepared_df = df.fillna(fill_map) if fill_map else df

    (
        prepared_df.write.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("dbtable", table_name)
        .option("user", CLICKHOUSE_PROPS["user"])
        .option("password", CLICKHOUSE_PROPS["password"])
        .option("driver", CLICKHOUSE_PROPS["driver"])
        .mode("append")
        .save()
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("bds2-star-to-clickhouse")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    fact = read_pg(spark, "dwh.fact_sales").alias("f")
    dim_product = read_pg(spark, "dwh.dim_product").alias("p")
    dim_customer = read_pg(spark, "dwh.dim_customer").alias("c")
    dim_date = read_pg(spark, "dwh.dim_date").alias("d")
    dim_store = read_pg(spark, "dwh.dim_store").alias("s")
    dim_supplier = read_pg(spark, "dwh.dim_supplier").alias("u")

    sales_ext = (
        fact.join(dim_product, F.col("f.product_id") == F.col("p.product_id"), "left")
        .join(dim_customer, F.col("f.customer_id") == F.col("c.customer_id"), "left")
        .join(dim_date, F.col("f.date_id") == F.col("d.date_id"), "left")
        .join(dim_store, F.col("f.store_id") == F.col("s.store_id"), "left")
        .join(dim_supplier, F.col("f.supplier_id") == F.col("u.supplier_id"), "left")
        .select(
            F.col("f.source_row_id").alias("sale_id"),
            F.col("f.customer_id"),
            F.col("f.seller_id"),
            F.col("f.product_id"),
            F.col("f.store_id"),
            F.col("f.supplier_id"),
            F.col("f.quantity"),
            F.col("f.total_price"),
            F.col("f.unit_price"),
            F.col("d.date_value"),
            F.col("d.year"),
            F.col("d.month"),
            F.col("d.day"),
            F.col("c.first_name").alias("customer_first_name"),
            F.col("c.last_name").alias("customer_last_name"),
            F.col("c.country").alias("customer_country"),
            F.col("p.name").alias("product_name"),
            F.col("p.category").alias("product_category"),
            F.col("p.rating").alias("product_rating"),
            F.col("p.reviews").alias("product_reviews"),
            F.col("p.price").alias("product_price"),
            F.col("s.name").alias("store_name"),
            F.col("s.city").alias("store_city"),
            F.col("s.country").alias("store_country"),
            F.col("u.name").alias("supplier_name"),
            F.col("u.country").alias("supplier_country"),
        )
    )

    products_top10 = (
        sales_ext.groupBy("product_id", "product_name", "product_category")
        .agg(
            F.sum("quantity").alias("total_qty"),
            F.sum("total_price").alias("revenue"),
        )
        .orderBy(F.desc("total_qty"))
        .limit(10)
        .select(
            F.lit("top10_products_by_qty").alias("metric_type"),
            "product_id",
            "product_name",
            "product_category",
            F.col("total_qty").cast("double").alias("value_1"),
            F.col("revenue").cast("double").alias("value_2"),
            F.lit(None).cast("double").alias("value_3"),
            F.current_date().alias("report_date"),
        )
    )

    products_revenue_by_category = (
        sales_ext.groupBy("product_category")
        .agg(F.sum("total_price").alias("revenue"))
        .select(
            F.lit("revenue_by_category").alias("metric_type"),
            F.lit(None).cast("int").alias("product_id"),
            F.lit(None).cast("string").alias("product_name"),
            "product_category",
            F.col("revenue").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.lit(None).cast("double").alias("value_3"),
            F.current_date().alias("report_date"),
        )
    )

    products_rating_reviews = (
        sales_ext.select("product_id", "product_name", "product_rating", "product_reviews").distinct()
        .select(
            F.lit("rating_and_reviews_by_product").alias("metric_type"),
            "product_id",
            "product_name",
            F.lit(None).cast("string").alias("product_category"),
            F.col("product_rating").cast("double").alias("value_1"),
            F.col("product_reviews").cast("double").alias("value_2"),
            F.lit(None).cast("double").alias("value_3"),
            F.current_date().alias("report_date"),
        )
    )

    report_products = products_top10.unionByName(products_revenue_by_category).unionByName(products_rating_reviews)

    customers_top10 = (
        sales_ext.groupBy("customer_id", "customer_first_name", "customer_last_name", "customer_country")
        .agg(F.sum("total_price").alias("total_spent"), F.avg("total_price").alias("avg_check"))
        .orderBy(F.desc("total_spent"))
        .limit(10)
        .select(
            F.lit("top10_customers_by_total_spent").alias("metric_type"),
            "customer_id",
            F.concat_ws(" ", "customer_first_name", "customer_last_name").alias("customer_name"),
            F.col("customer_country").alias("country"),
            F.col("total_spent").cast("double").alias("value_1"),
            F.col("avg_check").cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    customers_country_distribution = (
        dim_customer.groupBy("country")
        .agg(F.count("customer_id").alias("customers_cnt"))
        .select(
            F.lit("customers_distribution_by_country").alias("metric_type"),
            F.lit(None).cast("int").alias("customer_id"),
            F.lit(None).cast("string").alias("customer_name"),
            "country",
            F.col("customers_cnt").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    customers_avg_check = (
        sales_ext.groupBy("customer_id", "customer_first_name", "customer_last_name")
        .agg(F.avg("total_price").alias("avg_check"))
        .select(
            F.lit("avg_check_by_customer").alias("metric_type"),
            "customer_id",
            F.concat_ws(" ", "customer_first_name", "customer_last_name").alias("customer_name"),
            F.lit(None).cast("string").alias("country"),
            F.col("avg_check").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    report_customers = customers_top10.unionByName(customers_country_distribution).unionByName(customers_avg_check)

    time_month_year = (
        sales_ext.groupBy("year", "month")
        .agg(F.sum("total_price").alias("revenue"), F.sum("quantity").alias("units"))
        .select(
            F.lit("monthly_yearly_sales_trend").alias("metric_type"),
            F.concat_ws("-", F.col("year"), F.format_string("%02d", F.col("month"))).alias("period"),
            "year",
            "month",
            F.col("revenue").cast("double").alias("value_1"),
            F.col("units").cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    time_revenue_period = (
        sales_ext.withColumn("period", F.when(F.col("year") <= 2021, F.lit("2021_and_earlier")).otherwise(F.lit("after_2021")))
        .groupBy("period")
        .agg(F.sum("total_price").alias("revenue"))
        .select(
            F.lit("revenue_by_period").alias("metric_type"),
            F.col("period"),
            F.lit(None).cast("int").alias("year"),
            F.lit(None).cast("int").alias("month"),
            F.col("revenue").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    time_avg_order = (
        sales_ext.groupBy("year", "month")
        .agg(F.avg("total_price").alias("avg_order_size"))
        .select(
            F.lit("avg_order_by_month").alias("metric_type"),
            F.concat_ws("-", F.col("year"), F.format_string("%02d", F.col("month"))).alias("period"),
            "year",
            "month",
            F.col("avg_order_size").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    report_time = time_month_year.unionByName(time_revenue_period).unionByName(time_avg_order)

    stores_top5 = (
        sales_ext.groupBy("store_id", "store_name")
        .agg(F.sum("total_price").alias("revenue"), F.avg("total_price").alias("avg_check"))
        .orderBy(F.desc("revenue"))
        .limit(5)
        .select(
            F.lit("top5_stores_by_revenue").alias("metric_type"),
            "store_id",
            "store_name",
            F.lit(None).cast("string").alias("city"),
            F.lit(None).cast("string").alias("country"),
            F.col("revenue").cast("double").alias("value_1"),
            F.col("avg_check").cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    stores_city_country = (
        sales_ext.groupBy("store_city", "store_country")
        .agg(F.sum("total_price").alias("revenue"))
        .select(
            F.lit("sales_by_city_country").alias("metric_type"),
            F.lit(None).cast("int").alias("store_id"),
            F.lit(None).cast("string").alias("store_name"),
            F.col("store_city").alias("city"),
            F.col("store_country").alias("country"),
            F.col("revenue").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    stores_avg_check = (
        sales_ext.groupBy("store_id", "store_name")
        .agg(F.avg("total_price").alias("avg_check"))
        .select(
            F.lit("avg_check_by_store").alias("metric_type"),
            "store_id",
            "store_name",
            F.lit(None).cast("string").alias("city"),
            F.lit(None).cast("string").alias("country"),
            F.col("avg_check").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    report_stores = stores_top5.unionByName(stores_city_country).unionByName(stores_avg_check)

    suppliers_top5 = (
        sales_ext.groupBy("supplier_id", "supplier_name")
        .agg(F.sum("total_price").alias("revenue"))
        .orderBy(F.desc("revenue"))
        .limit(5)
        .select(
            F.lit("top5_suppliers_by_revenue").alias("metric_type"),
            "supplier_id",
            "supplier_name",
            F.lit(None).cast("string").alias("supplier_country"),
            F.col("revenue").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    suppliers_avg_price = (
        sales_ext.groupBy("supplier_id", "supplier_name")
        .agg(F.avg("product_price").alias("avg_product_price"))
        .select(
            F.lit("avg_product_price_by_supplier").alias("metric_type"),
            "supplier_id",
            "supplier_name",
            F.lit(None).cast("string").alias("supplier_country"),
            F.col("avg_product_price").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    suppliers_country_distribution = (
        sales_ext.groupBy("supplier_country")
        .agg(F.sum("total_price").alias("revenue"))
        .select(
            F.lit("sales_by_supplier_country").alias("metric_type"),
            F.lit(None).cast("int").alias("supplier_id"),
            F.lit(None).cast("string").alias("supplier_name"),
            "supplier_country",
            F.col("revenue").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    report_suppliers = suppliers_top5.unionByName(suppliers_avg_price).unionByName(suppliers_country_distribution)

    all_rows_window = Window.partitionBy()

    quality_rating_extremes = (
        sales_ext.select("product_id", "product_name", "product_rating").distinct()
        .withColumn(
            "metric_type",
            F.when(
                F.col("product_rating") == F.max("product_rating").over(all_rows_window),
                F.lit("highest_rating_product"),
            ).when(
                F.col("product_rating") == F.min("product_rating").over(all_rows_window),
                F.lit("lowest_rating_product"),
            ),
        )
        .where(F.col("metric_type").isNotNull())
        .select(
            "metric_type",
            "product_id",
            "product_name",
            F.col("product_rating").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    sales_by_product = (
        sales_ext.groupBy("product_id", "product_name")
        .agg(F.sum("quantity").alias("sales_volume"))
        .join(
            sales_ext.select("product_id", "product_rating", "product_reviews").distinct(),
            "product_id",
            "left",
        )
    )

    corr_val = sales_by_product.stat.corr("product_rating", "sales_volume")
    quality_corr_schema = T.StructType(
        [
            T.StructField("metric_type", T.StringType(), False),
            T.StructField("product_id", T.IntegerType(), True),
            T.StructField("product_name", T.StringType(), True),
            T.StructField("value_1", T.DoubleType(), True),
            T.StructField("value_2", T.DoubleType(), True),
        ]
    )
    quality_corr = spark.createDataFrame(
        [
            (
                "rating_sales_correlation",
                None,
                None,
                float(corr_val) if corr_val is not None else None,
                None,
            )
        ],
        schema=quality_corr_schema,
    ).withColumn("report_date", F.current_date())

    quality_reviews_top = (
        sales_ext.select("product_id", "product_name", "product_reviews").distinct().orderBy(F.desc("product_reviews"))
        .limit(10)
        .select(
            F.lit("top_products_by_reviews").alias("metric_type"),
            "product_id",
            "product_name",
            F.col("product_reviews").cast("double").alias("value_1"),
            F.lit(None).cast("double").alias("value_2"),
            F.current_date().alias("report_date"),
        )
    )

    report_quality = quality_rating_extremes.unionByName(quality_corr).unionByName(quality_reviews_top)

    write_clickhouse(report_products, "report_sales_products")
    write_clickhouse(report_customers, "report_sales_customers")
    write_clickhouse(report_time, "report_sales_time")
    write_clickhouse(report_stores, "report_sales_stores")
    write_clickhouse(report_suppliers, "report_sales_suppliers")
    write_clickhouse(report_quality, "report_product_quality")

    spark.stop()


if __name__ == "__main__":
    main()