from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


POSTGRES_URL = "jdbc:postgresql://postgres:5432/bds2"
POSTGRES_PROPS = {
    "user": "bds_user",
    "password": "bds_pass",
    "driver": "org.postgresql.Driver",
}


def ns_eq(left_col, right_col):
    return left_col.eqNullSafe(right_col)


def write_pg(df, table_name: str, mode: str = "append") -> None:
    df.write.jdbc(
        url=POSTGRES_URL,
        table=table_name,
        mode=mode,
        properties=POSTGRES_PROPS,
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("bds2-etl-to-star")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    raw = spark.read.jdbc(
        url=POSTGRES_URL,
        table="public.mock_data",
        properties=POSTGRES_PROPS,
    )

    df = (
        raw.withColumn("sale_date", F.to_date(F.col("sale_date"), "M/d/yyyy"))
        .withColumn("product_release_date", F.to_date(F.col("product_release_date"), "M/d/yyyy"))
        .withColumn("product_expiry_date", F.to_date(F.col("product_expiry_date"), "M/d/yyyy"))
    )

    customer_w = Window.partitionBy("sale_customer_id").orderBy(F.col("id").asc())
    dim_customer = (
        df.filter(F.col("sale_customer_id").isNotNull())
        .withColumn("rn", F.row_number().over(customer_w))
        .filter(F.col("rn") == 1)
        .select(
            F.col("sale_customer_id").cast("int").alias("customer_id"),
            F.col("customer_first_name").alias("first_name"),
            F.col("customer_last_name").alias("last_name"),
            F.col("customer_age").cast("int").alias("age"),
            F.col("customer_email").alias("email"),
            F.col("customer_country").alias("country"),
            F.col("customer_postal_code").alias("postal_code"),
            F.col("customer_pet_type").alias("pet_type"),
            F.col("customer_pet_name").alias("pet_name"),
            F.col("customer_pet_breed").alias("pet_breed"),
        )
    )

    seller_w = Window.partitionBy("sale_seller_id").orderBy(F.col("id").asc())
    dim_seller = (
        df.filter(F.col("sale_seller_id").isNotNull())
        .withColumn("rn", F.row_number().over(seller_w))
        .filter(F.col("rn") == 1)
        .select(
            F.col("sale_seller_id").cast("int").alias("seller_id"),
            F.col("seller_first_name").alias("first_name"),
            F.col("seller_last_name").alias("last_name"),
            F.col("seller_email").alias("email"),
            F.col("seller_country").alias("country"),
            F.col("seller_postal_code").alias("postal_code"),
        )
    )

    dim_store = (
        df.select(
            F.col("store_name").alias("name"),
            F.col("store_location").alias("location"),
            F.col("store_city").alias("city"),
            F.col("store_state").alias("state"),
            F.col("store_country").alias("country"),
            F.col("store_phone").alias("phone"),
            F.col("store_email").alias("email"),
        )
        .distinct()
        .withColumn(
            "store_id",
            F.dense_rank().over(Window.orderBy("name", "location", "city", "state", "country", "phone", "email")),
        )
        .select("store_id", "name", "location", "city", "state", "country", "phone", "email")
    )

    dim_supplier = (
        df.select(
            F.col("supplier_name").alias("name"),
            F.col("supplier_contact").alias("contact"),
            F.col("supplier_email").alias("email"),
            F.col("supplier_phone").alias("phone"),
            F.col("supplier_address").alias("address"),
            F.col("supplier_city").alias("city"),
            F.col("supplier_country").alias("country"),
        )
        .distinct()
        .withColumn(
            "supplier_id",
            F.dense_rank().over(Window.orderBy("name", "contact", "email", "phone", "address", "city", "country")),
        )
        .select("supplier_id", "name", "contact", "email", "phone", "address", "city", "country")
    )

    product_w = Window.partitionBy("sale_product_id").orderBy(F.col("id").asc())
    dim_product = (
        df.where(F.col("sale_product_id").isNotNull())
        .withColumn("rn", F.row_number().over(product_w))
        .where(F.col("rn") == 1)
        .select(
            F.col("sale_product_id").cast("int").alias("product_id"),
            F.col("product_name").alias("name"),
            F.col("product_category").alias("category"),
            F.col("product_price").cast("double").alias("price"),
            F.col("product_quantity").cast("int").alias("quantity"),
            F.col("pet_category").alias("pet_category"),
            F.col("product_weight").cast("double").alias("weight"),
            F.col("product_color").alias("color"),
            F.col("product_size").alias("size"),
            F.col("product_brand").alias("brand"),
            F.col("product_material").alias("material"),
            F.col("product_description").alias("description"),
            F.col("product_rating").cast("double").alias("rating"),
            F.col("product_reviews").cast("int").alias("reviews"),
            F.col("product_release_date").alias("release_date"),
            F.col("product_expiry_date").alias("expiry_date"),
        )
    )

    dim_date = (
        df.select(F.col("sale_date").alias("date_value"))
        .where(F.col("date_value").isNotNull())
        .distinct()
        .withColumn("date_id", F.dense_rank().over(Window.orderBy("date_value")))
        .withColumn("year", F.year("date_value"))
        .withColumn("month", F.month("date_value"))
        .withColumn("day", F.dayofmonth("date_value"))
        .select("date_id", "date_value", "year", "month", "day")
    )

    m = df.alias("m")
    d = dim_date.alias("d")
    st = dim_store.alias("st")
    sup = dim_supplier.alias("sup")

    fact = (
        m.join(d, F.col("m.sale_date") == F.col("d.date_value"), "left")
        .join(
            st,
            ns_eq(F.col("m.store_name"), F.col("st.name"))
            & ns_eq(F.col("m.store_location"), F.col("st.location"))
            & ns_eq(F.col("m.store_city"), F.col("st.city"))
            & ns_eq(F.col("m.store_state"), F.col("st.state"))
            & ns_eq(F.col("m.store_country"), F.col("st.country"))
            & ns_eq(F.col("m.store_phone"), F.col("st.phone"))
            & ns_eq(F.col("m.store_email"), F.col("st.email")),
            "left",
        )
        .join(
            sup,
            ns_eq(F.col("m.supplier_name"), F.col("sup.name"))
            & ns_eq(F.col("m.supplier_contact"), F.col("sup.contact"))
            & ns_eq(F.col("m.supplier_email"), F.col("sup.email"))
            & ns_eq(F.col("m.supplier_phone"), F.col("sup.phone"))
            & ns_eq(F.col("m.supplier_address"), F.col("sup.address"))
            & ns_eq(F.col("m.supplier_city"), F.col("sup.city"))
            & ns_eq(F.col("m.supplier_country"), F.col("sup.country")),
            "left",
        )
        .select(
            F.col("m.id").cast("int").alias("source_row_id"),
            F.col("d.date_id").cast("long").alias("date_id"),
            F.col("m.sale_customer_id").cast("int").alias("customer_id"),
            F.col("m.sale_seller_id").cast("int").alias("seller_id"),
            F.col("m.sale_product_id").cast("int").alias("product_id"),
            F.col("st.store_id").cast("long").alias("store_id"),
            F.col("sup.supplier_id").cast("long").alias("supplier_id"),
            F.col("m.sale_quantity").cast("int").alias("quantity"),
            F.col("m.sale_total_price").cast("double").alias("total_price"),
            F.round(
                F.col("m.sale_total_price").cast("double")
                / F.when(F.col("m.sale_quantity") == 0, F.lit(None)).otherwise(F.col("m.sale_quantity")),
                2,
            ).alias("unit_price"),
        )
    )

    write_pg(dim_customer, "dwh.dim_customer")
    write_pg(dim_seller, "dwh.dim_seller")
    write_pg(dim_store, "dwh.dim_store")
    write_pg(dim_supplier, "dwh.dim_supplier")
    write_pg(dim_product, "dwh.dim_product")
    write_pg(dim_date, "dwh.dim_date")
    write_pg(fact, "dwh.fact_sales")

    spark.stop()


if __name__ == "__main__":
    main()