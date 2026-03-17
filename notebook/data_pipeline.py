from pyspark.sql.functions import col, sum

customers_df = spark.read.table("customers")
accounts_df = spark.read.table("accounts")
transactions_df = spark.read.table("transactions")

t = transactions_df.alias("t")
a = accounts_df.alias("a")
c = customers_df.alias("c")

final_df = (
    t.join(a, col("t.account_id") == col("a.account_id"), "inner")
     .join(c, col("a.customer_id") == col("c.customer_id"), "inner")
     .select(
         col("t.transaction_id"),
         col("t.account_id"),
         col("a.customer_id"),
         col("t.transaction_date"),
         col("t.transaction_type"),
         col("t.amount"),
         col("t.merchant_name"),
         col("t.merchant_category"),
         col("t.payment_channel"),
         col("t.status").alias("transaction_status"),
         col("a.account_type"),
         col("a.currency"),
         col("a.opened_date"),
         col("a.status").alias("account_status"),
         col("c.customer_name"),
         col("c.date_of_birth"),
         col("c.city"),
         col("c.country"),
         col("c.risk_profile")
     )
)

final_clean_df = final_df.dropna().dropDuplicates()

top_customers = (
    final_clean_df
    .groupBy("customer_name")
    .agg(sum("amount").alias("total_spent"))
    .orderBy("total_spent", ascending=False)
)

top_customers.show()
