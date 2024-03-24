from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer


def get_product_category_names(products_df, categories_df):
    exploded_products_df = products_df.withColumn("category_id", explode_outer("category_id"))

    joined_df = exploded_products_df.join(categories_df, "category_id", "left_outer")

    result_df = joined_df.select("product_name", "category_name")

    return result_df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ProductCategoryNames") \
        .getOrCreate()

    products_data = [(1, "product1", [1, 2]), (2, "product2", [1, 2]), (3, "product3", [])]
    products_df = spark.createDataFrame(products_data, ["product_id", "product_name", "category_id"])

    categories_data = [(1, "category1"), (2, "category2")]
    categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])

    result_df = get_product_category_names(products_df, categories_df)
    result_df.show()
