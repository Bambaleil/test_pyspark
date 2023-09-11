from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def get_product_category_pairs(products_df, categories_df):
    spark = SparkSession.builder.getOrCreate()

    product_category_df = products_df.join(categories_df, 'product_id', 'left')

    result_df = product_category_df.select('product_name', 'category_name')

    return result_df