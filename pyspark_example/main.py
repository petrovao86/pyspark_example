from pyspark.sql import SparkSession


def run():
    spark = SparkSession.builder.getOrCreate()
    products = spark.createDataFrame(
        [
            (1, "Product_1"),
            (2, "Product_2"),
            (3, "Product_3"),
            (4, "Product_4"),
        ],
        ("id", "name"),
    )

    categories = spark.createDataFrame(
        [
            (1, "Category_1"),
            (2, "Category_2"),
        ],
        ("id", "name"),
    )

    product_categories = spark.createDataFrame(
        [
            (1, 1),
            (2, 1),
            (3, 1),
            (3, 2),
        ],
        ("product_id", "category_id"),
    )

    (
        products
        .join(product_categories, products.id == product_categories.product_id, "left")
        .join(categories, product_categories.category_id == categories.id, "left")
        .select(products.name.alias("Product"), categories.name.alias("Category"))
        .show()
    )


if __name__ == "__main__":
    run()
