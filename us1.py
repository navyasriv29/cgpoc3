from pyspark.sql import SparkSession
from configparser import ConfigParser


def main():
    spark = SparkSession.builder.config("spark.jars", 'C:\CG-POC3\postgresql-42.6.0.jar').appName("jdbc").master(
        "local").getOrCreate()
    config = ConfigParser()
    config_path = "C:/Users/venavyas/PycharmProjects/python/cgpoc3.properties"
    with open(config_path, "r") as config_file:
        content = config_file.read()
        config.read_string(content)
    properties = {
        "driver": config.get("database", "driver"),
        "user": config.get("database", "user"),
        "url": config.get("database", "url"),
        "password": config.get("database", "password")
    }
    table = ["customers", "orders", "items", "salesperson", "order_details", "ship_to"]
    op_path = config.get("output", "output_path")
    for i in table:
        data = spark.read.jdbc(url=properties["url"], table=i, properties=properties)
        data.show()
        data.write.parquet(op_path.format(str(i)))
    spark.stop()


if __name__ == '__main__':
    main()
