from pyspark.sql import SparkSession
from configparser import ConfigParser


def filtered_customers():
    temp_cust = spark_session.read.parquet(customers_path)
    latest_date = temp_cust.selectExpr('max(created_date) as max_date').collect()[0]['max_date']
    print("latest date in customers : ", latest_date)
    db_customers = spark_session.read.jdbc(properties["url"], table='customers', properties=properties)
    filtered_cust = db_customers.filter(db_customers['created_date'] > latest_date)
    filtered_cust.show(100)
    filtered_cust.write.mode('append').parquet('file:///C:/us-3/customers_filtered')
    filtered_cust.write.mode('append').jdbc(properties["url"], table='customers_filtered', properties=properties)


def filtered_items():
    temp_items = spark_session.read.parquet(items_path)
    latest_date = temp_items.selectExpr('max(created_date) as max_date').collect()[0]['max_date']
    print("latest date in items : ", latest_date)
    db_items = spark_session.read.jdbc(properties["url"], table='items', properties=properties)
    filtered_item = db_items.filter(db_items["created_date"] > latest_date)
    filtered_item.show(100)
    filtered_item.write.mode('append').parquet('file:///C:/us-3/items_filtered')
    filtered_item.write.mode('append').jdbc(properties["url"], table='items_filtered', properties=properties)


def filtered_order_details():
    temp_details = spark_session.read.parquet(orders_path)
    latest_date = temp_details.selectExpr('max(created_date) as max_date').collect()[0]['max_date']
    print("latest date in order_details : ", latest_date)
    db_details = spark_session.read.jdbc(properties["url"], table='order_details', properties=properties)
    filtered_order_detail = db_details.filter(db_details["created_date"] > latest_date)
    filtered_order_detail.show(100)
    filtered_order_detail.write.mode('append').parquet('file:///C:/us-3/order_details_filtered')
    filtered_order_detail.write.mode('append').jdbc(properties["url"], table='order_details_filtered',
                                                    properties=properties)


def filtered_orders():
    temp_orders = spark_session.read.parquet(orders_path)
    latest_date = temp_orders.selectExpr('max(created_date) as max_date').collect()[0]['max_date']
    print("latest date in orders : ", latest_date)
    db_details = spark_session.read.jdbc(properties["url"], table='order_details', properties=properties)
    filtered_order = db_details.filter(db_details["created_date"] > latest_date)
    filtered_order.show()
    filtered_order.write.mode('append').parquet('file:///C:/us-3/orders_filtered')
    filtered_order.write.mode('append').jdbc(properties["url"], table='order_filtered', properties=properties)


def filtered_sales():
    temp_sales = spark_session.read.parquet(salespersons_path)
    latest_date = temp_sales.selectExpr('max(created_date) as max_date').collect()[0]['max_date']
    print("latest date in salespersons : ", latest_date)
    db_sales = spark_session.read.jdbc(properties["url"], table='salesperson', properties=properties)
    filtered_sale = db_sales.filter(db_sales["created_date"] > latest_date)
    filtered_sale.show()
    filtered_sale.write.mode('append').parquet('file:///C:/us-3/salesperson_filtered')
    filtered_sale.write.mode('append').jdbc(properties["url"], table='salesperson_filtered', properties=properties)


def filtered_ship_to():
    temp_ship_to = spark_session.read.parquet(ship_to_path)
    latest_date = temp_ship_to.selectExpr("max(created_date) as max_date").collect()[0]["max_date"]
    print("Latest Date in ship to : ", latest_date)
    db_ship_to = spark_session.read.jdbc(url=properties["url"], table='ship_to', properties=properties)
    filtered_db_df = db_ship_to.filter(db_ship_to["created_date"] > latest_date)
    filtered_db_df.show()
    table_name = "ship_to_filtered"
    filtered_db_df.write.mode("append").jdbc(properties["url"], table_name, properties=properties)
    filtered_db_df.write.mode("append").parquet("file:///C:/us-3/ship_to_filtered")


if __name__ == '__main__':
    spark_session = SparkSession.builder.config("spark.jars", 'C:\CG-POC3\postgresql-42.6.0.jar').appName(
        "jdbc").master(
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
    customers_path = config.get('parquet', 'customers')
    items_path = config.get('parquet', 'items')
    order_details_path = config.get('parquet', 'order_details')
    orders_path = config.get('parquet', 'orders')
    salespersons_path = config.get('parquet', 'salespersons')
    ship_to_path = config.get('parquet', 'ship_to')

    filtered_customers()
    filtered_items()
    filtered_order_details()
    filtered_orders()
    filtered_sales()
    filtered_ship_to()
