from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from configparser import ConfigParser


def configuration():
    spark_session = SparkSession.builder.config("spark.jars", 'C:\CG-POC3\postgresql-42.6.0.jar').appName(
        "jdbc").master("local").getOrCreate()
    config = ConfigParser()
    config_path = "C:/Users/venavyas/PycharmProjects/python/cgpoc3.properties"
    with open(config_path, "r") as config_file:
        content = config_file.read()
        config.read_string(content)
    db_properties = {
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

    temp_cust = spark_session.read.parquet(customers_path)
    temp_cust.createOrReplaceTempView("customers_view")
    # temp_cust.show()

    temp_item = spark_session.read.parquet(items_path)
    temp_item.createOrReplaceTempView("items_view")
    # temp_item.show()

    temp_details = spark_session.read.parquet(order_details_path)
    temp_details.createOrReplaceTempView("order_details_view")
    # temp_details.show()

    temp_orders = spark_session.read.parquet(orders_path)
    temp_orders.createOrReplaceTempView("orders_view")
    # temp_details.show()

    temp_sales = spark_session.read.parquet(salespersons_path)
    temp_sales.createOrReplaceTempView("salespersons_view")
    # temp_sales.show()

    temp_ship = spark_session.read.parquet(ship_to_path)
    temp_ship.createOrReplaceTempView("ship_to_view")
    # spark.sql('select * from ship_to_view').show()
    # temp_ship.show()

    return spark_session, db_properties


# 1. Monthly/Weekly Customer wise order count
def query1(spark, properties):
    query = ''' select c.cust_id,c.cust_name, count(od.ORDER_ID) as count
                from customers_view c  
                left join orders_view o on (c.cust_id= o.cust_id )
                left join order_details_view od on ( o.order_id = od.order_id)
                left join items_view i on (od.item_id = i.item_id)
                group by c.cust_id,c.cust_name; '''
    data = spark.sql(query).withColumn('current_date', current_date())
    print('--Output1--')
    data.show()
    data.write.mode('overwrite').jdbc(properties['url'], table='query1_customer_wise_order_count',
                                      properties=properties)
    data.coalesce(1).write.mode('overwrite').partitionBy('current_date').parquet(
        'file:///C:/query/query1_customer_wise_order_count')


# 2. Monthly/Weekly Customer wise sum of orders
def query2(spark, properties):
    query = ''' select c.cust_id,c.cust_name,sum(od.detail_unit_price*od.item_quantity) as total_order
                from customers_view c left join orders_view o 
                on c.cust_id=o.cust_id left join order_details_view od
                on od.order_id=o.order_id left join items_view i
                on i.item_id=od.item_id
                group by c.cust_id, c.cust_name; '''
    data = spark.sql(query).withColumn('current_date', current_date())
    print('--Output2--')
    data.show()
    data.write.mode('overwrite').jdbc(properties['url'], table='query2_customer_wise_sum_of_orders',
                                      properties=properties)
    data.coalesce(1).write.mode('overwrite').partitionBy('current_date').parquet(
        'file:///C:/query/query2_customer_wise_sum_of_orders')


# 3. Item wise Total Order count
def query3(spark, properties):
    query = ''' select i.item_id, i.item_description, sum(od.item_quantity) as sum
                from items_view i left join order_details_view od on i.item_id=od.item_id
                group by i.item_id, i.item_description
                order by sum desc; '''
    data = spark.sql(query).withColumn('current_date', current_date())
    print('--Output3--')
    data.show()
    data.write.mode('overwrite').jdbc(properties['url'], table='query3_item_wise_Total_Order_count',
                                      properties=properties)
    data.coalesce(1).write.mode('overwrite').partitionBy('current_date').parquet(
        'file:///C:/query/query3_item_wise_Total_Order_count')


# 4. Item name/category wise  Total Order count descending
def query4(spark, properties):
    query = ''' select i.category, sum(od.item_quantity)as sum
                from items_view i left join order_details_view od on i.item_id=od.item_id
                group by i.category
                order by sum desc; '''
    data = spark.sql(query).withColumn('current_date', current_date())
    print('--Output4--')
    data.show()
    data.write.mode('overwrite').jdbc(properties['url'], table='query4_category_wise_Total_Order_count_descending',
                                      properties=properties)
    data.coalesce(1).write.mode('overwrite').partitionBy('current_date').parquet(
        'file:///C:/query/query4_category_wise_Total_Order_count_descending')


# 5. Item wise total order amount in descending
def query5(spark, properties):
    query = ''' select i.item_id, i.item_description, sum(i.unit_price*od.item_quantity) as sum
                from items_view i left join order_details_view od on i.item_id=od.item_id
                group by i.item_id, i.item_description
                order by sum desc; '''
    data = spark.sql(query).withColumn('current_date', current_date())
    print('--Output5--')
    data.show()
    data.write.mode('overwrite').jdbc(properties['url'], table='query5_Item_wise_total_order_amount_in_descending',
                                      properties=properties)
    data.write.mode('overwrite').partitionBy('current_date').parquet(
        'file:///C:/query/query5_Item_wise_total_order_amount_in_descending')


# 6. Item name/category wise  total order amount in descending
def query6(spark, properties):
    query = ''' select i.category, sum(od.item_quantity * od.detail_unit_price) as sum 
                from items_view i left join order_details_view od on (od.item_id = i.item_id)
                group by i.category  
                order by sum desc; '''
    data = spark.sql(query).withColumn('current_date', current_date())
    print('--Output6--')
    data.show()
    data.write.mode('overwrite').jdbc(properties['url'],
                                      table='query6_Item_name_category_wise_total_order_amount_in_descending',
                                      properties=properties)
    data.write.mode('overwrite').partitionBy('current_date').parquet(
        'file:///C:/query/query6_Item_name/category_wise_total_order_amount_in_descending')


# 7. Salesman wise total order amt and incentive
def query7(spark, properties):
    query = ''' select s.salesman_id, sum(od.item_quantity * od.detail_unit_price) as total_order_amount, round(sum(od.item_quantity * od.detail_unit_price)*0.1, 2)  as commission
                from salespersons_view s left join customers_view c on (s.salesman_id = c.salesman_id)
                left join orders_view o on (c.cust_id= o.cust_id )
                left join order_details_view od on ( o.order_id = od.order_id)
                group by s.salesman_id; '''
    data = spark.sql(query).withColumn('current_date', current_date())
    print('--Output7--')
    data.show()
    data.write.mode('overwrite').jdbc(properties['url'], table='query7_Salesman_wise_total_order_amt_and_incentive',
                                      properties=properties)
    data.write.mode('overwrite').partitionBy('current_date').parquet(
        'file:///C:/query/query7_Salesman_wise_total_order_amt_and_incentive')


# 8. Reports for Items not sold in previous month
def query8(spark, properties):
    query = ''' select i.item_id,i.item_description 
                from items_view i 
                where i.ITEM_ID not in (select item_id from order_details_view ); '''
    data = spark.sql(query).withColumn('current_date', current_date())
    print('--Output8--')
    data.show()
    data.write.mode('overwrite').jdbc(properties['url'], table='query8_Items_not_sold_in_previous_month',
                                      properties=properties)
    data.write.mode('overwrite').partitionBy('current_date').parquet('file:///C:/query/query8_Items_not_sold_in_previous_month')


# 9. Report customer whose orders shipped in previous month
def query9(spark, properties):
    query = ''' select c.cust_id, c.cust_name 
                from customers_view c 
                where c.cust_id not in (select cust_id  from ship_to_view); '''
    data = spark.sql(query).withColumn('current_date', current_date())
    print('--Output9--')
    data.show()
    data.write.mode('overwrite').jdbc(properties['url'], table='query9_customer_whose_orders_shipped_in_previous_month',
                                      properties=properties)
    data.write.mode('overwrite').partitionBy('current_date').parquet(
        'file:///C:/query/query9_customer_whose_orders_shipped_in_previous_month')


# 10. All customer shipment whose address is not filled correctly(incorrect pincode) for previous month
def query10(spark, properties):
    query = ''' select c.CUST_ID, c.cust_name 
                from customers_view c join ship_to_view s  on (s.cust_id =c.cust_id)  
                where length(s.postal_code) !=6; '''
    data = spark.sql(query).withColumn('current_date', current_date())
    print('--Output10--')
    data.show()
    data.write.mode('overwrite').jdbc(properties['url'], table='query10_customer_shipment_whose_address_not_filled_correctly_for_previous_month', properties=properties)
    data.coalesce(1).write.mode('overwrite').partitionBy('current_date').parquet('file:///C:/query/query10_customer_shipment_whose_address_not_filled_correctly_for_previous_month')


if __name__ == '__main__':
    SparkSession, dbProperties = configuration()
    print('outputs')
    # query1(SparkSession, dbProperties)
    # query2(SparkSession, dbProperties)
    # query3(SparkSession, dbProperties)
    # query4(SparkSession, dbProperties)
    # query5(SparkSession, dbProperties)
    # query6(SparkSession, dbProperties)
    # query7(SparkSession, dbProperties)
    # query8(SparkSession, dbProperties)
    # query9(SparkSession, dbProperties)
    # query10(SparkSession, dbProperties)
