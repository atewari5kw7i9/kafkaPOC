import pandas as pd
from uuid import uuid4
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from time import gmtime, strftime

class TeradataRecord(object):
    """
    TeradataRecord record
    Args:
        domain_name (str): TeradataRecord's domain_name
        domain_address (str): TeradataRecord's domain_address
        run_dt (str): TeradataRecord's run_dt
    """

    def __init__(self,category,	category_keyword,	currency,	customer_first_name,	customer_first_name_keyword,	customer_full_name,	customer_full_name_keyword,	customer_gender,	customer_id,	customer_last_name,	customer_last_name_keyword,	customer_phone,	day_of_week,	day_of_week_i,	email,	event_dataset,	geoip_city_name,	geoip_continent_name,	geoip_country_iso_code,	geoip_location,	geoip_region_name,	manufacturer,	manufacturer_keyword,	order_date,	order_id,	products_id,	products_id_keyword,	products_base_price,	products_base_unit_price,	products_category,	products_category_keyword,	products_created_on,	products_discount_amount,	products_discount_percentage,	products_manufacturer,	products_manufacturer_keyword,	products_min_price,	products_price,	products_product_id,	products_product_name,	products_product_name_keyword,	products_quantity,	products_sku,	products_tax_amount,	products_taxful_price,	products_taxless_price,	products_unit_discount_amount,	sku,	taxful_total_price,	taxless_total_price,	total_quantity,	total_unique_products,	type,	user):
        self.category = category
        self.category_keyword = category_keyword
        self.currency = currency
        self.customer_first_name = customer_first_name
        self.customer_first_name_keyword = customer_first_name_keyword
        self.customer_full_name = customer_full_name
        self.customer_full_name_keyword = customer_full_name_keyword
        self.customer_gender = customer_gender
        self.customer_id = customer_id
        self.customer_last_name = customer_last_name
        self.customer_last_name_keyword = customer_last_name_keyword
        self.customer_phone = customer_phone
        self.day_of_week = day_of_week
        self.day_of_week_i = day_of_week_i
        self.email = email
        self.event_dataset = event_dataset
        self.geoip_city_name = geoip_city_name
        self.geoip_continent_name = geoip_continent_name
        self.geoip_country_iso_code = geoip_country_iso_code
        self.geoip_location = geoip_location
        self.geoip_region_name = geoip_region_name
        self.manufacturer = manufacturer
        self.manufacturer_keyword = manufacturer_keyword
        self.order_date = order_date
        self.order_id = order_id
        self.products_id = products_id
        self.products_id_keyword = products_id_keyword
        self.products_base_price = products_base_price
        self.products_base_unit_price = products_base_unit_price
        self.products_category = products_category
        self.products_category_keyword = products_category_keyword
        self.products_created_on = products_created_on
        self.products_discount_amount = products_discount_amount
        self.products_discount_percentage = products_discount_percentage
        self.products_manufacturer = products_manufacturer
        self.products_manufacturer_keyword = products_manufacturer_keyword
        self.products_min_price = products_min_price
        self.products_price = products_price
        self.products_product_id = products_product_id
        self.products_product_name = products_product_name
        self.products_product_name_keyword = products_product_name_keyword
        self.products_quantity = products_quantity
        self.products_sku = products_sku
        self.products_tax_amount = products_tax_amount
        self.products_taxful_price = products_taxful_price
        self.products_taxless_price = products_taxless_price
        self.products_unit_discount_amount = products_unit_discount_amount
        self.sku = sku
        self.taxful_total_price = taxful_total_price
        self.taxless_total_price = taxless_total_price
        self.total_quantity = total_quantity
        self.total_unique_products = total_unique_products
        self.type = type
        self.user = user
def prod_csv(df):

    #df_products = df.groupby(['StockCode','Description']).size().reset_index()
    df_cust_writable = df.filter(['country', 'id', 'industry', 'name', 'locality', 'region'], axis=1)
    #print(df_products_writable)
    df_cust_writable.to_json("Customer.json" , orient = 'records')

# def prod_csv_with_catg(df):
#     df_products = df.groupby(['Uniq Id', 'Product Name', 'List Price', 'Brand','Category']).size().reset_index()
#     df_products_writable = df_products.filter(['Uniq Id', 'Product Name', 'List Price', 'Brand','Category'], axis=1)
#     df_products_writable.rename(columns={'Uniq Id': 'ProdID', 'Product Name': 'ProdName','List Price':'Price'}, inplace=True)
#     print(df_products_writable)
#     df_products_writable.to_json("Products_withCatg.json", orient = 'records')

def record_to_dict(record, ctx):
    """
    Returns a dict representation of a TeradataRecord instance for serialization.
    Args:
        record (TeradataRecord): TeradataRecord instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """
    # User._address must not be serialized; omit from dict
    return dict(category = record.category,category_keyword = record.category_keyword,currency = record.currency,customer_first_name = record.customer_first_name,customer_first_name_keyword = record.customer_first_name_keyword,customer_full_name = record.customer_full_name,customer_full_name_keyword = record.customer_full_name_keyword,customer_gender = record.customer_gender,customer_id = record.customer_id,customer_last_name = record.customer_last_name,customer_last_name_keyword = record.customer_last_name_keyword,customer_phone = record.customer_phone,day_of_week = record.day_of_week,day_of_week_i = record.day_of_week_i,email = record.email,event_dataset = record.event_dataset,geoip_city_name = record.geoip_city_name,geoip_continent_name = record.geoip_continent_name,geoip_country_iso_code = record.geoip_country_iso_code,geoip_location = record.geoip_location,geoip_region_name = record.geoip_region_name,manufacturer = record.manufacturer,manufacturer_keyword = record.manufacturer_keyword,order_date = record.order_date,order_id = record.order_id,products_id = record.products_id,products_id_keyword = record.products_id_keyword,products_base_price = record.products_base_price,products_base_unit_price = record.products_base_unit_price,products_category = record.products_category,products_category_keyword = record.products_category_keyword,products_created_on = record.products_created_on,products_discount_amount = record.products_discount_amount,products_discount_percentage = record.products_discount_percentage,products_manufacturer = record.products_manufacturer,products_manufacturer_keyword = record.products_manufacturer_keyword,products_min_price = record.products_min_price,products_price = record.products_price,products_product_id = record.products_product_id,products_product_name = record.products_product_name,products_product_name_keyword = record.products_product_name_keyword,products_quantity = record.products_quantity,products_sku = record.products_sku,products_tax_amount = record.products_tax_amount,products_taxful_price = record.products_taxful_price,products_taxless_price = record.products_taxless_price,products_unit_discount_amount = record.products_unit_discount_amount,sku = record.sku,taxful_total_price = record.taxful_total_price,taxless_total_price = record.taxless_total_price,total_quantity = record.total_quantity,total_unique_products = record.total_unique_products,type = record.type,user = record.user)


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def write_to_kafka():
    boot_strap = 'pkc-4nym6.us-east-1.aws.confluent.cloud:9092'
    topic = 'elasticssearch_new'
    schema_registry = 'https://psrc-gn6wr.us-east-2.aws.confluent.cloud'
    schema_str ="""
		{
		  "$schema": "http://json-schema.org/draft-07/schema#",
          "title": "SalesData",
          "description": "A Confluent Kafka",
          "type": "object",
          "properties": {
            "category": {
              "type": ["null","string"]
            },
            "category_keyword": {
              "type": ["null","string"]
            },
            "currency": {
              "type": ["null","string"]
            },
			"customer_first_name": {
              "type": ["null","string"]
            },
            "customer_first_name_keyword": {
              "type": ["null","string"]
            },
            "customer_full_name": {
              "type": ["null","string"]
            },
			"customer_full_name_keyword": {
              "type": ["null","string"]
            },
            "customer_gender": {
              "type": ["null","string"]
            },
            "customer_id": {
              "type": ["null","number"]
            },
			"customer_last_name": {
              "type": ["null","string"]
            },
            "customer_last_name_keyword": {
              "type": ["null","string"]
            },
            "customer_phone": {
              "type": ["null","string"]
            },
			"day_of_week": {
              "type": ["null","string"]
            },
            "day_of_week_i": {
              "type": ["null","number"]
            },
            "email": {
              "type": ["null","string"]
            },
			"event_dataset": {
              "type": ["null","string"]
            },
            "geoip_city_name": {
              "type": ["null","string"]
            },
            "geoip_continent_name": {
              "type": ["null","string"]
            },
			"geoip_country_iso_code": {
              "type": ["null","string"]
            },
            "geoip_location": {
              "type": ["null","object"]
            },
            "geoip_region_name": {
              "type": ["null","string"]
            },
			"manufacturer": {
              "type": ["null","string"]
            },
            "manufacturer_keyword": {
              "type": ["null","string"]
            },
            "order_date": {
              "type": ["null","string"]
            },
            "order_id": {
               "type": ["null","number"]
            },
            "products_id": {
                "type": ["null","string"]
            },
            "products_id_keyword": {
                  "type": ["null","string"]
            },
            "products_base_price": {
                   "type": ["null","string"]
            },
            "products_base_unit_price": {
                          "type": ["null","string"]
            },
            "products_category": {
                          "type": ["null","string"]
            },
            "products_category_keyword": {
                          "type": ["null","string"]
            },
            "products_created_on": {
                          "type": ["null","string"]
            },
            "products_discount_amount": {
                          "type": ["null","string"]
            },
            "products_discount_percentage": {
                          "type": ["null","string"]
            },
            "products_manufacturer": {
                          "type": ["null","string"]
            },
            "products_manufacturer_keyword": {
                          "type": ["null","string"]
            },
            "products_min_price": {
              "type": ["null","string"]
            },
			"products_price": {
              "type": ["null","string"]
            },
            "products_product_id": {
              "type": ["null","string"]
            },
            "products_product_name": {
              "type": ["null","string"]
            },
			"products_product_name_keyword": {
              "type": ["null","string"]
            },
			"products_quantity": {
              "type": ["null","string"]
            },
            "products_sku": {
              "type": ["null","string"]
            },
            "products_tax_amount": {
              "type": ["null","string"]
            },
			"products_taxful_price": {
              "type": ["null","string"]
            },
			"products_taxless_price": {
              "type": ["null","string"]
            },
            "products_unit_discount_amount": {
              "type": ["null","string"]
            },
            "sku": {
              "type": ["null","string"]
            },
			"taxful_total_price": {
              "type": ["null","string"]
            },
			"taxless_total_price": {
              "type": ["null","string"]
            },
            "total_quantity": {
              "type": ["null","number"]
            },
            "total_unique_products": {
              "type": ["null","number"]
            },
			"type": {
              "type": ["null","string"]
            },
			"user": {
              "type": ["null","string"]
            }
          }
        }
    """
    schema_registry_conf = {'url': schema_registry,'basic.auth.user.info':'YKGR5ZHZUUL23HN4:+qADQ2pAUoOPzfoi2Ii23PTE37nU4vQJGQJeiQHWa/Dw0zDGisxrRneUo/M1oUDy'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    json_serializer = JSONSerializer(schema_str, schema_registry_client, record_to_dict)
    producer_conf = {'bootstrap.servers': boot_strap, 'security.protocol': 'SASL_SSL'
        , 'key.serializer': StringSerializer('utf_8')
        , 'value.serializer': json_serializer
        ,'sasl.mechanisms': 'PLAIN'
        ,'sasl.username': 'KUERMAKTUWI6ISIF'
        ,'sasl.password': 'hyqBH8Ls573nxtCSn7haOEivw4NgUrx79f25wOkPZcDyW2/CLFJPrafUeleHcxUV'}
    producer = SerializingProducer(producer_conf)
    producer.poll(0.0)
    df_to_kafka = pd.read_csv(r"C:\Users\XAXT176\working\kafka\archive\ElasticSearchData.csv")[0:3]
    for index, row in df_to_kafka.iterrows():
        category = row['category']
        category_keyword = row['category.keyword']
        currency = row['currency']
        customer_first_name = row['customer_first_name']
        customer_first_name_keyword = row['customer_first_name.keyword']
        customer_full_name = row['customer_full_name']
        customer_full_name_keyword = row['customer_full_name.keyword']
        customer_gender = row['customer_gender']
        customer_id = row['customer_id']
        customer_last_name = row['customer_last_name']
        customer_last_name_keyword = row['customer_last_name.keyword']
        customer_phone = row['customer_phone']
        day_of_week = row['day_of_week']
        day_of_week_i = row['day_of_week_i']
        email = row['email']
        event_dataset = row['event.dataset']
        geoip_city_name = row['geoip.city_name']
        geoip_continent_name = row['geoip.continent_name']
        geoip_country_iso_code = row['geoip.country_iso_code']
        geoip_location = None
        geoip_region_name = row['geoip.region_name']
        manufacturer = row['manufacturer']
        manufacturer_keyword = row['manufacturer.keyword']
        order_date = row['order_date']
        order_id = row['order_id']
        products_id = row['products._id']
        products_id_keyword = row['products._id.keyword']
        products_base_price = row['products.base_price']
        products_base_unit_price = row['products.base_unit_price']
        products_category = row['products.category']
        products_category_keyword = row['products.category.keyword']
        products_created_on = row['products.created_on']
        products_discount_amount = row['products.discount_amount']
        products_discount_percentage = row['products.discount_percentage']
        products_manufacturer = row['products.manufacturer']
        products_manufacturer_keyword = row['products.manufacturer.keyword']
        products_min_price = row['products.min_price']
        products_price = row['products.price']
        products_product_id = row['products.product_id']
        products_product_name = row['products.product_name']
        products_product_name_keyword = row['products.product_name.keyword']
        products_quantity = row['products.quantity']
        products_sku = row['products.sku']
        products_tax_amount = row['products.tax_amount']
        products_taxful_price = row['products.taxful_price']
        products_taxless_price = row['products.taxless_price']
        products_unit_discount_amount = row['products.unit_discount_amount']
        sku = row['sku']
        taxful_total_price = row['taxful_total_price']
        taxless_total_price = row['taxless_total_price']
        total_quantity = row['total_quantity']
        total_unique_products = row['total_unique_products']
        type = row['type']
        user = row['user']
        terarecord = TeradataRecord(category,	category_keyword,	currency,	customer_first_name,	customer_first_name_keyword,	customer_full_name,	customer_full_name_keyword,	customer_gender,	customer_id,	customer_last_name,	customer_last_name_keyword,	customer_phone,	day_of_week,	day_of_week_i,	email,	event_dataset,	geoip_city_name,	geoip_continent_name,	geoip_country_iso_code,	geoip_location,	geoip_region_name,	manufacturer,	manufacturer_keyword,	order_date,	order_id,	products_id,	products_id_keyword,	products_base_price,	products_base_unit_price,	products_category,	products_category_keyword,	products_created_on,	products_discount_amount,	products_discount_percentage,	products_manufacturer,	products_manufacturer_keyword,	products_min_price,	products_price,	products_product_id,	products_product_name,	products_product_name_keyword,	products_quantity,	products_sku,	products_tax_amount,	products_taxful_price,	products_taxless_price,	products_unit_discount_amount,	sku,	taxful_total_price,	taxless_total_price,	total_quantity,	total_unique_products,	type,	user)
        producer.produce(topic=topic, key=str(uuid4()), value=terarecord,
                         on_delivery=delivery_report)
        producer.flush()


if __name__=='__main__':
    ##df_main = pd.read_csv(r"C:\Users\XAXT176\working\kafka\archive\free_company_dataset.csv",nrows=69000)
    ##prod_csv(df_main)
    #df_product = pd.read_csv(r"marketing_sample_for_walmart_com-ecommerce__20191201_20191231__30k_data.csv")
    #prod_csv_with_catg(df_product)
    write_to_kafka()