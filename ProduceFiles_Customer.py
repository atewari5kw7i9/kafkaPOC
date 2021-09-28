import pandas as pd
from uuid import uuid4
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

class TeradataRecord(object):
    """
    TeradataRecord record
    Args:
        domain_name (str): TeradataRecord's domain_name
        domain_address (str): TeradataRecord's domain_address
        run_dt (str): TeradataRecord's run_dt
    """

    def __init__(self, country, id, industry,name, locality, region,rewardstatus):
        self.country = country
        self.id = id
        self.industry = industry
        self.name = name
        self.locality = locality
        self.region = region
        self.rewardstatus = rewardstatus
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
    return dict(country=record.country,
                id=record.id,
                industry=record.industry,
                name=record.name,
                locality=record.locality,
                region=record.region,
                rewardstatus=record.rewardstatus)


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
    topic = 'customer'
    schema_registry = 'https://psrc-gn6wr.us-east-2.aws.confluent.cloud'
    schema_str = """
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "title": "CustomerData",
          "description": "A Confluent Kafka S3 to TD",
          "type": "object",
          "properties": {
            "country": {
              "description": "country",
              "type": ["null","string"]
            },
            "id": {
              "description": "id",
              "type": ["null","string"]
            },
            "industry": {
              "description": "industry",
              "type": ["null","string"]
            },
            "name": {
              "description": "name",
              "type": ["null","string"]
            },
            "locality": {
              "description": "locality",
              "type": ["null","string"]
            },
            "region": {
              "description": "region",
              "type": ["null","string"]
            },
            "region": {
              "description": "rewardstatus",
              "type": ["null","string"]
            }
          },
          "required": [ "id"]
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
    df_to_kafka = pd.read_json(r"C:\Users\XAXT176\working\kafka\archive\Customer.json")[0:6]
    for index, row in df_to_kafka.iterrows():
        ProdID = row['country']
        ProdName = row['id']
        Brand = row['industry']
        Category = row['name']
        Price = row['locality']
        region = row['region']
        rewardstatus = 'Silver'
        if (index%5 ==0):
            rewardstatus = "Platinum"
        if (index%3==0):
            rewardstatus = "Gold"
        terarecord = TeradataRecord(ProdID, ProdName, Brand, Category, Price, region, rewardstatus)
        print(terarecord)
        producer.produce(topic=topic, key=ProdName, value=terarecord,
                         on_delivery=delivery_report)
        producer.flush()


if __name__=='__main__':
    ##df_main = pd.read_csv(r"C:\Users\XAXT176\working\kafka\archive\free_company_dataset.csv",nrows=69000)
    ##prod_csv(df_main)
    #df_product = pd.read_csv(r"marketing_sample_for_walmart_com-ecommerce__20191201_20191231__30k_data.csv")
    #prod_csv_with_catg(df_product)
    write_to_kafka()