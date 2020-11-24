from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column

def main():
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    topic = "pageviews"
    kafka_servers = "http://localhost:9092"
    schema_registry_server = "http://localhost:8081"
    destination = "/tmp/out/destination"
    checkpoint = "/tmp/out/checkpoint"
    df = spark.readStream\
        .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")\
        .option("subscribe", topic)\
        .option("startingOffsets", "earliest")\
        .option("kafka.bootstrap.servers", kafka_servers)\
        .load()

    avro_df = df.select(from_avro("value", topic, schema_registry_server).alias("decoded"))
    query = avro_df.writeStream\
        .option("checkpointLocation", checkpoint)\
        .trigger(once=True)\
        .format("parquet")\
        .start(destination)

    query.awaitTermination()
    spark.stop()

def from_avro(col, topic, schema_registry_url):
    """
    avro deserialize

    :param col: column name "key" or "value"
    :param topic: kafka topic
    :param schema_registry_url: schema registry http address
    :return:
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    naming_strategy = getattr(
        getattr(abris_avro.read.confluent.SchemaManager, "SchemaStorageNamingStrategies$"),
        "MODULE$"
    ).TOPIC_NAME()

    schema_registry_config_dict = {
        "schema.registry.url": schema_registry_url,
        "schema.registry.topic": topic,
        "{col}.schema.id".format(col=col): "latest",
        "{col}.schema.naming.strategy".format(col=col): naming_strategy
    }

    conf_map = getattr(getattr(jvm_gateway.scala.collection.immutable.Map, "EmptyMap$"), "MODULE$")
    for k, v in schema_registry_config_dict.items():
        conf_map = getattr(conf_map, "$plus")(jvm_gateway.scala.Tuple2(k, v))

    return Column(abris_avro.functions.from_confluent_avro(_to_java_column(col), conf_map))


def to_avro(col, topic, schema_registry_url):
    """
    avro  serialize
    :param col: column name "key" or "value"
    :param topic: kafka topic
    :param schema_registry_url: schema registry http address
    :return:
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    naming_strategy = getattr(
        getattr(abris_avro.read.confluent.SchemaManager, "SchemaStorageNamingStrategies$"),
        "MODULE$"
    ).TOPIC_NAME()

    schema_registry_config_dict = {
        "schema.registry.url": schema_registry_url,
        "schema.registry.topic": topic,
        "{col}.schema.id".format(col=col): "latest",
        "{col}.schema.naming.strategy".format(col=col): naming_strategy
    }

    conf_map = getattr(getattr(jvm_gateway.scala.collection.immutable.Map, "EmptyMap$"), "MODULE$")
    for k, v in schema_registry_config_dict.items():
        conf_map = getattr(conf_map, "$plus")(jvm_gateway.scala.Tuple2(k, v))

    return Column(abris_avro.functions.to_confluent_avro(_to_java_column(col), conf_map))


if __name__ == '__main__':
    main()