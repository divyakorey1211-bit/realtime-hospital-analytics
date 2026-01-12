from pyspark.sql.functions import *

# Azure Event Hub Configuration
EVENTHUBS_NAMESPACE = "<<Namespace_hostname>>"
EVENT_HUB_NAME = "<<Eventhub_Name>>"
CONNECTION_STRING = "<<Connection_string>>"


kafka_options = {
    'kafka.bootstrap.servers': f"{EVENTHUBS_NAMESPACE}:9093",
    'subscribe': EVENT_HUB_NAME,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{CONNECTION_STRING}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

#Read from eventhub
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

#Cast data to json
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

#ADLS configuration 
spark.conf.set(
  "fs.azure.account.key.<<Storageaccount_name>>.dfs.core.windows.net",
  "<<Storage_Account_access_key>>"
)



bronze_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
#Write stream to bronze

(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/mnt/bronze/_checkpoints/patient_event")
    .start(bronze_path)
)
