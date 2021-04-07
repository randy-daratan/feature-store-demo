# import os
# import sys
# from pathlib import Path
# from feast import Client, Feature, Entity, ValueType, FeatureTable
# from feast.data_source import FileSource, KafkaSource
# from feast.data_format import ParquetFormat, AvroFormat
# #Using environmental variables
#
# #sys.path.append(Path(__file__).parent.parent)
#
# os.environ["FEAST_CORE_URL"] = "core:6565"
# os.environ["FEAST_SERVING_URL"] = "online_serving:6566"
# os.environ["FEAST_HISTORICAL_FEATURE_OUTPUT_LOCATION"] = "/home/jovyan/output/historical_feature_output"
# os.environ["FEAST_SPARK_STAGING_LOCATION"] = "/home/jovyan/output/staging"
#
# #Provide a map during client initialization
# options = {
#     "CORE_URL": "core:6565",
#     "SERVING_URL": "online_serving:6566",
# }
# client = Client(options)
#
# #As keyword arguments, without the `FEAST` prefix
# client = Client(core_url="core:6565", serving_url="online_serving:6566")