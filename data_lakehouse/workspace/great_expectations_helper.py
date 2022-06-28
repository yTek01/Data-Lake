
from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Great Expectation").config("spark.some.config.option", "some-value").getOrCreate()
Employee = spark.createDataFrame([('1', 'Joe', 70000, '1'),
                                  ('2', 'Henry', 80000, '2'),
                                  ('3', 'Sam', 60000, '2'),
                                  ('4', 'Max', 90000, '1')],
                                 ['Id', 'Name', 'Sallary','DepartmentId'])

Employee.printSchema()
Employee.show()



def expectations_helper(data):
    
    store_backend_defaults = InMemoryStoreBackendDefaults()

    data_context_config = DataContextConfig(
        store_backend_defaults=store_backend_defaults,
        checkpoint_store_name=store_backend_defaults.checkpoint_store_name,
    )


    context = BaseDataContext(project_config=data_context_config)

    datasource_config = {
        "name": "my_spark_dataframe",
        "class_name": "Datasource",
        "execution_engine": {"class_name": "SparkDFExecutionEngine"},
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["batch_id"],
            }
        },
    }

    context.test_yaml_config(yaml.dump(datasource_config))

    context.add_datasource(**datasource_config)


    batch_request = RuntimeBatchRequest(
        datasource_name="my_spark_dataframe",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="Spark_Dataframe",  
        batch_identifiers={"batch_id": "default_identifier"},
        runtime_parameters={"batch_data": Employee},
    )


    context.create_expectation_suite(
        expectation_suite_name="test_suite", overwrite_existing=True
    )


    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )


    print(validator.head())
    validator.expect_column_values_to_not_be_null('Id')
    validator.expect_column_values_to_be_unique('Name')
    return "Your data is available in S3 bucket"
    