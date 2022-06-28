
from pprint import pprint
import boto3
from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    S3StoreBackendDefaults
)
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
Employee = spark.createDataFrame([('1', 'Joe', 70000, '1'),
                                  ('2', 'Henry', 80000, '2'),
                                  ('3', 'Sam', 60000, '2'),
                                  ('4', 'Max', 90000, '1')],
                                 ['Id', 'Name', 'Sallary','DepartmentId'])

Employee.printSchema()
Employee.show()



def expectations_helper():
    
    s3 = boto3.resource('s3',
       aws_access_key_id = 'XXXXXXXXXXXXXXXXXXXXXXXXX', #FOR THE CREDENTIAL TO WORK FOR GE you must use AWS Configure method on the instance
       aws_secret_access_key= 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
       region_name = 'us-east-1')
    
    bucket_name = "great.expectations.results"
    store_backend_defaults = S3StoreBackendDefaults(default_bucket_name=bucket_name,
                                                   validations_store_bucket_name='override_default_validations_store_bucket_name_in_S3StoreBackendDefaults',
                                                   validations_store_prefix='override_default_validations_store_prefix_in_S3StoreBackendDefaults'
                                                   )

    stores = {
        'evaluation_parameter_store' : {'class_name' : 'EvaluationParameterStore'}, 
        'expectations_S3_store' : {
            'class_name' : 'ExpectationsStore', 
            'store_backend' : {
                'class_name' : 'TupleS3StoreBackend', 
                'bucket' : bucket_name,
                'prefix' : 'expectations'
            }
        },
        'validations_s3_store' : {
            'class_name' : 'ValidationsStore', 
            'store_backend' : {
                'bucket' : bucket_name,
                'class_name' : 'TupleS3StoreBackend', 
                'prefix' : 'expectations'
            }
        }
    }



    data_context_config = DataContextConfig(
        store_backend_defaults=store_backend_defaults,
        stores=stores,
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
        runtime_parameters={"batch_data": DATA},
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
    