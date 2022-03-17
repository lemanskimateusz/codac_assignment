import logging
import pytest

from pyspark import HiveContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)

@pytest.fixture(scope="session")
def spark_context(request):
    conf = (SparkConf().setMaster("local[2]").setAppName("pytest-pyspark-local-testing"))
    request.addfinalizer(lambda: sc.stop())

    sc = SparkContext(conf=conf)
    quiet_py4j()
    return sc

@pytest.fixture(scope="session")
def spark_session(request):
    spark_session = SparkSession.builder \
        .master("local[*]") \
        .appName("local-testing-pyspark") \
        .getOrCreate()

    request.addfinalizer(lambda: spark_session.sparkContext.stop())

    return spark_session


@pytest.fixture(scope="session")
def hive_context(spark_context):
    return HiveContext(spark_context)

@pytest.fixture(scope="session")
def streaming_context(spark_context):
    return StreamingContext(spark_context, 1)