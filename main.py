import pyspark
import logging
import sys
import pandas
from pyspark.sql import functions as F

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType, StringType

from logging.handlers import TimedRotatingFileHandler
from logging import Formatter


def load_data(path, is_header):
    """
    Function for loading data in csv format
    :param path: path to .csv file location
    :param is_header: Boolean value, which specify if we take header row as column name for our dataframe
    :returns: spark dataframe with data with or without column names
    """
    data = spark.read.option("header", is_header) \
        .csv(path)
    return data


def rename_columns(df, columns):
    """
    Function takes dataframe and change their names based on given dict, which format should look like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}
    :param df: dataframe
    :param columns: dict with old and new names of columns
    :returns: return dataframe with renamed columns
    :raises ValueError: raises an exception
    """
    if isinstance(columns, dict):
        return df.select(*[F.col(col_name).alias(columns.get(col_name, col_name)) for col_name in df.columns])
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")


def list_filter(df, given_list, colname):
    """
    Function which filter with multiple values on chosen dataframe column
    :param df: dataframe
    :param given_list: list with values used for filtering
    :param colname: column name, which will used for filtering

    :returns: filtered dataframe
    """

    @F.udf(returnType=BooleanType())
    def f(col1):
        """
            User defined function for filtering every row in dataframe using given list
            :param col1: column used for filtering

            :returns: Boolean value, which specify if processed value is in or not in given list
            """

        return col1 in given_list

    return df.filter(f(col(colname)))


def logger_conf(path):
    """
    Function which takes path for a log file and configure logger e.g. setting level of logging or adding handler

    :param path: path to log file location
    :returns: return configured logger
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    handler = TimedRotatingFileHandler(filename=path, when='D', interval=1, backupCount=90,
                                       encoding='utf-8',
                                       delay=False)
    formatter = Formatter(fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


if __name__ == "__main__":
    logger = logger_conf('logs/runtime.log')
    logger.info("1 - INFO - Libraries imported")

    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("assignment.com") \
        .getOrCreate()

    logger.info("2 - INFO - Session started")

    logger.debug("1 - DEBUG - Passed arguments to application {}".format(sys.argv))

    client_data = load_data(sys.argv[1], True)
    financial_data = load_data(sys.argv[2], True)
    countries = str(sys.argv[3]).split(",")

    logger.info("3 - INFO - Data loaded")

    logger.debug("2 - DEBUG - Countries List")
    logger.debug(countries)

    # filtering countries
    client_data = list_filter(client_data, countries, "country")

    logger.debug("3 - DEBUG - Client data first 10 records after filtering")
    logger.debug(client_data.show(n=10))

    # dropping personal info
    client_data = client_data.drop("first_name", "last_name", "country")

    logger.debug("4 - DEBUG - Client data schema after dropping personal columns")
    logger.debug(client_data.printSchema())

    financial_data = financial_data.drop("cc_n")
    logger.debug("5 - DEBUG - Financial data schema after dropping card number column")
    logger.debug(financial_data.printSchema())

    # join

    joined_data = client_data.join(financial_data, ["id"], "inner")

    logger.debug("6 - DEBUG - joined data schema ")
    logger.debug(joined_data.printSchema())

    logger.debug("6 - DEBUG - joined data 10 first record ")
    logger.debug(joined_data.show(n=10))

    # rename

    joined_data = rename_columns(joined_data,
                                 {'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'})

    logger.debug("7 - DEBUG - joined data 10 first record ")
    logger.debug(joined_data.show(n=10))

    logger.info("4 - INFO - Data transformed")

    joined_data.toPandas().to_csv("./client_data/output.csv", index=False)

    logger.info("5 - INFO - Data saved")
