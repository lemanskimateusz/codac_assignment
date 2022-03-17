import pyspark
import pytest
from main import list_filter, rename_columns

from chispa.dataframe_comparer import *

pytestmark = pytest.mark.usefixtures("spark_session")


def test_spark_session_dataframe(spark_session):
    test_df = spark_session.createDataFrame([[1, 3], [2, 4]], "a: int, b: int")

    assert type(test_df) == pyspark.sql.dataframe.DataFrame
    assert test_df.count() == 2


def test_spark_session_sql(spark_session):
    test_df = spark_session.createDataFrame([[1, 3], [2, 4]], "a: int, b: int")
    test_df.registerTempTable('test')

    test_filtered_df = spark_session.sql('SELECT a, b from test where a > 1')
    assert test_filtered_df.count() == 1


def test_spark_session_rename_columns(spark_session):
    source_data = [
        ("1", "1", "1"),
        ("2", "2", "2"),
        ("3", "3", "3")
    ]
    source_df = spark_session.createDataFrame(source_data, ["first", "second", "third"])

    actual_df = rename_columns(source_df,
                               {'first': 'third', 'second': 'first', 'third': 'second'})

    expected_data = [
        ("2", "2", "2"),
        ("1", "1", "1"),
        ("3", "3", "3")
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["third", "first", "second"])

    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_spark_session_list_filter(spark_session):
    source_data = [
        ("1", "a"),
        ("2", "c"),
        ("3", "b"),
        ("4", "f"),
        ("5", "e"),
        ("6", "d"),
        (None, "a")
    ]
    source_df = spark_session.createDataFrame(source_data, ["id", "letter"])

    letters = ['a', 'c', 'f']
    actual_df = list_filter(source_df, letters, "letter")

    expected_data = [
        ("1", "a"),
        ("2", "c"),
        ("4", "f"),
        (None, "a")
    ]
    expected_df = spark_session.createDataFrame(expected_data, ["id", "letter"])

    assert_df_equality(actual_df, expected_df)
