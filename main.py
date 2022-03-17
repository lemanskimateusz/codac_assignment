import pyspark

from pyspark.sql import functions as F

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("assignment.com") \
    .getOrCreate()

client_data = spark.read.option("header", True) \
    .csv("./dataset_one.csv")

# sys.argv[1]
financial_data = spark.read.option("header", True) \
    .csv("./dataset_two.csv")
# sys.argv[2]

countries = ["United Kingdom", "Netherlands"]

# sys.argv[3] convert to list





# filtering countries


@F.udf(returnType=BooleanType())
def list_filter(col1):
    return col1 in countries


client_data = client_data.filter(list_filter(client_data.country))

# dropping personal info
client_data = client_data.drop("first_name", "last_name", "country")
financial_data = financial_data.drop("cc_n")

# join

joined_data = client_data.join(financial_data, ["id"], "right")

joined_data.printSchema()


# rename

def rename_columns(df, columns):
    if isinstance(columns, dict):
        return df.select(*[F.col(col_name).alias(columns.get(col_name, col_name)) for col_name in df.columns])
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")


joined_data = rename_columns(joined_data,
                             {'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'})
joined_data.printSchema()
joined_data.show(n=10)

# joined_data.write.csv("./client_data/output.csv")

joined_data.toPandas().to_csv("./client_data/output.csv", index=False)
