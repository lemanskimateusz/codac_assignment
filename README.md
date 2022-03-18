codac_assignment
======

[![Python 3.7](https://img.shields.io/badge/python-3.7-green.svg)](https://www.python.org/downloads/release/python-370/)
[![Spark 3.1.2](https://img.shields.io/badge/spark-3.1.2-green)](https://spark.apache.org/releases/spark-release-3-1-2.html)
[![Java 8](https://img.shields.io/badge/Java-8-green)](https://www.oracle.com/pl/java/technologies/javase/jav)

Spark application for merging client data with their financial data from listed countries by user.

### Actions:
* filters data, based on given countries list
* drops columns with personal data
* joins given two dataset on column 'id'
* renames columns for better readability

### How to run spark application?
1. Setup your spark+python environment to which you will extract the project
2. Install requirements
>pip install -r requirements.txt
3. Run tests
>pytest
4. Submit spark job with 3 arguments:
- path to client data
- path to financial data
- write countries to filter using quotes and commas. Remember to not put space after comma.

>submit-spark main.py ./dataset_one.csv ./dataset_two.csv "United Kingdom,Netherlands"

5. Check the result in client_data/output.csv
6. Create source distribution file
>python setup.py sdist
>