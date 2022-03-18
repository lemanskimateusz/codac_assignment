"""A setuptools based setup module.
See:
https://packaging.python.org/guides/distributing-packages-using-setuptools/
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / 'README.md').read_text(encoding='utf-8')

setup(

    name='codac-assignment',  # Required
    version='2.0.0',  # Required
    description='Pyspark assignment',  # Optional
    author='Mateusz Lemanski',  # Optional
    author_email='mateusz.lemanski@capgemini.com',  # Optional
    keywords='pyspark,bigdata,CI/CD',  # Optional
    py_modules=["main", "spark_test", "conftest"],
    python_requires='>=3.7, <4',
    data_files=[('data', ['dataset_one.csv', 'dataset_two.csv']),
                ('config_travis', ['.travis.yml']),
                ('requirements', ['requirements.txt'])],
    project_urls={  # Optional
        'Source': 'https://github.com/lemanskimateusz/codac_assignment',
    },
)
