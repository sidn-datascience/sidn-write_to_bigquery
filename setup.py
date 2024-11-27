from setuptools import setup, find_packages

setup(
    name = 'write_to_bq',
    version = '1.1.0',
    packages = find_packages(),
    description = "Sends Pandas DataFrame content to a Bigquery table",
    python_requires=">=3.9",
    install_requires=[
        "google-cloud-bigquery",
        "pyarrow",
        "pandas",
        "setuptools >= 61.0"
    ],
    classifiers=[
        "Programming Language :: Python :: 3"
    ]
)