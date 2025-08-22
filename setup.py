from setuptools import setup, find_packages

setup(
    name="argument-mining-db",
    version="0.1.0",
    packages=find_packages(),
    package_dir={"": "."},
    install_requires=[
        "SQLAlchemy>=1.4.0",
        "PyMySQL>=1.0.0",
        "mysql-connector-python>=8.0.0",
        "pandas>=1.5.0",
    ],
    python_requires=">=3.9",
)