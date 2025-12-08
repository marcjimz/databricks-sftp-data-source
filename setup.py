from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="databricks-sftp-datasource",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Custom SFTP data source for Databricks using Paramiko",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/databricks-sftp-data-source",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=[
        "paramiko==3.4.0",
        "pandas==2.1.4",
    ],
)
