"""
Databricks SFTP Data Source - Ingest Module
A custom data source for writing data to SFTP using Paramiko.
"""

from .SFTPWriter import SFTPWriter, SFTPDataSource

__version__ = "0.1.0"
__all__ = ["SFTPWriter", "SFTPDataSource"]
