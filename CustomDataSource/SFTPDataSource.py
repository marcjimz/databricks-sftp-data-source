"""
SFTP Data Source implementation using Databricks Python Data Source API
Uses Paramiko for SFTP operations
"""

from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType
from typing import Iterator, List
from dataclasses import dataclass
import os


@dataclass
class SFTPCommitMessage(WriterCommitMessage):
    """Commit message containing partition write metadata"""
    partition_id: int
    row_count: int
    file_path: str


class SFTPWriter(DataSourceWriter):
    """
    SFTP Data Source Writer using Paramiko

    Options:
        host: SFTP server hostname
        username: SFTP username
        private_key_content: SSH private key content (recommended for distributed writes)
        private_key_path: Path to SSH private key file (alternative to private_key_content)
        password: Password for authentication (alternative to private_key)
        port: SFTP port (default: 22)
        path: Remote path to write to
        format: File format (csv, json, parquet) - default: csv
        header: Include header for CSV (default: true)
    """

    def __init__(self, options):
        self.options = options
        self.host = options.get("host")
        self.username = options.get("username")
        self.private_key_content = options.get("private_key_content")
        self.private_key_path = options.get("private_key_path")
        self.password = options.get("password")
        self.port = int(options.get("port", "22"))
        self.path = options.get("path")
        self.format = options.get("format", "csv")
        self.header = options.get("header", "true").lower() == "true"

        # Validate required options
        assert self.host is not None, "Option 'host' is required"
        assert self.username is not None, "Option 'username' is required"
        assert self.path is not None, "Option 'path' is required"
        assert self.private_key_content or self.private_key_path or self.password, \
            "Either 'private_key_content', 'private_key_path', or 'password' is required"

    def write(self, iterator: Iterator) -> SFTPCommitMessage:
        """
        Write partition data to SFTP.

        Args:
            iterator: Iterator of Row objects

        Returns:
            SFTPCommitMessage with write metadata
        """
        # Import libraries within method (required for serialization)
        import paramiko
        import pandas as pd
        import io
        from pyspark import TaskContext

        # Get partition context
        context = TaskContext.get()
        partition_id = context.partitionId()

        # Convert iterator to list for processing
        rows = list(iterator)
        row_count = len(rows)

        if row_count == 0:
            # No data to write for this partition
            return SFTPCommitMessage(
                partition_id=partition_id,
                row_count=0,
                file_path=""
            )

        # Convert rows to pandas DataFrame
        # Extract column names from first row
        if hasattr(rows[0], '__fields__'):
            columns = rows[0].__fields__
        else:
            columns = rows[0].asDict().keys()

        data = [row.asDict() if hasattr(row, 'asDict') else dict(zip(columns, row)) for row in rows]
        pdf = pd.DataFrame(data)

        # Generate partition file path
        base_name = os.path.basename(self.path)
        dir_name = os.path.dirname(self.path) or "."
        name_without_ext = os.path.splitext(base_name)[0]
        ext = os.path.splitext(base_name)[1] or f".{self.format}"

        partition_file = f"{name_without_ext}_part{partition_id:05d}{ext}"
        remote_path = os.path.join(dir_name, partition_file)

        # Create SFTP connection and write
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        sftp = None
        temp_key_file = None

        try:
            # Connect with private key or password
            auth_kwargs = {
                "hostname": self.host,
                "port": self.port,
                "username": self.username,
                "timeout": 30
            }

            if self.private_key_content:
                # Create temporary key file on executor
                import tempfile
                temp_key_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_sftp_key')
                temp_key_file.write(self.private_key_content)
                temp_key_file.close()
                os.chmod(temp_key_file.name, 0o600)

                private_key = paramiko.RSAKey.from_private_key_file(temp_key_file.name)
                auth_kwargs["pkey"] = private_key
            elif self.private_key_path:
                private_key = paramiko.RSAKey.from_private_key_file(self.private_key_path)
                auth_kwargs["pkey"] = private_key
            else:
                auth_kwargs["password"] = self.password

            client.connect(**auth_kwargs)
            sftp = client.open_sftp()

            # Ensure remote directory exists
            self._ensure_remote_dir(sftp, dir_name)

            # Write data to buffer
            buffer = io.BytesIO()

            if self.format == "csv":
                pdf.to_csv(buffer, index=False, header=self.header)
            elif self.format == "json":
                pdf.to_json(buffer, orient="records", lines=True)
            else:
                raise ValueError(f"Unsupported format: {self.format}")

            buffer.seek(0)

            # Upload to SFTP
            sftp.putfo(buffer, remote_path)

            # Clean up
            sftp.close()
            client.close()

            # Clean up temporary key file if created
            if temp_key_file and os.path.exists(temp_key_file.name):
                os.remove(temp_key_file.name)

            return SFTPCommitMessage(
                partition_id=partition_id,
                row_count=row_count,
                file_path=remote_path
            )

        except Exception as e:
            # Clean up connections on error
            try:
                if sftp:
                    sftp.close()
            except:
                pass
            try:
                if client:
                    client.close()
            except:
                pass

            # Clean up temporary key file if created
            try:
                if temp_key_file and os.path.exists(temp_key_file.name):
                    os.remove(temp_key_file.name)
            except:
                pass

            raise RuntimeError(f"Failed to write partition {partition_id} to SFTP: {e}")

    def commit(self, messages: List[SFTPCommitMessage]) -> None:
        """
        Called when all partition writes succeed.

        Args:
            messages: List of commit messages from all partitions
        """
        total_rows = sum(msg.row_count for msg in messages)
        total_files = len([msg for msg in messages if msg.row_count > 0])

        print(f"✓ SFTP write committed successfully")
        print(f"  Total rows written: {total_rows}")
        print(f"  Total partition files: {total_files}")
        print(f"  Remote path: {self.path}")

        # List written files
        if total_files > 0:
            print(f"  Written files:")
            for msg in messages:
                if msg.row_count > 0:
                    print(f"    - {msg.file_path} ({msg.row_count} rows)")

    def abort(self, messages: List[SFTPCommitMessage]) -> None:
        """
        Called when some partition writes fail.

        Args:
            messages: List of commit messages from successful partitions
        """
        successful = len(messages)
        print(f"✗ SFTP write aborted")
        print(f"  Successful partitions: {successful}")
        print(f"  Note: Successful partition files may remain on SFTP server")

    def _ensure_remote_dir(self, sftp, remote_dir: str) -> None:
        """Ensure remote directory exists"""
        if not remote_dir or remote_dir == ".":
            return

        dirs = []
        current = remote_dir
        while current and current != "/":
            dirs.append(current)
            current = os.path.dirname(current)

        for dir_path in reversed(dirs):
            try:
                sftp.stat(dir_path)
            except FileNotFoundError:
                sftp.mkdir(dir_path)


class SFTPConnectionTester:
    """
    Utility class for testing SFTP connections.

    This is separate from the Spark DataSource API and used for connection verification.
    """

    def __init__(self, host: str, username: str, private_key_path: str = None,
                 password: str = None, port: int = 22):
        """
        Initialize SFTP connection tester.

        Args:
            host: SFTP server hostname
            username: SFTP username
            private_key_path: Path to SSH private key file
            password: Password for authentication (alternative to private_key)
            port: SFTP port (default: 22)
        """
        self.host = host
        self.username = username
        self.private_key_path = private_key_path
        self.password = password
        self.port = port
        self._client = None
        self._sftp = None

    def connect(self):
        """Establish SFTP connection"""
        import paramiko

        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        auth_kwargs = {
            "hostname": self.host,
            "port": self.port,
            "username": self.username,
            "timeout": 30
        }

        if self.private_key_path:
            private_key = paramiko.RSAKey.from_private_key_file(self.private_key_path)
            auth_kwargs["pkey"] = private_key
        elif self.password:
            auth_kwargs["password"] = self.password
        else:
            raise ValueError("Either private_key_path or password must be provided")

        self._client.connect(**auth_kwargs)
        self._sftp = self._client.open_sftp()
        return self

    def disconnect(self):
        """Close SFTP connection"""
        if self._sftp:
            self._sftp.close()
        if self._client:
            self._client.close()

    def __enter__(self):
        """Context manager entry"""
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()

    def list_files(self, remote_dir: str = "."):
        """
        List files in remote directory.

        Args:
            remote_dir: Remote directory path

        Returns:
            List of file names
        """
        if not self._sftp:
            raise RuntimeError("Not connected. Call connect() first or use context manager.")
        return self._sftp.listdir(remote_dir)


class SFTPDataSource(DataSource):
    """
    SFTP Data Source for Databricks

    This class implements the Databricks Python Data Source API for SFTP writes.
    For connection testing, use SFTPConnectionTester instead.

    Example usage:
        # Register the data source
        spark.dataSource.register(SFTPDataSource)

        # Write DataFrame to SFTP
        df.write \\
            .format("sftp") \\
            .option("host", "sftp.example.com") \\
            .option("username", "user") \\
            .option("private_key_path", "/path/to/key") \\
            .option("path", "/remote/data.csv") \\
            .mode("overwrite") \\
            .save()
    """

    @classmethod
    def name(cls) -> str:
        """Return the name used in format() calls"""
        return "sftp"

    def schema(self) -> str:
        """Return the schema (not used for write-only source)"""
        return "id int, data string"

    def writer(self, schema: StructType, overwrite: bool):
        """
        Create a writer for batch writes.

        Args:
            schema: Schema of the DataFrame being written
            overwrite: Whether to overwrite existing data

        Returns:
            SFTPWriter instance
        """
        return SFTPWriter(self.options)
