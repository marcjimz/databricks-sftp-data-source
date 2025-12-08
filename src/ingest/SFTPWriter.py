"""
SFTP Writer implementation using Paramiko SSHv2
"""

import paramiko
import io
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SFTPWriter:
    """SFTP Writer using Paramiko for secure file transfers"""

    def __init__(
        self,
        host: str,
        username: str,
        port: int = 22,
        private_key_path: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30
    ):
        """
        Initialize SFTP Writer

        Args:
            host: SFTP server hostname
            username: SFTP username
            port: SFTP port (default: 22)
            private_key_path: Path to private SSH key file
            password: Password for authentication
            timeout: Connection timeout in seconds
        """
        self.host = host
        self.username = username
        self.port = port
        self.private_key_path = private_key_path
        self.password = password
        self.timeout = timeout
        self._client: Optional[paramiko.SSHClient] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    def connect(self) -> None:
        """Establish SFTP connection"""
        try:
            self._client = paramiko.SSHClient()
            self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Prepare authentication
            auth_kwargs = {
                "hostname": self.host,
                "port": self.port,
                "username": self.username,
                "timeout": self.timeout,
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
            logger.info(f"Connected to SFTP server: {self.host}")

        except Exception as e:
            logger.error(f"Failed to connect to SFTP server: {e}")
            raise

    def disconnect(self) -> None:
        """Close SFTP connection"""
        try:
            if self._sftp:
                self._sftp.close()
            if self._client:
                self._client.close()
            logger.info("Disconnected from SFTP server")
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")

    @contextmanager
    def session(self):
        """Context manager for SFTP session"""
        try:
            self.connect()
            yield self
        finally:
            self.disconnect()

    def write_dataframe(
        self,
        df: Any,
        remote_path: str,
        format: str = "csv",
        **kwargs
    ) -> None:
        """
        Write DataFrame to SFTP

        Args:
            df: Spark or Pandas DataFrame
            remote_path: Remote file path on SFTP server
            format: File format (csv, parquet, json)
            **kwargs: Additional format-specific options
        """
        if not self._sftp:
            raise RuntimeError("Not connected to SFTP server. Call connect() first.")

        try:
            # Convert to CSV bytes
            if hasattr(df, 'toPandas'):  # Spark DataFrame
                pdf = df.toPandas()
            else:  # Pandas DataFrame
                pdf = df

            # Create bytes buffer
            buffer = io.BytesIO()

            if format == "csv":
                csv_options = {
                    "index": False,
                    "header": kwargs.get("header", True),
                    "sep": kwargs.get("sep", ",")
                }
                pdf.to_csv(buffer, **csv_options)
            elif format == "json":
                json_options = {
                    "orient": kwargs.get("orient", "records"),
                    "lines": kwargs.get("lines", True)
                }
                pdf.to_json(buffer, **json_options)
            else:
                raise ValueError(f"Unsupported format: {format}")

            buffer.seek(0)

            # Ensure remote directory exists
            self._ensure_remote_dir(remote_path)

            # Write to SFTP
            self._sftp.putfo(buffer, remote_path)
            logger.info(f"Successfully wrote data to {remote_path}")

        except Exception as e:
            logger.error(f"Failed to write DataFrame to SFTP: {e}")
            raise

    def write_file(self, local_path: str, remote_path: str) -> None:
        """
        Upload file to SFTP

        Args:
            local_path: Local file path
            remote_path: Remote file path on SFTP server
        """
        if not self._sftp:
            raise RuntimeError("Not connected to SFTP server. Call connect() first.")

        try:
            self._ensure_remote_dir(remote_path)
            self._sftp.put(local_path, remote_path)
            logger.info(f"Successfully uploaded {local_path} to {remote_path}")
        except Exception as e:
            logger.error(f"Failed to upload file: {e}")
            raise

    def list_files(self, remote_dir: str = ".") -> list:
        """
        List files in remote directory

        Args:
            remote_dir: Remote directory path

        Returns:
            List of file names
        """
        if not self._sftp:
            raise RuntimeError("Not connected to SFTP server. Call connect() first.")

        try:
            return self._sftp.listdir(remote_dir)
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            raise

    def _ensure_remote_dir(self, remote_path: str) -> None:
        """Ensure remote directory exists"""
        dir_path = "/".join(remote_path.split("/")[:-1])
        if dir_path:
            try:
                self._sftp.stat(dir_path)
            except FileNotFoundError:
                self._mkdir_p(dir_path)

    def _mkdir_p(self, remote_dir: str) -> None:
        """Create remote directory recursively"""
        dirs = []
        while remote_dir and remote_dir != "/":
            dirs.append(remote_dir)
            remote_dir = "/".join(remote_dir.split("/")[:-1])

        for dir_path in reversed(dirs):
            try:
                self._sftp.stat(dir_path)
            except FileNotFoundError:
                self._sftp.mkdir(dir_path)


class SFTPDataSource:
    """
    Databricks Data Source API wrapper for SFTP
    """

    @staticmethod
    def create_writer(config: Dict[str, Any]) -> SFTPWriter:
        """
        Create SFTP writer from configuration

        Args:
            config: Dictionary with connection parameters

        Returns:
            SFTPWriter instance
        """
        required_keys = ["host", "username"]
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required configuration key: {key}")

        return SFTPWriter(
            host=config["host"],
            username=config["username"],
            port=config.get("port", 22),
            private_key_path=config.get("private_key_path"),
            password=config.get("password"),
            timeout=config.get("timeout", 30)
        )

    @staticmethod
    def write(df: Any, remote_path: str, config: Dict[str, Any], **kwargs) -> None:
        """
        Write DataFrame to SFTP (convenience method)

        Args:
            df: DataFrame to write
            remote_path: Remote file path
            config: Connection configuration
            **kwargs: Additional write options
        """
        writer = SFTPDataSource.create_writer(config)
        with writer.session():
            writer.write_dataframe(df, remote_path, **kwargs)
