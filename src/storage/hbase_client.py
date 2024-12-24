import happybase
import logging
from typing import Dict, Optional
from datetime import datetime





class HBaseClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 9090,
        table_name: str = "reddit_sentiments",
    ):
        self.host = host
        self.port = port
        self.table_name = table_name
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self.table = None
        self.connect()

    def connect(self) -> None:
        try:
            self.connection = happybase.Connection(self.host, self.port)
            self.ensure_table()
            self.table = self.connection.table(self.table_name)
        except Exception as e:
            self.logger.error(f"Failed to connect to HBase: {e}")
            raise

    def ensure_table(self) -> None:
        try:
            tables = self.connection.tables()
            if self.table_name.encode() not in tables:
                column_families = {
                    "post_data": dict(),
                    "sentiment": dict(),
                    "metadata": dict(),
                }
                self.connection.create_table(self.table_name, column_families)
        except Exception as e:
            self.logger.error(f"Failed to create table: {e}")
            raise

    def store_sentiment(self, row_key: str, data: Dict) -> None:
        try:
            self.table.put(
                row_key.encode(),
                {
                    b"post_data:text": str(data.get("text", "")).encode(),
                    b"sentiment:label": str(data.get("label", "")).encode(),
                    b"sentiment:score": str(data.get("score", 0.0)).encode(),
                    b"metadata:timestamp": str(datetime.now().timestamp()).encode(),
                },
            )
        except Exception as e:
            self.logger.error(f"Failed to store data: {e}")
            raise

    def get_sentiment(self, row_key: str) -> Optional[Dict]:
        try:
            row = self.table.row(row_key.encode())
            if not row:
                return None

            return {
                "text": row[b"post_data:text"].decode(),
                "label": row[b"sentiment:label"].decode(),
                "score": float(row[b"sentiment:score"].decode()),
                "timestamp": float(row[b"metadata:timestamp"].decode()),
            }
        except Exception as e:
            self.logger.error(f"Failed to retrieve data: {e}")
            return None

    def scan_recent(self, limit: int = 100) -> list:
        try:
            results = []
            for key, row in self.table.scan(limit=limit):
                results.append(
                    {
                        "id": key.decode(),
                        "text": row[b"post_data:text"].decode(),
                        "label": row[b"sentiment:label"].decode(),
                        "score": float(row[b"sentiment:score"].decode()),
                        "timestamp": float(row[b"metadata:timestamp"].decode()),
                    }
                )
            return results
        except Exception as e:
            self.logger.error(f"Failed to scan data: {e}")
            return []
