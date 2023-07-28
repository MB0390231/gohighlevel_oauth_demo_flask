import sqlite3
import threading
from typing import Dict


class SQLiteDB:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SQLiteDB, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, db_name: str = "database.db"):
        self.local_storage = threading.local()
        self.db_name = db_name

    @property
    def conn(self):
        if not hasattr(self.local_storage, "conn"):
            self.local_storage.conn = sqlite3.connect(self.db_name)
            self.create_table()
        return self.local_storage.conn

    def create_table(self):
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS api_data (
                userType TEXT,
                companyId TEXT,
                locationId TEXT,
                access_token TEXT,
                token_type TEXT,
                expires_in INTEGER,
                refresh_token TEXT,
                scope TEXT,
                hashedCompanyId TEXT,
                PRIMARY KEY(userType, companyId, locationId)
            );
            """
        )
        self.conn.commit()

    def insert_or_update_token(self, data: Dict):
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO api_data (userType, companyId, locationId, access_token) 
            VALUES (:userType, :companyId, :locationId, :access_token)
            ON CONFLICT(userType, companyId, locationId) 
            DO UPDATE SET access_token = :access_token
            """,
            {
                'userType': data['userType'],
                'companyId': data['companyId'],
                'locationId': data['locationId'],
                'access_token': data['access_token'],
            },
        )
        self.conn.commit()
        print("Access token saved to database")
