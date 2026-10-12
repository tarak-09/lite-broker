import aiosqlite
import time
import uuid
import logging
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)

class Broker:
    def __init__(self, db_path: str = "sqslite.db"):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        logger.info(f"Connecting to database at {self.db_path}")
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute('PRAGMA journal_mode=WAL')
        await self._conn.execute('PRAGMA synchronous=NORMAL')
        await self._init_db()

    async def close(self):
        if self._conn:
            await self._conn.close()

    async def _init_db(self):
        await self._conn.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                body TEXT NOT NULL,
                enqueued_at REAL NOT NULL,
                visible_after REAL NOT NULL,
                receipt_handle TEXT
            )
        ''')
        await self._conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_messages_queue_visibility 
            ON messages (queue_name, visible_after)
        ''')
        await self._conn.commit()

    async def send_message(self, queue_name: str, body: str, delay_seconds: float = 0.0) -> int:
        if not isinstance(queue_name, str) or not queue_name:
            raise ValueError("queue_name must be a non-empty string")
        if not isinstance(body, str):
            raise ValueError("body must be a string")
            
        now = time.time()
        visible_after = now + delay_seconds
        
        cursor = await self._conn.execute('''
            INSERT INTO messages (queue_name, body, enqueued_at, visible_after)
            VALUES (?, ?, ?, ?)
        ''', (queue_name, body, now, visible_after))
        await self._conn.commit()
        return cursor.lastrowid

    async def receive_messages(self, queue_name: str, max_messages: int = 1, visibility_timeout: float = 30.0) -> List[Dict[str, Any]]:
        now = time.time()
        new_visible_after = now + visibility_timeout
        receipt_base = str(uuid.uuid4())
        
        query = '''
            UPDATE messages
            SET visible_after = ?, receipt_handle = ? || '-' || id
            WHERE id IN (
                SELECT id FROM messages 
                WHERE queue_name = ? AND visible_after <= ?
                ORDER BY id
                LIMIT ?
            )
            RETURNING id, body, receipt_handle
        '''
        
        try:
            cursor = await self._conn.execute(
                query, 
                (new_visible_after, receipt_base, queue_name, now, max_messages)
            )
            rows = await cursor.fetchall()
            await self._conn.commit()
            
            return [
                {"id": row[0], "body": row[1], "receipt_handle": row[2]}
                for row in rows
            ]
        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
            await self._conn.rollback()
            raise

    async def delete_message(self, queue_name: str, receipt_handle: str) -> bool:
        cursor = await self._conn.execute('''
            DELETE FROM messages
            WHERE queue_name = ? AND receipt_handle = ?
        ''', (queue_name, receipt_handle))
        await self._conn.commit()
        return cursor.rowcount > 0

    async def change_message_visibility(self, queue_name: str, receipt_handle: str, visibility_timeout: float) -> bool:
        now = time.time()
        new_visible_after = now + visibility_timeout
        cursor = await self._conn.execute('''
            UPDATE messages
            SET visible_after = ?
            WHERE queue_name = ? AND receipt_handle = ?
        ''', (new_visible_after, queue_name, receipt_handle))
        await self._conn.commit()
        return cursor.rowcount > 0
