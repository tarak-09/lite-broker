import aiosqlite
import asyncio
import time
import uuid
import logging
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)

class Broker:
    def __init__(self, db_path: str = "sqslite.db"):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None
        self._events: Dict[str, asyncio.Event] = {}

    def _get_event(self, queue_name: str) -> asyncio.Event:
        if queue_name not in self._events:
            self._events[queue_name] = asyncio.Event()
        return self._events[queue_name]


    async def connect(self):
        logger.info(f"Connecting to database at {self.db_path}")
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute('PRAGMA journal_mode=WAL')
        await self._conn.execute('PRAGMA synchronous=NORMAL')
        await self._conn.execute('PRAGMA foreign_keys=ON')
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
                receipt_handle TEXT,
                receive_count INTEGER DEFAULT 0
            )
        ''')
        
        # Add receive_count if it doesn't exist (for existing DBs)
        cursor = await self._conn.execute('PRAGMA table_info(messages)')
        columns = [row[1] for row in await cursor.fetchall()]
        if 'receive_count' not in columns:
            await self._conn.execute('ALTER TABLE messages ADD COLUMN receive_count INTEGER DEFAULT 0')

        await self._conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_messages_queue_visibility 
            ON messages (queue_name, visible_after)
        ''')
        
        await self._conn.execute('''
            CREATE TABLE IF NOT EXISTS queues (
                queue_name TEXT PRIMARY KEY,
                dlq_name TEXT NOT NULL,
                max_receive_count INTEGER NOT NULL
            )
        ''')

        await self._conn.execute('''
            CREATE TABLE IF NOT EXISTS topics (
                topic_name TEXT PRIMARY KEY
            )
        ''')

        await self._conn.execute('''
            CREATE TABLE IF NOT EXISTS subscriptions (
                topic_name TEXT,
                queue_name TEXT,
                PRIMARY KEY (topic_name, queue_name),
                FOREIGN KEY (topic_name) REFERENCES topics(topic_name) ON DELETE CASCADE
            )
        ''')
        await self._conn.commit()

    async def set_queue_config(self, queue_name: str, dlq_name: str, max_receive_count: int):
        await self._conn.execute('''
            INSERT INTO queues (queue_name, dlq_name, max_receive_count)
            VALUES (?, ?, ?)
            ON CONFLICT(queue_name) DO UPDATE SET
                dlq_name=excluded.dlq_name,
                max_receive_count=excluded.max_receive_count
        ''', (queue_name, dlq_name, max_receive_count))
        await self._conn.commit()

    async def create_topic(self, topic_name: str):
        if not isinstance(topic_name, str) or not topic_name:
            raise ValueError("topic_name must be a non-empty string")
        await self._conn.execute('''
            INSERT OR IGNORE INTO topics (topic_name) VALUES (?)
        ''', (topic_name,))
        await self._conn.commit()

    async def subscribe(self, topic_name: str, queue_name: str):
        if not isinstance(topic_name, str) or not topic_name:
            raise ValueError("topic_name must be a non-empty string")
        if not isinstance(queue_name, str) or not queue_name:
            raise ValueError("queue_name must be a non-empty string")
        
        # Verify topic exists, though foreign key would also catch it if enabled,
        # but INSERT OR IGNORE won't throw if it fails foreign key silently or similar.
        # Actually with foreign keys ON it throws an integrity error if topic_name doesn't exist.
        try:
            await self._conn.execute('''
                INSERT OR IGNORE INTO subscriptions (topic_name, queue_name)
                VALUES (?, ?)
            ''', (topic_name, queue_name))
            await self._conn.commit()
        except aiosqlite.IntegrityError:
            raise ValueError(f"Topic '{topic_name}' does not exist")

    async def publish(self, topic_name: str, body: str, delay_seconds: float = 0.0) -> int:
        if not isinstance(topic_name, str) or not topic_name:
            raise ValueError("topic_name must be a non-empty string")
        if not isinstance(body, str):
            raise ValueError("body must be a string")
            
        now = time.time()
        visible_after = now + delay_seconds
        
        cursor = await self._conn.execute('''
            SELECT queue_name FROM subscriptions WHERE topic_name = ?
        ''', (topic_name,))
        queues = [row[0] for row in await cursor.fetchall()]
        
        if not queues:
            return 0
            
        published_count = 0
        for queue_name in queues:
            await self._conn.execute('''
                INSERT INTO messages (queue_name, body, enqueued_at, visible_after)
                VALUES (?, ?, ?, ?)
            ''', (queue_name, body, now, visible_after))
            event = self._get_event(queue_name)
            event.set()
            event.clear()
            published_count += 1
            
        await self._conn.commit()
        return published_count

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
        
        event = self._get_event(queue_name)
        event.set()
        event.clear()
        
        return cursor.lastrowid

    async def receive_messages(self, queue_name: str, max_messages: int = 1, visibility_timeout: float = 30.0, wait_time_seconds: float = 0.0) -> List[Dict[str, Any]]:
        start_time = time.time()
        deadline = start_time + wait_time_seconds
        
        while True:
            now = time.time()
            new_visible_after = now + visibility_timeout
            receipt_base = str(uuid.uuid4())
            
            query = '''
                UPDATE messages
                SET 
                    visible_after = CASE 
                        WHEN receive_count + 1 > ifnull((SELECT max_receive_count FROM queues WHERE queue_name = messages.queue_name), 999999999) THEN ? 
                        ELSE ? 
                    END,
                    queue_name = CASE
                        WHEN receive_count + 1 > ifnull((SELECT max_receive_count FROM queues WHERE queue_name = messages.queue_name), 999999999) THEN (SELECT dlq_name FROM queues WHERE queue_name = messages.queue_name)
                        ELSE queue_name
                    END,
                    receipt_handle = CASE
                        WHEN receive_count + 1 > ifnull((SELECT max_receive_count FROM queues WHERE queue_name = messages.queue_name), 999999999) THEN NULL
                        ELSE ? || '-' || id
                    END,
                    receive_count = CASE
                        WHEN receive_count + 1 > ifnull((SELECT max_receive_count FROM queues WHERE queue_name = messages.queue_name), 999999999) THEN 0
                        ELSE receive_count + 1
                    END
                WHERE id IN (
                    SELECT id FROM messages 
                    WHERE queue_name = ? AND visible_after <= ?
                    ORDER BY id
                    LIMIT ?
                )
                RETURNING id, body, receipt_handle, queue_name
            '''
            
            try:
                cursor = await self._conn.execute(
                    query, 
                    (now, new_visible_after, receipt_base, queue_name, now, max_messages)
                )
                rows = await cursor.fetchall()
                await self._conn.commit()
                
                messages = []
                dlq_notifications = set()
                
                for row in rows:
                    msg_id, body, receipt_handle, updated_queue_name = row
                    if updated_queue_name != queue_name:
                        # Routed to DLQ
                        dlq_notifications.add(updated_queue_name)
                    else:
                        messages.append({"id": msg_id, "body": body, "receipt_handle": receipt_handle})
                        
                for dlq_name in dlq_notifications:
                    event = self._get_event(dlq_name)
                    event.set()
                    event.clear()
                
                if messages or wait_time_seconds <= 0:
                    return messages
                    
                remaining_time = deadline - time.time()
                if remaining_time <= 0:
                    return []
                    
                event = self._get_event(queue_name)
                try:
                    await asyncio.wait_for(event.wait(), timeout=remaining_time)
                except asyncio.TimeoutError:
                    return []
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
