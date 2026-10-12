import sqlite3

conn = sqlite3.connect(':memory:')
conn.execute('CREATE TABLE messages (id INTEGER PRIMARY KEY, q TEXT, v REAL, r TEXT)')
conn.execute('INSERT INTO messages (q, v) VALUES ("test", 0)')
conn.execute('INSERT INTO messages (q, v) VALUES ("test", 0)')

q = """
    UPDATE messages
    SET v = ?, r = ? || '-' || id
    WHERE id IN (
        SELECT id FROM messages WHERE q = ? AND v <= ? ORDER BY id LIMIT ?
    )
    RETURNING id, r
"""
cursor = conn.execute(q, (10, 'uuid', 'test', 5, 2))
print(cursor.fetchall())
