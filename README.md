# SQSLite

**SQSLite** is a lightweight, asynchronous HTTP message broker backed by SQLite. It implements core SQS-like semantics including message enqueuing, message dequeuing with configurable visibility timeouts, and explicit message acknowledgments (deletions).

## What it does
It provides a simple HTTP API to:
1. **Send Messages:** Enqueue string bodies to named queues (with optional initial delay).
2. **Receive Messages:** Dequeue one or more messages, making them temporarily invisible to other consumers for a set `visibility_timeout`.
3. **Change Visibility:** Adjust the visibility timeout of an inflight message (e.g., extend it if processing takes longer than expected).
4. **Delete Messages:** Explicitly acknowledge a message once it has been processed to remove it from the queue entirely.

SQSLite uses atomic SQL transactions powered by `UPDATE ... RETURNING` (available in modern SQLite) to safely handle concurrent queue polling without complex locking overhead. 

## Why it exists
Sometimes you need an SQS-like queuing mechanism for local development, integration testing, or a single-node application, but you don't want the overhead of deploying RabbitMQ, Redis, or an actual AWS SQS mock like LocalStack. SQSLite gives you those semantics using only Python and SQLite.

## Installation

Ensure you have Python 3.10+ installed.

```bash
# Clone the repository and install via pip
git clone <repository-url> sqslite
cd sqslite
python -m venv .venv
source .venv/bin/activate
pip install .
```

To install development dependencies (for running tests):
```bash
pip install -e ".[dev]"
```

## Usage

Start the broker server:

```bash
# Binds to 127.0.0.1:8080 by default
sqslite --host 127.0.0.1 --port 8080 --db my_queue.db
```

Or start it with debug logging:

```bash
sqslite --debug
```

### Worked Examples

**1. Send a Message**

Send a message to a queue named `myqueue`:

```bash
curl -X POST http://127.0.0.1:8080/queues/myqueue/messages \
     -H "Content-Type: application/json" \
     -d '{"body": "Hello World", "delay_seconds": 0}'
```
*Output:*
```json
{
  "message_id": 1,
  "status": "sent"
}
```

**2. Receive a Message**

Receive up to 1 message, making it invisible to other consumers for 30 seconds. You can optionally use HTTP Long Polling by providing `wait_time_seconds` (up to 20 seconds) to block the request until a message arrives:

```bash
curl "http://127.0.0.1:8080/queues/myqueue/messages?max_messages=1&visibility_timeout=30&wait_time_seconds=20"
```
*Output:*
```json
{
  "messages": [
    {
      "id": 1,
      "body": "Hello World",
      "receipt_handle": "f47ac10b-58cc-4372-a567-0e02b2c3d479-1"
    }
  ]
}
```

**3. Change Message Visibility**

If your worker needs more time, you can extend the visibility timeout:

```bash
# Assuming the receipt_handle from step 2
curl -X PUT http://127.0.0.1:8080/queues/myqueue/messages/f47ac10b-58cc-4372-a567-0e02b2c3d479-1/visibility \
     -H "Content-Type: application/json" \
     -d '{"visibility_timeout": 60}'
```
*Output:*
```json
{
  "status": "visibility updated"
}
```

**4. Delete a Message (Acknowledge)**

Once successfully processed, delete the message using its `receipt_handle` to prevent it from reappearing:

```bash
curl -X DELETE http://127.0.0.1:8080/queues/myqueue/messages/f47ac10b-58cc-4372-a567-0e02b2c3d479-1
```
*Output:*
```json
{
  "status": "deleted"
}
```

## Running Tests

SQSLite includes a comprehensive test suite covering the broker logic, the HTTP API, and concurrent atomic dequeues.

```bash
pytest
```
*Example Output:*
```text
============================= test session starts ==============================
...
tests/test_api.py ..                                                     [ 33%]
tests/test_broker.py ....                                                [100%]
============================== 6 passed in 0.24s ===============================
```
