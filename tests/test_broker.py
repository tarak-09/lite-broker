import pytest
import asyncio
from sqslite.broker import Broker

@pytest.mark.asyncio
async def test_send_and_receive(broker: Broker):
    await broker.send_message("testq", "hello")
    msgs = await broker.receive_messages("testq", max_messages=1)
    assert len(msgs) == 1
    assert msgs[0]["body"] == "hello"

@pytest.mark.asyncio
async def test_visibility_timeout(broker: Broker):
    await broker.send_message("testq", "hello")
    
    msgs = await broker.receive_messages("testq", max_messages=1, visibility_timeout=0.1)
    assert len(msgs) == 1
    
    msgs2 = await broker.receive_messages("testq", max_messages=1)
    assert len(msgs2) == 0
    
    await asyncio.sleep(0.15)
    
    msgs3 = await broker.receive_messages("testq", max_messages=1)
    assert len(msgs3) == 1
    assert msgs3[0]["id"] == msgs[0]["id"]

@pytest.mark.asyncio
async def test_delete_message(broker: Broker):
    await broker.send_message("testq", "hello")
    msgs = await broker.receive_messages("testq", max_messages=1)
    receipt = msgs[0]["receipt_handle"]
    
    success = await broker.delete_message("testq", receipt)
    assert success is True
    
    msgs2 = await broker.receive_messages("testq", max_messages=1)
    assert len(msgs2) == 0

@pytest.mark.asyncio
async def test_concurrent_dequeues(temp_db: str):
    brokers = [Broker(temp_db) for _ in range(5)]
    for b in brokers:
        await b.connect()
        
    try:
        for i in range(10):
            await brokers[0].send_message("testq", f"msg_{i}")
            
        async def receive_loop(b: Broker):
            received = []
            while True:
                msgs = await b.receive_messages("testq", max_messages=1, visibility_timeout=10.0)
                if not msgs:
                    break
                received.extend(msgs)
            return received
            
        results = await asyncio.gather(*(receive_loop(b) for b in brokers))
        
        all_received = []
        for r in results:
            all_received.extend(r)
            
        assert len(all_received) == 10
        ids = [m["id"] for m in all_received]
        assert len(set(ids)) == 10
    finally:
        for b in brokers:
            await b.close()

@pytest.mark.asyncio
async def test_long_polling_timeout(broker: Broker):
    import time
    start = time.time()
    msgs = await broker.receive_messages("test_lp", wait_time_seconds=0.5)
    duration = time.time() - start
    assert len(msgs) == 0
    assert duration >= 0.5
    assert duration < 1.0

@pytest.mark.asyncio
async def test_long_polling_success(broker: Broker):
    import time
    import asyncio
    
    async def receiver():
        return await broker.receive_messages("test_lp2", wait_time_seconds=2.0)
    
    task = asyncio.create_task(receiver())
    await asyncio.sleep(0.2)
    await broker.send_message("test_lp2", "msg1")
    
    msgs = await task
    assert len(msgs) == 1
    assert msgs[0]["body"] == "msg1"

@pytest.mark.asyncio
async def test_dlq_routing(broker: Broker):
    await broker.set_queue_config("testq_dlq", "my_dlq", 2)
    await broker.send_message("testq_dlq", "poison_message")
    
    # Receive 1 (count goes 0 -> 1)
    msgs = await broker.receive_messages("testq_dlq", max_messages=1, visibility_timeout=0.01)
    assert len(msgs) == 1
    assert msgs[0]["body"] == "poison_message"
    
    await asyncio.sleep(0.02)
    
    # Receive 2 (count goes 1 -> 2)
    msgs2 = await broker.receive_messages("testq_dlq", max_messages=1, visibility_timeout=0.01)
    assert len(msgs2) == 1
    assert msgs2[0]["id"] == msgs[0]["id"]
    
    await asyncio.sleep(0.02)
    
    # Receive 3 (count goes 2 -> 3 > 2), so it should route to DLQ and return empty
    msgs3 = await broker.receive_messages("testq_dlq", max_messages=1, visibility_timeout=0.01)
    assert len(msgs3) == 0
    
    # Message should be in my_dlq
    dlq_msgs = await broker.receive_messages("my_dlq", max_messages=1)
    assert len(dlq_msgs) == 1
    assert dlq_msgs[0]["body"] == "poison_message"
    assert dlq_msgs[0]["id"] == msgs[0]["id"]

@pytest.mark.asyncio
async def test_pubsub(broker: Broker):
    await broker.create_topic("my_topic")
    await broker.subscribe("my_topic", "queue1")
    await broker.subscribe("my_topic", "queue2")
    
    count = await broker.publish("my_topic", "fanout_msg")
    assert count == 2
    
    msgs1 = await broker.receive_messages("queue1", max_messages=1)
    assert len(msgs1) == 1
    assert msgs1[0]["body"] == "fanout_msg"
    
    msgs2 = await broker.receive_messages("queue2", max_messages=1)
    assert len(msgs2) == 1
    assert msgs2[0]["body"] == "fanout_msg"

@pytest.mark.asyncio
async def test_pubsub_no_subscribers(broker: Broker):
    await broker.create_topic("empty_topic")
    count = await broker.publish("empty_topic", "fanout_msg")
    assert count == 0
