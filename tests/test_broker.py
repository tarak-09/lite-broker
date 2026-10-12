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
