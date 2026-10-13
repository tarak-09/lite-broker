import pytest

@pytest.mark.asyncio
async def test_api_send_receive_delete(client):
    resp = await client.post('/queues/apiq/messages', json={"body": "apimsg"})
    assert resp.status == 201
    data = await resp.json()
    assert "message_id" in data
    
    resp = await client.get('/queues/apiq/messages?max_messages=1')
    assert resp.status == 200
    data = await resp.json()
    assert len(data["messages"]) == 1
    msg = data["messages"][0]
    assert msg["body"] == "apimsg"
    receipt = msg["receipt_handle"]
    
    resp = await client.delete(f'/queues/apiq/messages/{receipt}')
    assert resp.status == 200
    
    resp = await client.get('/queues/apiq/messages?max_messages=1')
    data = await resp.json()
    assert len(data["messages"]) == 0

@pytest.mark.asyncio
async def test_api_change_visibility(client):
    await client.post('/queues/visq/messages', json={"body": "vismsg"})
    
    resp = await client.get('/queues/visq/messages?max_messages=1&visibility_timeout=100')
    data = await resp.json()
    receipt = data["messages"][0]["receipt_handle"]
    
    resp = await client.put(f'/queues/visq/messages/{receipt}/visibility', json={"visibility_timeout": 0.0})
    assert resp.status == 200
    
    resp = await client.get('/queues/visq/messages?max_messages=1')
    data = await resp.json()
    assert len(data["messages"]) == 1

@pytest.mark.asyncio
async def test_api_long_polling(client):
    import asyncio
    
    async def receive_req():
        resp = await client.get('/queues/lapiq/messages?max_messages=1&wait_time_seconds=2')
        assert resp.status == 200
        return await resp.json()
        
    task = asyncio.create_task(receive_req())
    await asyncio.sleep(0.2)
    
    resp = await client.post('/queues/lapiq/messages', json={"body": "lapimsg"})
    assert resp.status == 201
    
    data = await task
    assert len(data["messages"]) == 1
    assert data["messages"][0]["body"] == "lapimsg"
