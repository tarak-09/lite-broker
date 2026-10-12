from aiohttp import web
from sqslite.broker import Broker
import logging
import json

logger = logging.getLogger(__name__)

def create_app(db_path: str = "sqslite.db") -> web.Application:
    app = web.Application()
    broker = Broker(db_path)
    
    async def init_broker(app):
        await broker.connect()
        app['broker'] = broker

    async def close_broker(app):
        await broker.close()

    app.on_startup.append(init_broker)
    app.on_cleanup.append(close_broker)
    
    app.add_routes([
        web.post('/queues/{queue_name}/messages', send_message),
        web.get('/queues/{queue_name}/messages', receive_messages),
        web.delete('/queues/{queue_name}/messages/{receipt_handle}', delete_message),
        web.put('/queues/{queue_name}/messages/{receipt_handle}/visibility', change_visibility)
    ])
    
    return app

async def send_message(request: web.Request) -> web.Response:
    broker: Broker = request.app['broker']
    queue_name = request.match_info['queue_name']
    
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return web.json_response({"error": "Invalid JSON body"}, status=400)
        
    if 'body' not in data:
        return web.json_response({"error": "Missing 'body' in request"}, status=400)
        
    delay_seconds = float(data.get('delay_seconds', 0.0))
    
    try:
        msg_id = await broker.send_message(queue_name, data['body'], delay_seconds)
        return web.json_response({"message_id": msg_id, "status": "sent"}, status=201)
    except ValueError as e:
        return web.json_response({"error": str(e)}, status=400)
    except Exception as e:
        logger.exception("Error sending message")
        return web.json_response({"error": "Internal server error"}, status=500)

async def receive_messages(request: web.Request) -> web.Response:
    broker: Broker = request.app['broker']
    queue_name = request.match_info['queue_name']
    
    try:
        max_messages = int(request.query.get('max_messages', 1))
        visibility_timeout = float(request.query.get('visibility_timeout', 30.0))
    except ValueError:
        return web.json_response({"error": "Invalid query parameters"}, status=400)
        
    if max_messages < 1 or max_messages > 10:
        return web.json_response({"error": "max_messages must be between 1 and 10"}, status=400)
        
    messages = await broker.receive_messages(queue_name, max_messages, visibility_timeout)
    return web.json_response({"messages": messages}, status=200)

async def delete_message(request: web.Request) -> web.Response:
    broker: Broker = request.app['broker']
    queue_name = request.match_info['queue_name']
    receipt_handle = request.match_info['receipt_handle']
    
    success = await broker.delete_message(queue_name, receipt_handle)
    if success:
        return web.json_response({"status": "deleted"}, status=200)
    else:
        return web.json_response({"error": "Message not found or already deleted"}, status=404)

async def change_visibility(request: web.Request) -> web.Response:
    broker: Broker = request.app['broker']
    queue_name = request.match_info['queue_name']
    receipt_handle = request.match_info['receipt_handle']
    
    try:
        data = await request.json()
        visibility_timeout = float(data['visibility_timeout'])
    except (json.JSONDecodeError, KeyError, ValueError):
        return web.json_response({"error": "Invalid or missing 'visibility_timeout'"}, status=400)
        
    success = await broker.change_message_visibility(queue_name, receipt_handle, visibility_timeout)
    if success:
        return web.json_response({"status": "visibility updated"}, status=200)
    else:
        return web.json_response({"error": "Message not found or receipt handle invalid"}, status=404)
