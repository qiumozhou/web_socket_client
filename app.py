import json

import aioredis
from aiohttp import web

import socketio

try:
    from config.dev import REDIS_URL, TASK_QUEUE_NAME, CALLBACK_QUEUE_NAME, RPC_URL, \
        NICK_NAME_URL_TPL
except ImportError:
    from config.pro import REDIS_URL, TASK_QUEUE_NAME, CALLBACK_QUEUE_NAME, RPC_URL, \
        NICK_NAME_URL_TPL

mgr = socketio.AsyncRedisManager(REDIS_URL)
sio = socketio.AsyncServer(async_mode='aiohttp', client_manager=mgr)
app = web.Application()
sio.attach(app)


async def background_publish_task():
    emit_methods = {
        "/hanxin_status": hanxin_status_reply,
        "/charging_server_status": charging_status_reply,
        "/agent_monitor_all": agent_monitor_all_reply,
        "/bwms_server_status": bwms_server_status_reply,
        "/ros_node_status": ros_node_status_reply,
        "/pop_agents_dtc": pop_agents_dtc_reply,
    }
    conn = await aioredis.create_redis(REDIS_URL)
    channels = await conn.subscribe("bito_realtime_data")
    if len(channels) < 1:
        raise Exception("Subscribe 'bito_realtime_data' failed.")
    channel = channels[0]
    while await channel.wait_message():
        message = await channel.get_json()
        topic_name = message.get("topic_name", "")
        print(topic_name)
        func = emit_methods.get(topic_name)
        if not func:
            continue
        await func(message)


async def background_pc_ack_task():
    emit_methods = {
        "resume": resume_reply,
        "monitor": monitor_reply
    }
    conn = await aioredis.create_connection(REDIS_URL)
    while True:
        task_data = await conn.execute("BRPOP", TASK_QUEUE_NAME, 0)
        if len(task_data) != 2:
            continue
        data = json.loads(task_data[-1])
        if "key" not in data.keys():
            continue
        dit = {
            "data": data.get("value")
        }
        func = emit_methods.get(data["key"])
        await func(dit)


async def monitor_reply(data):
    await sio.emit('monitorReply', data)


async def resume_reply(data):
    await sio.emit('resumeReply', data)


async def hanxin_status_reply(data):
    await sio.emit("hanxinStatusReply", data)


async def agent_monitor_all_reply(data):
    await sio.emit("agentMonitorAllReply", data)


async def charging_status_reply(data):
    await sio.emit("chargingStatusReply", data)


async def bwms_server_status_reply(data):
    await sio.emit("bwmsServerStatusReply", data)


async def ros_node_status_reply(data):
    await sio.emit("rosNodeStatusReply", data)


async def pop_agents_dtc_reply(data):
    await sio.emit("popAgentsDtcReply", data)


@sio.event
async def connect(sid, environ):
    print("Client connect.", sid, environ)


@sio.on("resumeConfirm")
async def resume_confirm(sid, message):
    conn = await aioredis.create_connection(REDIS_URL)
    confirm_data = {
        "resume": message.get("data")
    }
    print("resume_confirm", sid, confirm_data)
    await conn.execute("LPUSH", CALLBACK_QUEUE_NAME, json.dumps(confirm_data))


@sio.on("resumeCancel")
async def resume_cancel(sid, message):
    conn = await aioredis.create_connection(REDIS_URL)
    confirm_data = {
        "resume": message.get("data")
    }
    print("resume_cancel", sid, confirm_data)
    await conn.execute("LPUSH", CALLBACK_QUEUE_NAME, json.dumps(confirm_data))


@sio.event
async def join(sid, message):
    print("close_room", sid, message)
    sio.enter_room(sid, message['room'])


@sio.event
async def leave(sid, message):
    print("close_room", sid, message)
    sio.leave_room(sid, message['room'])


@sio.event
async def close_room(sid, message):
    print("close_room", sid, message)
    await sio.close_room(message['room'])


@sio.event
async def disconnect_request(sid):
    await sio.disconnect(sid)


@sio.event
async def disconnect(sid):
    print('Client disconnected', sid)


async def handle(request):
    return web.Response(text="Hello World.")


if __name__ == '__main__':
    sio.start_background_task(background_publish_task)
    sio.start_background_task(background_pc_ack_task)
    web.run_app(app, host="0.0.0.0", port=8000)
