import hashlib
from typing import Callable
from fastapi import WebSocket
from functools import wraps
import json

from pydantic import BaseModel
from autogoal_remote.distributed.proxy import (
    dumps,
    loads,
)
from autogoal.utils import RestrictedWorkerWithState
from autogoal.utils._dynamic import dynamic_call


async def send_large_message(websocket: WebSocket, data: str, chunk_size: int):
    async def send(data):
        func = (
            websocket.send_text if hasattr(websocket, "send_text") else websocket.send
        )
        return await func(json.dumps(data))

    # Split the data into chunks
    chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]
    # Send the number of chunks to the client
    await send({"type": "chunk_count", "count": len(chunks)})
    # Send each chunk separately
    for chunk in chunks:
        await send({"type": "chunk", "data": chunk})


async def receive_large_message(websocket: WebSocket):
    async def receive():
        func = (
            websocket.receive_text
            if hasattr(websocket, "receive_text")
            else websocket.recv
        )
        return json.loads(await func())

    # Receive the number of chunks
    message = await receive()
    assert message["type"] == "chunk_count"
    chunk_count = message["count"]
    # Receive each chunk separately
    chunks = []
    for _ in range(chunk_count):
        message = await receive()
        assert message["type"] == "chunk"
        chunks.append(message["data"])
    # Reassemble the original message
    data = "".join(chunks)
    return data


def digest(string):
    if not isinstance(string, bytes):
        string = str(string).encode('utf8')
    return hashlib.sha1(string).digest()

def load_data_and_call_instance(inst, data: dict, algorithm_pool: dict):
    attr = getattr(inst, data["attr"])
    is_callable = hasattr(attr, "__call__")
    run_as_restricted = is_callable and data["attr"] == "run"

    args = loads(data["args"])
    kwargs = loads(data["kwargs"])

    func = (
        RestrictedWorkerWithState(
            dynamic_call, remote_call_timeout, remote_call_memory_limit
        )
        if run_as_restricted
        else dynamic_call
    )

    try:
        result = attr
        if is_callable:
            result = func(
                inst,
                data["attr"],
                *args,
                **kwargs,
            )

        if run_as_restricted:
            result, ninstance = result
            if ninstance is not None:
                algorithm_pool[id] = ninstance

        result_data = json.dumps({"result": dumps(result)})
    except Exception as e:
        result_data = json.dumps({"error": str(e)})

    return result_data

