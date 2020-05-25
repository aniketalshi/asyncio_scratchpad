from asyncio.streams import StreamWriter, StreamReader
from asyncio import Queue
from read_write import read_msg, send_msg
from typing import DefaultDict, Deque
from collections import defaultdict, deque
import asyncio
import logging
import socket


_PEER_NAME = "peername"

# clients who have subscribed to given channel
SUBSCRIBER_LIST: DefaultDict[bytes, Deque] = defaultdict(deque)

# to hold all the msgs to be sent on given channel
CHAN_QUEUE: DefaultDict[bytes, Queue] = defaultdict(Queue) 

async def make_client(reader: StreamReader, writer: StreamWriter):

    # get peer name
    peer_name = writer.get_extra_info(_PEER_NAME)
    print(f"Recieved request for peer: {peer_name}")
    # get subscriber channel and add to list
    subscriber_chan = await read_msg(reader)
    SUBSCRIBER_LIST[subscriber_chan].append(writer)

    try:
        while True:
            chan = await read_msg(reader)
            data = await read_msg(reader)
            print(f"sending data to channel: {chan}: {data}")

            connections = SUBSCRIBER_LIST[chan]
            await asyncio.gather(*[send_msg(conn, data) for conn in connections])

    except asyncio.CancelledError:
        print(f"remote peer closing : {peer_name}")
        writer.close()
        await writer.wait_closed()
    
    except asyncio.IncompleteReadError:
        print(f"remote peer {peer_name} disconnected")

    finally:
        print(f"remote peer {peer_name} closed")
        # remove our writer form channel list
        SUBSCRIBER_LIST[subscriber_chan].remove(writer)


async def main(*args, **kwargs):
    server = await asyncio.start_server(*args, **kwargs)
    async with server:
        await server.serve_forever()

try:
    asyncio.run(main(make_client, host="127.0.0.1", port=5000))
except KeyboardInterrupt:
    print('Bye!')
