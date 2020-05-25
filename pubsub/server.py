from asyncio.streams import StreamWriter, StreamReader
from asyncio import Queue
from read_write import read_msg, send_msg
from typing import DefaultDict, Dict, Deque
from collections import defaultdict, deque
from contextlib import suppress
import asyncio
import logging
import socket


PEER_NAME = "peername"
CHAN_QUEUE_SIZE = 10 # size of buffer to queue up the msgs for given channel

# writers who have subscribed to given channel
SUBSCRIBER_LIST: DefaultDict[bytes, Deque] = defaultdict(deque)
# to hold all the msgs to be sent on given channel
CHAN_QUEUE: Dict[bytes, Queue] = dict()
# queue per writer
SEND_QUEUE: DefaultDict[StreamWriter, Queue] = defaultdict(Queue)


async def make_client(reader: StreamReader, writer: StreamWriter):

    # get peer name
    peer_name = writer.get_extra_info(PEER_NAME)
    print(f"Recieved request for peer: {peer_name}")

    # get subscriber channel and add to list
    subscriber_chan = await read_msg(reader)

    # adding this writer to subscriber list as well
    SUBSCRIBER_LIST[subscriber_chan].append(writer)

    # spawn a async task for this writer to pull msgs off to queue and send to subscribers
    asyncio.create_task(msg_writer(writer, SEND_QUEUE[writer]))

    try:
        while True:
            chan = await read_msg(reader)
            if chan not in CHAN_QUEUE:
                # create a queue where we will enqueue msgs for given channel
                CHAN_QUEUE[chan] = Queue(maxsize=CHAN_QUEUE_SIZE)
                # spawn up new channel sender
                asyncio.create_task(channel_sender(chan))

            data = await read_msg(reader)

            """
            put the msg onto the queue for this channel
            channel sender will pull msg off this queue and then put it in 
            the respective writers queues
            """
            await CHAN_QUEUE[chan].put(data)

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

        # close async task for this writer
        await SEND_QUEUE[writer].put(None)


async def channel_sender(chan_name: bytes) -> None:
    """
    This piece decouples our main loop from writers for each channel.
    It pulls msg from channel queue, looks up all the subscribers in
    subscriber list. Then it enqueues this msg into each of subscriber's
    queue which will be then pulled off by each individual subscriber
    in its own async task.
    """
    with suppress(asyncio.CancelledError):
        while True:
            data = await CHAN_QUEUE[chan_name].get()

            # if explicitly sent None to close this worker
            if data is None:
                break

            # if no subscribers yet then sleep
            if len(SUBSCRIBER_LIST[chan_name]) == 0:
                asyncio.sleep(1.0)

            for writer in SUBSCRIBER_LIST[chan_name]:
                if not SEND_QUEUE[writer].full():
                    await SEND_QUEUE[writer].put(data)


async def msg_writer(writer: StreamWriter, queue: Queue) -> None:
    """
    Pulls msg off the queue and sends it to a subscriber. 
    There will be one msg_writer for each subscriber per channel
    """

    while True:
        try:
            data = await queue.get()
        except asyncio.CancelledError:
            # task will only be closed by recieving None on the queue
            continue
        
        if data is None:
            break
        
        try:
            await send_msg(writer, data)
        except asyncio.CancelledError: 
            # finish sending msg even if writer was cancelled
            await send_msg(writer, data)

async def main(*args, **kwargs):
    server = await asyncio.start_server(*args, **kwargs)
    async with server:
        await server.serve_forever()

try:
    asyncio.run(main(make_client, host="127.0.0.1", port=5000))
except KeyboardInterrupt:
    print('Bye!')
