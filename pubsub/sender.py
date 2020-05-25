import argparse
import asyncio
import uuid
from read_write import read_msg, send_msg
from itertools import count


async def main(args):

    id = uuid.uuid4().hex[:8]

    reader, writer = await asyncio.open_connection(args.host, args.port)
    print(f"I am a {writer.get_extra_info('sockname')}")

    chan = args.channel.encode()
    await send_msg(writer, chan)

    try:
        for i in count():

            await asyncio.sleep(args.interval)
            data = b'X' * args.datasize

            print(f"sending msg {i} from id: {id}")

            await send_msg(writer, chan)
            await send_msg(writer, data)
    except asyncio.CancelledError: 
        writer.close()
        await writer.wait_closed()
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=5000, type=int)
    parser.add_argument("--channel", default="/foo/activity")
    parser.add_argument("--interval", default=1.0, type=float)
    parser.add_argument("--datasize", default=10, type=int)

    args = parser.parse_args()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        log.info("Bye!")
