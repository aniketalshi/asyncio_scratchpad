import argparse
import asyncio
import uuid
from read_write import read_msg, send_msg


async def main(args):

    id = uuid.uuid4().hex[:8]

    reader, writer = await asyncio.open_connection(args.host, args.port)
    print(f"I am a {writer.get_extra_info('sockname')}")

    chan = args.channel.encode()
    await send_msg(writer, chan)

    try:
        while True:
            data = await read_msg(reader)
            print(f"data recieved: {data} id: {id}")

        print(f"connection ended id: {id}")
    
    except asyncio.IncompleteReadError:
        print(f"server closed. id: {id}")
    
    finally:
        writer.close()
        await writer.wait_closed()
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=5000)
    parser.add_argument("--channel", default="/foo/activity")

    args = parser.parse_args()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("Bye!")
