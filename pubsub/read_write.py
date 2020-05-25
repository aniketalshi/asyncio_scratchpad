from asyncio import StreamWriter, StreamReader

# number of bytes depicting header
_NUM_BYTES_HEADER = 4
_BYTE_ORDER = 'big'


async def read_msg(stream: StreamReader) -> bytes:
    """
    Reads first 4 bytes to get size of payload. Then
    reads payload and returns the data.
    """
    size_bytes = await stream.readexactly(_NUM_BYTES_HEADER)
    payload_size = int.from_bytes(size_bytes, byteorder=_BYTE_ORDER)
    return await stream.readexactly(payload_size)


async def send_msg(stream: StreamWriter, data: bytes) -> None:
    """
    encodes the size of payload into its header and then sends combined
    header and payload as a msg on wire.
    """
    size_bytes = len(data).to_bytes(_NUM_BYTES_HEADER, byteorder=_BYTE_ORDER)
    stream.writelines([size_bytes, data])
    await stream.drain()