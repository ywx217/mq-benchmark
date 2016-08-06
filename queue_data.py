import zlib
import msgpack
import base64


PACKED_DATA = base64.decodestring(open('_queue_data.base64').read())
STRUCTURE_DATA = msgpack.unpackb(zlib.decompress(PACKED_DATA))

# def get_packed_data():
    # import zlib
    # import msgpack
    # import _queue_data

    # return zlib.compress(msgpack.packb(_queue_data.data))


# def get_packed_data_str():
    # import base64

    # return base64.encodestring(get_packed_data())


if __name__ == '__main__':
    print len(PACKED_DATA)
    print type(STRUCTURE_DATA), len(STRUCTURE_DATA)

