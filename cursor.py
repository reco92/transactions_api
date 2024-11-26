import base64
import json


def encode_cursor(position, pk):
    cursor_tuple = (position, pk)

    cursor_json = json.dumps(cursor_tuple)
    cursor_encoded = base64.urlsafe_b64encode(cursor_json.encode()).decode()

    return cursor_encoded


def decode_cursor(cursor):
    cursor_json = base64.urlsafe_b64decode(cursor).decode()
    position, pk = json.loads(cursor_json)

    return position, pk