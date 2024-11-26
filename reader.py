import ast
import csv
import random
from io import StringIO

CHUNK_SIZE=500


def safe_literal_eval(value):
    try:
        return ast.literal_eval(value)
    except (ValueError, SyntaxError):
        return str(value)


def serialize_row(headers, row):
    reader = csv.reader(StringIO(row))
    row = next(reader)
    typed_row = []
    for field in row:
        if field in ['', None]:
            typed_row.append(None)
        else:
            typed_row.append(safe_literal_eval(field))

    return dict(zip(headers, typed_row))


def read_random_rows(file_path, chunk=CHUNK_SIZE, has_header=True):
    """Read from a file randomly chunk number lines"""
    with open(file_path, 'r', encoding='utf-8') as file:
        header = None
        if has_header:
            header = file.readline()
        
        file.seek(0, 2)
        file_size = file.tell()
        
        random_rows = []
        for _ in range(chunk):
            while True:
                file.seek(random.randint(0, file_size - 1))                
                file.readline()
                
                line = file.readline()
                
                if line:
                    random_rows.append(line.strip())
                    break
        
        return header, random_rows


def read_positional_row(file_path, start_row, chunk=CHUNK_SIZE, has_header=True):
    """Start reading from a row number the next chunk lines"""
    with open(file_path, 'r', encoding='utf-8') as file:
        header = None
        if has_header:
            header = file.readline()
        
        for _ in range(start_row):
            if not file.readline():
                return header, []
        
        rows = []
        for _ in range(chunk):
            line = file.readline()
            if not line:
                break
            rows.append(line.strip())
        
        return header, rows


def generate_random_rows(file_path, chunk=CHUNK_SIZE):
    headers, rows = read_random_rows(file_path, chunk)
    fields = headers.split(',')
    fields = list(map(str.strip, fields))
    return [ serialize_row(fields, row) for row in rows]


def get_paginated_rows(file_path, start_row, chunk=CHUNK_SIZE):
    headers, rows = read_positional_row(file_path, start_row, chunk)
    fields = headers.split(',')
    fields = list(map(str.strip, fields))
    return [ serialize_row(fields, row) for row in rows]
