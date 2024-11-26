from typing import Optional
import datetime


import hashlib
import os

from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel

from reader import generate_random_rows, get_paginated_rows, CHUNK_SIZE
from cursor import encode_cursor, decode_cursor

app = FastAPI()
session_tokens = {}

FILE_NAME = 'synthetic_fraud_data.csv'
FILE_PATH =  os.path.join(os.path.dirname(__file__), FILE_NAME)


class TransactionParams(BaseModel):
    token: str

class HistoricalTransactionParams(BaseModel):
    token: str
    cursor: Optional[str] = None


@app.get("/")
def read_root():
    return "Hello there"


@app.get("/get/token")
def generate_token(request: Request):
    client_host = request.client.host
    token_input = f'{client_host}'

    token = hashlib.sha256(token_input.encode()).hexdigest()
    if token not in session_tokens: 
        session_tokens[token] = {
            'ip': client_host,
            'in_use': False,
        }
    return {"token": token}


@app.post("/transactions/historical")
def read_historical_data(params: HistoricalTransactionParams, request: Request):
    data_params = params.dict()
    token = data_params.get('token')
    if token not in session_tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token Required",
        )
    cursor = data_params.get('cursor')
    start_pos = 0
    data = {}
    if cursor:
        try:
            val_pos, start_pos = decode_cursor(cursor)
            data = get_paginated_rows(FILE_PATH, start_pos)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(e),
            )
    else:
        data = get_paginated_rows(FILE_PATH, start_pos)

    if data:
        start_pos = start_pos + CHUNK_SIZE
        value = datetime.datetime.now().isoformat()
        next_cursor = encode_cursor(value, start_pos)
        response = {
            'cursor': next_cursor,
            'data': data
        }
        return response
    return {
        'data': data
    }

@app.post("/transactions/latest")
def read_randomly(params: TransactionParams, request: Request):
    data_params = params.dict()
    token = data_params.get('token')
    if token not in session_tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token Required",
        )
    if session_tokens[token]['in_use']:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="A previous request is calculating",
        )

    session_tokens[token]['in_use'] = True
    data = generate_random_rows(FILE_PATH)
    session_tokens[token]['in_use'] = False

    return data
