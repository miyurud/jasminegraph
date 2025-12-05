# mock_ollama_server_array.py
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
import json, asyncio

app = FastAPI()

async def streamer(prompt: str):
    response = [
        ["Alice","knows","Bob","Person","Person"],
        ["Bob","works_at","AcmeCorp","Person","Organization"],
        ["AcmeCorp","located_in","London","Organization","Location"]
    ]

    # Simulate streaming by sending partial chunks
    chunk_size = 1
    yield "["  # start array
    for i, t in enumerate(response):
        yield json.dumps(t)
        if i < len(response) - 1:
            yield ","
        await asyncio.sleep(0.05)
    yield "]\n"  # end array

@app.post("/api/generate")
async def generate(request: Request):
    return StreamingResponse(streamer(""), media_type="application/json")
