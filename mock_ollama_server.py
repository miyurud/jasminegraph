# mock_ollama_server_array.py
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import json, asyncio

app = FastAPI()

# Mock available models
AVAILABLE_MODELS = [
    {"name": "gemma3:1b", "description": "A mock model for testing."},
    {"name": "mock-llama", "description": "Another mock LLM."}
]

async def streamer(prompt: str):
    """Simulate streaming a JSON array of arrays."""
    response = [
        ["Alice","knows","Bob","Person","Person"],
        ["Bob","works_at","AcmeCorp","Person","Organization"],
        ["AcmeCorp","located_in","London","Organization","Location"]
    ]

    yield "["  # start array
    for i, t in enumerate(response):
        yield json.dumps(t)
        if i < len(response) - 1:
            yield ","
        await asyncio.sleep(0.05)  # simulate streaming delay
    yield "]\n"  # end array

@app.get("api/tags")
async def get_models():
    """Return a list of available mock models."""
    return JSONResponse(content=AVAILABLE_MODELS)

@app.post("/api/generate")
async def generate(request: Request):
    """Simulate generating tuples from a prompt."""
    data = await request.json()
    prompt = data.get("prompt", "")
    return StreamingResponse(streamer(prompt), media_type="application/json")
