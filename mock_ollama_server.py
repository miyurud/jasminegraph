# mock_ollama_server_array.py
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import json
import asyncio

app = FastAPI()

# ======================
# CORS FIX (OPTIONS 405)
# ======================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mock available models
AVAILABLE_MODELS = [
    {"name": "gemma3:1b", "description": "A mock model for testing."},
    {"name": "mock-llama", "description": "Another mock LLM."}
]

async def streamer(prompt: str, model: str):
    """Stream Ollama-style NDJSON."""

    response = [
        ["Alice","knows","Bob","Person","Person"],
        ["Bob","works_at","AcmeCorp","Person","Organization"],
        ["AcmeCorp","located_in","London","Organization","Location"]
    ]

    for triple in response:
        yield json.dumps({
            "model": model,
            "created_at": "2025-01-01T00:00:00Z",
            "response": triple,
            "done": False
        }) + "\n"
        await asyncio.sleep(0.05)

    yield json.dumps({
        "model": model,
        "created_at": "2025-01-01T00:00:00Z",
        "response": "",
        "done": True
    }) + "\n"


@app.get("/api/tags")
async def get_models():
    return JSONResponse(content=AVAILABLE_MODELS)


@app.post("/api/generate")
async def generate(request: Request):
    data = await request.json()
    prompt = data.get("prompt", "")
    model = data.get("model", "mock-llama")
    stream = data.get("stream", False)

    if not stream:
        return JSONResponse(content={
            "model": model,
            "response": [
                ["Alice","knows","Bob","Person","Person"],
                ["Bob","works_at","AcmeCorp","Person","Organization"],
                ["AcmeCorp","located_in","London","Organization","Location"]
            ],
            "done": True
        })

    return StreamingResponse(
        streamer(prompt, model),
        media_type="application/x-ndjson"
    )
