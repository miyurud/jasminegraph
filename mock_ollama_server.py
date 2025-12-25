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

    # Example array-of-arrays tuples
    tuples = [
        ["Radio City", "is", "India's first private FM radio station"],
        ["Radio City", "was started on", "3 July 2001", "Organization", "Date"],
        ["it", "broadcasts on", "91.1", "Organization", "Frequency"]
    ]

    # Start of response
    for chunk in ["```", "json", "\n", "["]:
        yield json.dumps({"model": model, "created_at": "2025-01-01T00:00:00Z", "response": chunk, "done": False}) + "\n"
        await asyncio.sleep(0.05)

    # Stream each tuple as JSON array
    for i, t in enumerate(tuples):
        line = "  " + json.dumps(t)
        if i < len(tuples) - 1:
            line += ","
        yield json.dumps({"model": model, "created_at":  "2025-01-01T00:00:00Z", "response": line, "done": False}) + "\n"
        await asyncio.sleep(0.05)

    # End of array and code block
    for chunk in ["\n", "]", "```"]:
        yield json.dumps({"model": model, "created_at": "2025-01-01T00:00:00Z", "response": chunk, "done": False}) + "\n"
        await asyncio.sleep(0.05)

    # Done
    yield json.dumps({"model": model, "created_at":  "2025-01-01T00:00:00Z", "response": "", "done": True}) + "\n"



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


def fake_embedding(text, dim=768):
    """Return a deterministic mock embedding based on the text content."""
    # Simple deterministic embedding using character codes
    vec = [(ord(c) % 10 + 0.1 * i) for i, c in enumerate(text[:dim])]
    # Pad to dimension
    while len(vec) < dim:
        vec.append(0.0)
    return vec

@app.post("/api/embeddings")
async def single_embed(request: Request):
    data = await request.json()
    text = data.get("prompt") or (data.get("input")[0] if isinstance(data.get("input"), list) else "")
    embedding = fake_embedding(text, dim=768)
    return JSONResponse(content={"embedding": embedding})
