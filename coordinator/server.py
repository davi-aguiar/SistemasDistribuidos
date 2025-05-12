from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
import httpx

app = FastAPI()

REPLICAS = [
    "http://127.0.0.1:8001",
    "http://127.0.0.1:8002",
]

@app.get("/download")
async def download(arquivo: str):
    async def try_stream(replica: str):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # Try GET directly since HEAD may not be allowed
                async with client.stream("GET", f"{replica}/files/{arquivo}") as response:
                    if response.status_code == 200:
                        async for chunk in response.aiter_bytes():
                            yield chunk
                        return True
        except Exception as e:
            print(f"Failed to stream from {replica}: {str(e)}")
            return False

    for replica in REPLICAS:
        try:
            return StreamingResponse(
                try_stream(replica),
                media_type="application/octet-stream",
                headers={
                    "Content-Disposition": f"attachment; filename={arquivo}",
                    "Cache-Control": "no-store"
                }
            )
        except Exception as e:
            print(f"Error with {replica}: {str(e)}")
            continue

    raise HTTPException(status_code=503, detail="All replicas failed")