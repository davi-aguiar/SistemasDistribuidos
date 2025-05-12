# replica*/server.py
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import StreamingResponse
import os

app = FastAPI()
ARQUIVO = os.path.join(os.path.dirname(__file__), "foo.txt")
TAM = os.path.getsize(ARQUIVO)

@app.get("/files/{nome}")
async def serve(nome: str, request: Request):
    if nome != "foo.txt":
        raise HTTPException(404, "Não encontrado")
    # calcula início a partir do Range
    range_hdr = request.headers.get("range")
    start = 0
    if range_hdr:
        start = int(range_hdr.split("=")[1].split("-")[0])
    status = 206 if range_hdr else 200
    headers = {}
    if range_hdr:
        headers["Content-Range"] = f"bytes {start}-{TAM-1}/{TAM}"

    def chunker():
        with open(ARQUIVO, "rb") as f:
            f.seek(start)
            while chunk := f.read(64*1024):
                yield chunk
    return StreamingResponse(chunker(), status_code=status,
                             media_type="application/octet-stream", headers=headers)
