# replica*/server.py
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import StreamingResponse
import os
import time

app = FastAPI()

@app.get("/files/{nome}")
async def serve(nome: str, request: Request):
    caminho_arquivo = os.path.join(os.path.dirname(__file__), nome)

    if not os.path.isfile(caminho_arquivo):
        raise HTTPException(404, "Arquivo não encontrado")

    TAM = os.path.getsize(caminho_arquivo)

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
        bytes_sent = 0
        with open(caminho_arquivo, "rb") as f:
            f.seek(start)
            while chunk := f.read(64*1024):
                bytes_sent += len(chunk)
                if bytes_sent >= 10 * 1024 * 1024:  # 10 MB
                    print("Simulando falha da réplica")
                    raise Exception("Falha simulada na réplica")
                time.sleep(0.1)  # pequeno atraso para permitir acompanhamento
                yield chunk

    return StreamingResponse(chunker(), status_code=status,
                             media_type="application/octet-stream", headers=headers)
