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
    async def stream_all_replicas():
        for replica in REPLICAS:
            try:
                print(f"Tentando {replica}...")
                async with httpx.AsyncClient(timeout=None) as client:
                    async with client.stream("GET", f"{replica}/files/{arquivo}") as response:
                        if response.status_code != 200:
                            print(f"Réplica {replica} retornou status {response.status_code}")
                            continue
                        # Transmite todos os chunks desta réplica
                        async for chunk in response.aiter_bytes():
                            yield chunk
                        print(f"Download finalizado com sucesso pela réplica {replica}")
                        return  # finaliza após sucesso
            except Exception as e:
                print(f"Erro ao tentar a réplica {replica}: {e}")
                continue

        # Se nenhuma réplica respondeu corretamente
        raise HTTPException(status_code=503, detail="Todas as réplicas falharam")

    return StreamingResponse(
        stream_all_replicas(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={arquivo}",
            "Cache-Control": "no-store"
        }
    )