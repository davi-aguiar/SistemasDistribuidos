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
        downloaded_bytes = 0
        total_size = None
        progresso_anterior = -1

        while True:  # loop até completar o download ou esgotar as réplicas
            for replica in REPLICAS:
                try:
                    print(f"\nTentando {replica} a partir do byte {downloaded_bytes}...")
                    headers = {"Range": f"bytes={downloaded_bytes}-"}
                    async with httpx.AsyncClient(timeout=None) as client:
                        async with client.stream("GET", f"{replica}/files/{arquivo}", headers=headers) as response:
                            if response.status_code not in [200, 206]:
                                print(f"Réplica {replica} retornou status {response.status_code}")
                                continue

                            content_range = response.headers.get("Content-Range")
                            if content_range and total_size is None:
                                total_size = int(content_range.split("/")[-1])

                            async for chunk in response.aiter_bytes():
                                chunk_len = len(chunk)
                                downloaded_bytes += chunk_len

                                progresso_atual = downloaded_bytes // (1024 * 1024)  # em MB
                                if progresso_atual != progresso_anterior:
                                    print(f"Progresso: {progresso_atual} MB")
                                    progresso_anterior = progresso_atual

                                yield chunk

                            # Se terminou com sucesso
                            if total_size and downloaded_bytes >= total_size:
                                print(f"✅ Download finalizado com sucesso pela réplica {replica}")
                                return
                except Exception as e:
                    print(f"⚠️ Erro ao tentar a réplica {replica}: {e}")
                    continue

            raise HTTPException(status_code=503, detail="Todas as réplicas falharam")

    return StreamingResponse(
        stream_all_replicas(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={arquivo}",
            "Cache-Control": "no-store"
        }
    )
