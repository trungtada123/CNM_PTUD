from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from kkbox_poc.dashboard import get_dashboard_payload

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="Trung tâm điều hành rủi ro rời bỏ KKBox")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@app.on_event("startup")
def startup_event() -> None:
    try:
        get_dashboard_payload(force_refresh=True)
    except Exception:
        pass


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/summary")
def summary_api(refresh: bool = Query(default=False)) -> JSONResponse:
    payload = get_dashboard_payload(force_refresh=refresh)
    return JSONResponse(payload)


@app.get("/", response_class=HTMLResponse)
def dashboard_page(request: Request, refresh: bool = Query(default=False)) -> HTMLResponse:
    payload = get_dashboard_payload(force_refresh=refresh)
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"payload": payload},
    )
