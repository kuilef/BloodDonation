"""
python -m backend.data_pipeline.run_pipeline
— создаёт/обновляет donations.db, заполняет данными и кэшем координат.

Запуск сервера
uvicorn backend.app:app --reload

Открытие интерфейса
В браузере перейти на http://localhost:8000/ (или соответствующий хост), после чего:

увидеть на карте ближайшие станции и оформить запись.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from datetime import date
from typing import Optional

from .db import operations
from .db.schema import DONATIONS_DB_PATH

app = FastAPI(
    title="Blood Donation Map",
    description="Find blood donation stations in Israel.",
    version="1.0.0",
)

# Configure CORS to allow frontend access
# In a production environment, restrict the origins.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)





@app.on_event("startup")
async def startup_event():
    """Check for database on startup."""
    if not DONATIONS_DB_PATH.exists():
        raise RuntimeError(
            f"Database not found at {DONATIONS_DB_PATH}. "
            "Please run the data pipeline first to create and populate the database."
        )

@app.get("/donations", summary="Get donation stations by date")
async def get_donations(
    donation_date: Optional[str] = Query(
        None,
        description="Date in YYYY-MM-DD format. Defaults to today.",
        regex=r"^\d{4}-\d{2}-\d{2}$"
    )
):
    """
    Returns a list of all available blood donation stations for a given date.
    If no date is provided, it defaults to the current day.
    """
    target_date = donation_date or date.today().isoformat()
    try:
        results = operations.get_donations_by_date(target_date)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# Serve the frontend, including index.html and other assets like favicons
app.mount("/", StaticFiles(directory="frontend", html=True), name="static")