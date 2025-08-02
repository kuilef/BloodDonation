# Blood Donation Map

**Blood Donation Map** is a web application that visualizes Magen David Adom (MDA) blood donation station locations in Israel on an interactive map, allowing users to find and register for donation appointments.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Database Schema](#database-schema)
- [Running the Data Pipeline](#running-the-data-pipeline)
- [Running the Server](#running-the-server)
- [API Endpoints](#api-endpoints)
- [Frontend](#frontend)
- [Directory Structure](#directory-structure)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)

---

## Features

- **Fetch & Process Data**: Retrieves donation station details from the official MDA API.
- **Geocoding with Cache**: Uses Google Places Text Search API to geocode addresses, with SQLite-based caching to minimize API calls.
- **Backend API**: FastAPI-powered service exposing endpoints for donation data and static files.
- **Interactive Frontend**: Leaflet.js map with date filtering and pop-up registration links.
- **Multi-language Support**: English, Russian, and Hebrew UI.

## Architecture

1. **Data Pipeline**: `backend/data_pipeline/` fetches raw MDA data, geocodes locations, and populates two SQLite databases:
   - `donations.db` (processed station data)
   - `geocache.db` (address → coordinates cache)
2. **Database Layer**: `backend/db/` defines schemas and data access operations.
3. **API Layer**: `backend/app.py` serves the frontend and provides REST endpoints.
4. **Frontend**: `frontend/index.html` uses Leaflet.js to render the map and UI controls.

## Prerequisites

- **Python**: 3.8 or higher
- **pip**: Python package manager
- **Google API Key**: Enable the Places API in Google Cloud Console

## Installation

1. **Clone the repository**:

   ```bash
   git clone https://github.com/your-org/BloodDonationMap.git
   cd BloodDonationMap
   ```

2. **Install backend dependencies**:

   ```bash
   cd backend
   pip install -r requirements.txt
   ```

## Configuration

Before running the pipeline or server, set the required environment variable:

```bash
export GOOGLE_API_KEY="YOUR_GOOGLE_API_KEY"
```


## Database Schema

- `` (via `backend/db/schema.py`):

  - Table: `donations`
    - `id`, `donation_date` (YYYY-MM-DD), `city`, `street`, `num_house`, `name`, `from_hour`, `to_hour`, `scheduling_url` (unique), `latitude`, `longitude`.
    - Indexes on `donation_date` and `city`.

- ``:

  - Table: `geocache`
    - `key` (address composite), `lat`, `lon`, `is_exact` (flag), `updated_at`.

## Running the Data Pipeline

1. Navigate to the project root:
   ```bash
   cd BloodDonationMap
   ```
2. Execute the pipeline script:
   ```bash
   python -m backend.data_pipeline.run_pipeline
   ```
3. This step will:
   - Create or update both SQLite databases.
   - Fetch up donation records from MDA API.
   - Geocode each record and cache results.
   - Populate `donations.db` with processed data.

## Running the Server

Start the FastAPI server with Uvicorn:

```bash
uvicorn backend.app:app --reload
```

- The server will listen on `http://127.0.0.1:8000` by default.
- Ensure `donations.db` exists before startup (pipeline must run first).

## API Endpoints

- `GET /` → Serves `frontend/index.html`
- `GET /donations?donation_date=YYYY-MM-DD` → Returns JSON array of donation stations. Defaults to today's date if omitted.
- `Static Files` → Accessible under `/frontend/` for assets.

## Frontend

- Open your browser at `http://localhost:8000/`.
- Date picker filters stations by donation date.
- Language switcher toggles UI between English, Russian, and Hebrew.
- Click map markers to view station details and registration link.

## Directory Structure

```
BloodDonationMap/
├── backend/
│   ├── data_pipeline/
│   │   ├── geocoder.py       # Caching geocode logic (Google Places)
│   │   ├── processor.py      # Fetch & process MDA data
│   │   └── run_pipeline.py   # Entry point for pipeline
│   ├── db/
│   │   ├── operations.py     # Data access helpers
│   │   └── schema.py         # SQLite schema definitions
│   ├── app.py                # FastAPI application & endpoints
│   └── requirements.txt      # Python dependencies
├── frontend/
│   └── index.html            # Leaflet.js map UI
└── .gitignore
```

## Deployment

- **Domain & HTTPS**: Point your domain (e.g., `bloodmap.example.com`) to the server's IP. Use a reverse proxy (e.g., Nginx) with SSL certificates via Let's Encrypt.
- **Docker** (optional): You can containerize the backend and serve with Docker Compose.
- **Environment Variables**: Ensure `GOOGLE_API_KEY` is set in your production environment.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/foo`)
3. Commit your changes (`git commit -m "feat: add foo"`)
4. Push to the branch (`git push origin feature/foo`)
5. Open a Pull Request

## License

This project is released under the MIT License. See [LICENSE](LICENSE) for details.

## Acknowledgements

- [Magen David Adom (MDA)](https://www.mdais.org) public API
- [Google Places API](https://developers.google.com/maps/documentation/places)
- [Leaflet.js](https://leafletjs.com)
- [OpenStreetMap](https://www.openstreetmap.org)