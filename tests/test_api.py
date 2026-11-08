# ruff: noqa: DTZ001 -- naive datetimes here deliberately mirror quality.py's
# own naive comparisons against TLC's tz-naive timestamp columns.
import datetime
from unittest.mock import patch

import polars as pl
from fastapi.testclient import TestClient

from taxi_pipeline.api import app
from taxi_pipeline.model import train_on_all_data


def _synthetic_curated(num_weeks=4):
    total_hours = num_weeks * 7 * 24
    hours = [
        datetime.datetime(2024, 1, 1, 0, 0) + datetime.timedelta(hours=h)
        for h in range(total_hours)
    ]
    rows = []
    for zone in (1, 2):
        for i, hour in enumerate(hours):
            rows.append(
                {
                    "pickup_hour": hour,
                    "pickup_location_id": zone,
                    "pickup_count": 100 + zone * 10 + i,
                }
            )
    return pl.DataFrame(rows)


def test_get_next_day_predictions_returns_json_rows():
    # Mocks load_model_artifacts/load_curated so the test never touches the
    # real 1.3GB model file -- same reasoning as mocking the network in
    # test_ingest.py: a test shouldn't depend on heavy, slow, or unavailable
    # real state just to verify the endpoint's own behavior.
    curated = _synthetic_curated()
    model, zones, baseline_table = train_on_all_data(curated)
    fake_artifacts = {
        "model": model,
        "zones": zones,
        "baseline_table": baseline_table,
        "lag_hours": 168,
    }

    with (
        patch("taxi_pipeline.api.load_model_artifacts", return_value=fake_artifacts),
        patch("taxi_pipeline.api.load_curated", return_value=curated),
        TestClient(app) as client,
    ):
        response = client.get("/predictions/next-day")

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 24 * 2  # 24 hours x 2 zones
    assert {row["pickup_location_id"] for row in data} == {1, 2}
    assert all(row["predicted_pickups"] is not None for row in data)
