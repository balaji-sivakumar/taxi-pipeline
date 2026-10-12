# ruff: noqa: DTZ001 -- naive datetimes here deliberately mirror quality.py's
# own naive comparisons against TLC's tz-naive timestamp columns.
from datetime import datetime

import polars as pl

from taxi_pipeline.analysis import (
    day_of_week_profile,
    hourly_profile,
    plot_day_of_week_profile,
    plot_hourly_profile,
    plot_zone_totals,
    zone_totals,
)


def _write_lookup(path):
    pl.DataFrame(
        {
            "LocationID": [1, 2, 3],
            "Borough": ["Manhattan", "Queens", "Bronx"],
            "Zone": ["Zone One", "Zone Two", "Zone Three"],
            "service_zone": ["Yellow Zone", "Boro Zone", "Boro Zone"],
        }
    ).write_csv(path)


def test_hourly_profile_sums_zones_then_averages_across_days():
    curated = pl.DataFrame(
        {
            "pickup_hour": [
                datetime(2024, 1, 1, 10, 0),  # day 1, hour 10, zone 1
                datetime(2024, 1, 1, 10, 0),  # day 1, hour 10, zone 2
                datetime(2024, 1, 2, 10, 0),  # day 2, hour 10, zone 1
            ],
            "pickup_location_id": [1, 2, 1],
            "pickup_count": [10, 5, 20],
        }
    )

    profile = hourly_profile(curated)

    # day 1: 10+5=15 citywide; day 2: 20 citywide; mean = 17.5
    row = profile.filter(pl.col("hour_of_day") == 10)
    assert row["avg_citywide_pickups"].item() == 17.5


def test_day_of_week_profile_uses_mean_not_sum_across_unequal_occurrences():
    curated = pl.DataFrame(
        {
            "pickup_hour": [
                datetime(2024, 1, 1, 10, 0),  # Monday
                datetime(2024, 1, 8, 10, 0),  # Monday (2nd occurrence)
                datetime(2024, 1, 2, 10, 0),  # Tuesday (only 1 occurrence)
            ],
            "pickup_location_id": [1, 1, 1],
            "pickup_count": [100, 100, 100],
        }
    )

    profile = day_of_week_profile(curated)

    monday = profile.filter(pl.col("weekday_name") == "Monday")[
        "avg_citywide_pickups"
    ].item()
    tuesday = profile.filter(pl.col("weekday_name") == "Tuesday")[
        "avg_citywide_pickups"
    ].item()

    # Both days have identical per-occurrence magnitude (100); Monday
    # occurring twice must not make it look busier than Tuesday.
    assert monday == 100.0
    assert tuesday == 100.0


def test_day_of_week_profile_orders_monday_through_sunday():
    curated = pl.DataFrame(
        {
            "pickup_hour": [datetime(2024, 1, d, 10, 0) for d in range(1, 8)],
            "pickup_location_id": [1] * 7,
            "pickup_count": [10] * 7,
        }
    )

    profile = day_of_week_profile(curated)

    assert profile["weekday_name"].to_list() == [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]


def test_plot_hourly_profile_produces_a_real_file(tmp_path):
    curated = pl.DataFrame(
        {
            "pickup_hour": [datetime(2024, 1, 1, h, 0) for h in range(24)],
            "pickup_location_id": [1] * 24,
            "pickup_count": [10] * 24,
        }
    )
    save_path = tmp_path / "hourly.png"

    result = plot_hourly_profile(hourly_profile(curated), save_path)

    assert result == save_path
    assert save_path.exists()
    assert save_path.stat().st_size > 0


def test_plot_day_of_week_profile_produces_a_real_file(tmp_path):
    curated = pl.DataFrame(
        {
            "pickup_hour": [datetime(2024, 1, d, 10, 0) for d in range(1, 8)],
            "pickup_location_id": [1] * 7,
            "pickup_count": [10] * 7,
        }
    )
    save_path = tmp_path / "dow.png"

    result = plot_day_of_week_profile(day_of_week_profile(curated), save_path)

    assert result == save_path
    assert save_path.exists()
    assert save_path.stat().st_size > 0


def test_zone_totals_sums_pickups_and_joins_real_names(tmp_path):
    lookup_path = tmp_path / "lookup.csv"
    _write_lookup(lookup_path)
    curated = pl.DataFrame(
        {
            "pickup_hour": [datetime(2024, 1, 1, 10, 0)] * 3,
            "pickup_location_id": [1, 1, 2],
            "pickup_count": [10, 5, 3],
        }
    )

    totals = zone_totals(curated, lookup_path)

    zone_one = totals.filter(pl.col("pickup_location_id") == 1)
    assert zone_one["total_pickups"].item() == 15
    assert zone_one["Zone"].item() == "Zone One"
    assert zone_one["Borough"].item() == "Manhattan"


def test_zone_totals_sorted_busiest_first(tmp_path):
    lookup_path = tmp_path / "lookup.csv"
    _write_lookup(lookup_path)
    curated = pl.DataFrame(
        {
            "pickup_hour": [datetime(2024, 1, 1, 10, 0)] * 2,
            "pickup_location_id": [2, 1],
            "pickup_count": [3, 15],
        }
    )

    totals = zone_totals(curated, lookup_path)

    assert totals["pickup_location_id"].to_list() == [1, 2]


def test_plot_zone_totals_produces_a_real_file(tmp_path):
    lookup_path = tmp_path / "lookup.csv"
    _write_lookup(lookup_path)
    curated = pl.DataFrame(
        {
            "pickup_hour": [datetime(2024, 1, 1, 10, 0)] * 2,
            "pickup_location_id": [1, 2],
            "pickup_count": [10, 5],
        }
    )
    save_path = tmp_path / "zones.png"

    result = plot_zone_totals(zone_totals(curated, lookup_path), save_path, top_n=2)

    assert result == save_path
    assert save_path.exists()
    assert save_path.stat().st_size > 0
