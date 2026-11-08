from contextlib import asynccontextmanager

from fastapi import FastAPI

from taxi_pipeline.analysis import load_curated
from taxi_pipeline.model import load_model_artifacts, predict_next_day_from_artifacts

_state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    _state["artifacts"] = load_model_artifacts()
    yield
    _state.clear()


app = FastAPI(lifespan=lifespan)


@app.get("/predictions/next-day")
def get_next_day_predictions():
    curated = load_curated()
    predictions = predict_next_day_from_artifacts(curated, _state["artifacts"])
    return predictions.to_dicts()
