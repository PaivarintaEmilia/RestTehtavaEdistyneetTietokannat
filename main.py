from fastapi import FastAPI
from sqlalchemy import text

from db import DW

app = FastAPI()


# määrittää routen
@app.get('/api/hello_world')
async def hello_world():
    return {'hello': 'world'}


# Tehdää A tehtävän eka kysymys. Lainauksien määrä valitulta kk viikoittain mutta resepteille

@app.get('/api/recipes/weekly-by-month/{month}/{year}')
async def get_recipes_weekly_by_month(dw: DW, month: int, year: int):
    _query_str = "SELECT date_dim.week COUNT(*) AS recipe_count FROM recipe_fact " \
                 "INNER JOIN date_dim ON date_dim.date_key = recipe_fact.created_at " \
                 "WHERE date_dim.year = :year AND date_dim.month = :month " \
                 "GROUP BY date_dim.week " \
                 "ORDER BY date_dim.week ASC"

    _query = text(_query_str)

    rows = dw.execute(_query, {'year': year, 'month': month})

    data = rows.mappings().all()

    return {'data': data}


# käynnistä palvelin uudestaan uvicorn main:app

# funktion ja routen muuttujien tulee olla täsmälleen samat
# dw: DW > yhteys tietokantaan



# Lainauksien määrä valitulta jotain päivittäin kysymys 2

@app.get('/api/recipes/daily-by-month/{month}/{year}')
async def get_recipes_daily_by_month(dw: DW, month: int, year: int):
    _query_str = "SELECT date_dim.day COUNT(*) AS recipe_count FROM recipe_fact " \
                 "INNER JOIN date_dim ON date_dim.date_key = recipe_fact.created_at " \
                 "WHERE date_dim.year = :year AND date_dim.month = :month " \
                 "GROUP BY date_dim.day " \
                 "ORDER BY date_dim.day ASC"

    _query = text(_query_str)

    rows = dw.execute(_query, {'year': year, 'month': month})

    data = rows.mappings().all()

    return {'data': data}