from fastapi import FastAPI
from sqlalchemy import text

from db import DW

app = FastAPI()


# määrittää routen
@app.get('/api/hello_world')
async def hello_world():
    return {'hello': 'world'}


# 1. Lainauksien määrä valitulta kuukaudelta viikottain

@app.get('/api/recipes/weekly-by-month/{month}/{year}')
async def get_recipes_weekly_by_month(dw: DW, month: int, year: int):
    _query_str = "SELECT date_dim.week, COUNT(*) AS transaction_count " \
                 "FROM rental_transaction_fact " \
                 "INNER JOIN date_dim ON date_dim.date_key = rental_transaction_fact.date_dim_date_key " \
                 "WHERE date_dim.year = :year AND date_dim.month = :month " \
                 "GROUP BY date_dim.week ORDER BY date_dim.week ASC;"

    _query = text(_query_str)

    rows = dw.execute(_query, {'year': year, 'month': month})

    data = rows.mappings().all()

    return {'data': data}


# käynnistä palvelin uudestaan uvicorn main:app

# funktion ja routen muuttujien tulee olla täsmälleen samat
# dw: DW > yhteys tietokantaan



# 2. Lainauksien märää valitulta kuukaudelta päivittäin

@app.get('/api/recipes/daily-by-month/{month}/{year}')
async def get_recipes_daily_by_month(dw: DW, month: int, year: int):
    _query_str = "SELECT date_dim.day, COUNT(*) AS transaction_count " \
                 "FROM rental_transaction_fact " \
                 "INNER JOIN date_dim ON date_dim.date_key = rental_transaction_fact.date_dim_date_key " \
                 "WHERE date_dim.year = :year AND date_dim.month = :month " \
                 "GROUP BY date_dim.day ORDER BY date_dim.day ASC;"

    _query = text(_query_str)

    rows = dw.execute(_query, {'year': year, 'month': month})

    data = rows.mappings().all()

    return {'data': data}


# 3. Lainauksien määrä valitulta vuodelta kuukausittain

@app.get('/api/recipes/monthly-by-year/{year}')
async def get_recipes_daily_by_month(dw: DW, year: int):
    _query_str = "SELECT date_dim.month, COUNT(*) AS transaction_count " \
                 "FROM rental_transaction_fact " \
                 "INNER JOIN date_dim ON date_dim.date_key = rental_transaction_fact.date_dim_date_key " \
                 "WHERE date_dim.year = :year " \
                 "GROUP BY date_dim.month " \
                 "ORDER BY date_dim.month ASC"

    _query = text(_query_str)

    rows = dw.execute(_query, {'year': year})

    data = rows.mappings().all()

    return {'data': data}



# 4. Kaikkien aikojen top 10. lainatuimmat tavarat


@app.get('/api/recipes/top-ten-items')
async def get_recipes_daily_by_month(dw: DW):
    _query_str = "SELECT rental_item_dim.renta_item_name, COUNT(rental_item_dim.renta_item_name) AS esiintymiskerrat " \
                 "FROM rental_item_fact " \
                 "INNER JOIN rental_item_dim ON rental_item_dim.item_key = rental_item_fact.rental_item_key " \
                 "GROUP BY rental_item_dim.renta_item_name " \
                 "ORDER BY esiintymiskerrat DESC " \
                 "LIMIT 10"


    _query = text(_query_str)

    rows = dw.execute(_query)

    data = rows.mappings().all()

    return {'data': data}


# 5. Top 10. lainatut tavarat valitulta vuodelta kuukausittain


@app.get('/api/recipes/top-ten-items-by-year/{year}')
async def get_recipes_daily_by_month(dw: DW, year: int):
    _query_str = "SELECT rental_item_dim.renta_item_name, COUNT(rental_item_dim.renta_item_name) AS esiintymiskerrat " \
                 "FROM rental_item_fact " \
                 "INNER JOIN rental_item_dim ON rental_item_dim.item_key = rental_item_fact.rental_item_key " \
                 "INNER JOIN date_dim ON date_dim.date_key = rental_item_fact.rental_item_creation_date_key " \
                 "WHERE date_dim.year = 2004 GROUP BY date_dim.month " \
                 "ORDER BY date_dim.month DESC " \
                 "LIMIT 10"

    _query = text(_query_str)

    rows = dw.execute(_query, {'year': year})

    data = rows.mappings().all()

    return {'data': data}

# 6. Selvitä missä kuussa tavaroita lisätään järjestelmään eniten valittuna vuonna