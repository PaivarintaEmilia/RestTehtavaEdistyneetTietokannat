from typing import Annotated

import jwt
from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
from sqlalchemy import text
from passlib.hash import pbkdf2_sha512 as pl

from db import DW

app = FastAPI()

# Rekisteröityminen. Luodaan endPoint, jolla saa lisättyä käyttäjän
# DW on tietokantayhteys

class RegisterRequest(BaseModel):
    username: str
    password: str


# Secret key tokenia varten
SECRET_KEY = "dkslfsmdkgfdäasfpeo053486rmt4jsdmkamedowkroi0wei3mksmfdht48jhkdmfmmf"



def require_login(dw: DW, authorization = Header(None, alias='api_key')):
    try:
        if authorization is not None and len(authorization.split(' ')) == 2:
            split_header = authorization.split(' ')
            if len(split_header) == 2 and split_header[0] == 'Bearer':
                token = split_header[1]
                validated = jwt.decode(token, SECRET_KEY, algorithms=['HS512'])
                user = dw.execute(text('SELECT username FROM users WHERE id = :id'),
                                  {'id': validated['id']}).mappings().first()
                # Jos käyttäjää ei löydy tietokannasta niin user jää nulliksi ja nostetaan virheilmoitus
                if user is None:
                    raise HTTPException(detail='user not found', status_code=404)
                return user
        else:
            raise HTTPException(detail='user not found', status_code=404)

    except Exception as e:
        raise HTTPException(detail=str(e), status_code=500)


LoggedInUser = Annotated[dict, Depends(require_login)]


# Tehdään uusi route, josta saa sisäänkirjautuneen käyttäjän tiedot
@app.get('/api/account')
async def get_account(logged_in_user = LoggedInUser):
    return logged_in_user



# Luodaan sisäänkirjautuminen
@app.post('/api/login')
async def login(dw: DW, req: RegisterRequest):
    # 1. Haetaan käyttäjä usernamen perusteella
    _query_str = ("SELECT * FROM users WHERE username = :username")
    _query = text(_query_str)
    user = dw.execute(_query, {'username': req.username}).mappings().first() # First, koska tulee vain yksi
    # Nostetaan poikkeus, jos usernamea ei löydy tietokannasta
    if user is None:
        raise HTTPException(detail='user not found', status_code=404)
    # Jos username löytyy, niin verrataan sen salasanaa käyttäjän syöttämään salasanaan
    password_correct = pl.verify(req.password, user['password'])
    # Jos salasana on ok
    if password_correct:
        token = jwt.encode({'id': user['id']}, SECRET_KEY, algorithm='HS512')
        return {'token': token}
    # Jos salasana ei ole ok
    raise HTTPException(detail='password is incorrect', status_code=404)



@app.post('/api/register')
async def register(dw: DW, req: RegisterRequest):
    try:
        _query_str = ("INSERT INTO users (username, password) VALUES(:username, :password)")
        _query = text(_query_str)
        user = dw.execute(_query, {'username': req.username, 'password': pl.hash(req.password)})
        dw.commit()
        return{'username': req.username, 'id': user.lastrowid}
    except Exception as e:
        dw.rollback()
        print(e)
        # Käyttäjälle tuleva virheilmoitus
        raise HTTPException(status_code=422, detail='Error registering user')


# määrittää routen
@app.get('/api/hello_world')
async def hello_world():
    return {'hello': 'world'}


# 1. Lainauksien määrä valitulta kuukaudelta viikottain

@app.get('/api/rental/weekly-by-month/{month}/{year}')
async def get_rental_weekly_by_month(dw: DW, month: int, year: int, logged_in_user = LoggedInUser):
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

@app.get('/api/rental/daily-by-month/{month}/{year}')
async def get_rental_daily_by_month(dw: DW, month: int, year: int, logged_in_user = LoggedInUser):
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

@app.get('/api/rental/monthly-by-year/{year}')
async def get_rental_monthly_by_year(dw: DW, year: int, logged_in_user = LoggedInUser):
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


@app.get('/api/rental/top-ten-items')
async def get_rental_top_ten_items(dw: DW, logged_in_user = LoggedInUser):
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


@app.get('/api/rental/top-ten-items-by-year/{year}')
async def get_rental_top_ten_items_by_year(dw: DW, year: int,logged_in_user = LoggedInUser):
    _query_str = "SELECT rental_item_dim.renta_item_name, COUNT(rental_item_dim.renta_item_name) AS esiintymiskerrat " \
                 "FROM rental_item_fact " \
                 "INNER JOIN rental_item_dim ON rental_item_dim.item_key = rental_item_fact.rental_item_key " \
                 "INNER JOIN date_dim ON date_dim.date_key = rental_item_fact.rental_item_creation_date_key " \
                 "WHERE date_dim.year = 2004 " \
                 "GROUP BY date_dim.month " \
                 "ORDER BY date_dim.month DESC " \
                 "LIMIT 10"

    _query = text(_query_str)

    rows = dw.execute(_query, {'year': year})

    data = rows.mappings().all()

    return {'data': data}



# 6. Selvitä missä kuussa tavaroita lisätään järjestelmään eniten valittuna vuonna


@app.get('/api/rental/top-month-when-adding-items-by-year/{year}')
async def get_rental_top_month_when_adding_items_by_year(dw: DW, year: int, logged_in_user = LoggedInUser):
    _query_str = "SELECT date_dim.month AS kuukausi, COUNT(rental_item_fact.rental_item_creation_date_key) AS tavaramaara " \
                 "FROM rental_item_fact " \
                 "INNER JOIN rental_item_dim ON rental_item_dim.item_key = rental_item_fact.rental_item_key " \
                 "INNER JOIN date_dim ON date_dim.date_key = rental_item_fact.rental_item_creation_date_key " \
                 "WHERE date_dim.year = :year " \
                 "GROUP BY kuukausi " \
                 "ORDER BY tavaramaara DESC " \
                 "LIMIT 1"

    _query = text(_query_str)

    rows = dw.execute(_query, {'year': year})

    data = rows.mappings().all()

    return {'data': data}