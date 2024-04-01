from typing import Annotated

from fastapi import Depends
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

engina = create_engine('mysql+mysqlconnector://root:@localhost/laplanduas_rental')
dw_session = sessionmaker(bind=engina)

def get_dw():
    _dw = None
    try:
        _dw = dw_session()
        yield _dw
    finally:
        if _dw is not None:
            _dw.close()


DW = Annotated[Session, Depends(get_dw())]