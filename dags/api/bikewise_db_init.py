import os
from api.bikewise_v2 import getBikeInfo, getEventAfterBefore, getByApiURL, getManufacturerFromString
from sqlalchemy import create_engine, MetaData, Column, Integer, String, Boolean, exc, Date, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql.expression import desc, false
from sqlalchemy.sql.sqltypes import Boolean, Date, DateTime, TIMESTAMP
from datetime import datetime, date, timedelta
from pgeocode import Nominatim

from airflow.hooks.postgres_hook import PostgresHook

Base = declarative_base(metadata=MetaData(schema='source'))

# class Manufacturer(Base):
#     __tablename__ = "manufacturers"
#     id = Column('id', Integer, primary_key=True)
#     name = Column(String, nullable=False)
#     company_url = Column(String)
#     frame_maker = Column(Boolean)
#     description = Column(String)
#     short_name = Column(String)
#     slug = Column(String)
class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    loaded_until = Column(Integer, nullable=False)
    loaded_from = Column(Integer, nullable=False)
    success = Column(Boolean, nullable=False)
    def __init__(self, loaded_from, loaded_until):
        self.loaded_until = int(loaded_until)
        self.loaded_from = int(loaded_from)
        self.success = True

class Bike(Base):
    __tablename__ = "bikes"
    id = Column('id', Integer, primary_key=True)
    time_added = Column(DateTime, nullable=False)
    title = Column(String, nullable=False)
    # manufacturer
    manufacturer_name = Column(String, nullable=False)
    manufacturer_company_url = Column(String)
    manufacturer_frame_maker = Column(Boolean)
    manufacturer_description = Column(String)
    manufacturer_short_name = Column(String)
    manufacturer_slug = Column(String)

    # manufacturer should go in own dimension, this stuff should not (maybe)
    # model
    model = Column(String)
    model_year = Column(Integer)
    model_color = Column(String)
    description = Column(String)
    # location

    # address = Column(String, nullable=False)
    address_city = Column(String)
    address_postal = Column(String, nullable=False)
    address_lat = Column(String)
    address_lon = Column(String)
    address_country = Column(String)

    #datetime
    occurred_at = Column(Integer, nullable=False)
    updated_at = Column(Integer, nullable=False)
    __table_args__ = (
        UniqueConstraint('title', 'occurred_at', name='bike_stolen_duplicate_constraint'),
    )
    
    def __init__(self, json) -> None:
        self.title = json['title']
        self.time_added = datetime.today()
        v1info = getByApiURL(json['source']['api_url'])['bikes']
        manufacturer_info = getManufacturerFromString(v1info['manufacturer_name'])
        if manufacturer_info and v1info:
            self.manufacturer_name = manufacturer_info['name']
            self.manufacturer_company_url = manufacturer_info['company_url']
            self.manufacturer_frame_maker = manufacturer_info['frame_maker']
            self.manufacturer_description = manufacturer_info['description']
            self.manufacturer_short_name = manufacturer_info['short_name']
            self.manufacturer_slug = manufacturer_info['slug']
        if v1info:
            self.model = v1info['frame_model']
            self.model_year = v1info['year']
            self.model_color = ",".join(v1info['frame_colors'])
        self.description = json['description']
        address = json['address']
        if address:
            address = address.split(',')
            if len(address) == 3:
                self.address_city = address[0].strip()
                self.address_country = address[2].strip()
                postal = address[1].replace(" ", "")[:3].upper()
                self.address_postal = postal
                nomi = Nominatim('CA').query_postal_code(postal)
                self.address_lat = nomi['latitude']
                self.address_lon = nomi['longitude']
                
        self.occurred_at = json['occurred_at']
        self.updated_at = json['updated_at']

def intoDB():
    # Index("some_index", sometable.c.name)
    engine = create_engine('postgresql://airflow:airflow@localhost:5432/airflow', echo=True)
    #create declarative base's metadata
    Base.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    # user = User()
    # user.id = 0
    # user.username = 'alice'

    #session lets you do stuff, then commit changes
    # session.add(user)
    # session.commit()
    occurred_after_date = datetime.today() - timedelta(days=10)
    occurred_before_date = occurred_after_date + timedelta(days=8)
    # get_api_timestamp = occurred_after_date.timestamp() 
    session.add(Transaction(occurred_after_date.timestamp()))
    session.commit()
    stolen_bikes = getBikeInfo(occurred_after_date.timestamp())
    for s in stolen_bikes:
        b = Bike(s)
        try:
            session.add(b)
            timestamp = b.occurred_at
            session.commit()
        except exc.IntegrityError:
            session.rollback()
    # session.commit()
    session.close()

def intoDBToday(**kwargs):
    days_back = kwargs['days_back']
    time_delta = kwargs['time_delta']
    postgres_conn_id = kwargs['postgres_conn_id']

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    Base.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    run_date_string = str(os.environ["AIRFLOW_CTX_EXECUTION_DATE"]).split("+")
    run_date_string[1] =  run_date_string[1].replace(":", "")
    run_date_string = "+".join(run_date_string)

    if "." in run_date_string:
        d = datetime.strptime(run_date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
    else:
        d = datetime.strptime(run_date_string, "%Y-%m-%dT%H:%M:%S%z")

    occurred_after_date = d.astimezone(tz=None) - timedelta(days=days_back)
    occurred_before_date = occurred_after_date + timedelta(days=time_delta)
    stolen_bikes = getEventAfterBefore(occurred_after_date.timestamp(), occurred_before_date.timestamp())
    for s in stolen_bikes:
        try:
            b = Bike(s)
        except KeyError:
            print(s['id'], "- keyerror")
            continue
        try:
            session.add(b)
            session.commit()
        except exc.IntegrityError:
            session.rollback()
    session.close()

def capTransactions(**kwargs):
    days_back = kwargs['days_back']
    time_delta = kwargs['time_delta']
    postgres_conn_id = kwargs['postgres_conn_id']

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    #create declarative base's metadata
    Base.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    run_date_string = str(os.environ["AIRFLOW_CTX_EXECUTION_DATE"]).split("+")
    run_date_string[1] =  run_date_string[1].replace(":", "")
    run_date_string = "+".join(run_date_string)

    if "." in run_date_string:
        d = datetime.strptime(run_date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
    else:
        d = datetime.strptime(run_date_string, "%Y-%m-%dT%H:%M:%S%z")

    occurred_after_date = d.astimezone(tz=None) - timedelta(days=days_back)
    occurred_before_date = occurred_after_date + timedelta(days=time_delta)
    session.add(Transaction(occurred_after_date.timestamp(), occurred_before_date.timestamp()))
    session.commit()
    session.close()

def args_test(db_string, back_delta):
    print(db_string)
    print(os.environ["AIRFLOW_CTX_EXECUTION_DATE"])
    print(back_delta)
