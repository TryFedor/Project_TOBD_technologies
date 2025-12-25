# flows/database.py
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from prefect import variables
load_dotenv()  # загружает .env автоматически


'''
def get_engine():
    db_type = variables.get("db_type", default="sqlite")
    if db_type == "postgres":
        #return create_engine("postgresql+psycopg2://prefect:prefect@postgres:5432/prefect")
        host = os.getenv("DB_HOST", "localhost")
        return create_engine(f"postgresql+psycopg2://prefect:prefect@{host}:5432/prefect")
    else:
        db_path = variables.get("db_type", default="social_media_analytics.db")
        return create_engine(f"sqlite:///{db_path}")
'''


def get_engine():
    db_type = variables.get("db_type", default="sqlite")
    if db_type == "postgres":
        host = variables.get("db_host", default="localhost")
        return create_engine(f"postgresql+psycopg2://prefect:prefect@{host}:5432/prefect")
    else:
        db_path = variables.get("db_path", default="social_media_analytics.db")
        return create_engine(f"sqlite:///{db_path}")