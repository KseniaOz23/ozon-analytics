from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.config import load_config

def get_db_engine():
    cfg = load_config("config/project.yaml")["database"]
    # Строка подключения для psycopg2
    conn_str = f"postgresql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['name']}"
    return create_engine(conn_str)

def get_session():
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    return Session()