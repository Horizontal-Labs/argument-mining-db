from config import DB_URI
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, scoped_session

engine: Engine | None = None

# Use scoped_session to ensure thread safety
SessionLocal: scoped_session | None = None


def get_engine():
    global engine
    if engine is None:
        engine = create_engine(DB_URI, pool_pre_ping=True)
    return engine


def get_session() -> scoped_session:
    global SessionLocal
    if SessionLocal is None:
        SessionLocal = scoped_session(
            sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
        )
    return SessionLocal()


def get_db_session():
    """Dependency that provides a SQLAlchemy session"""
    db = get_session()
    try:
        yield db
    finally:
        db.close()