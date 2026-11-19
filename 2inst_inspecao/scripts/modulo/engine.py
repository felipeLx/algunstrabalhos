from sqlalchemy import create_engine

def create_postgres_engine(
    host: str,
    dbname: str,
    port: int | str,
    user: str,
    password: str,
    echo: bool = False,
):
    """
    Create and return a SQLAlchemy PostgreSQL engine.
    """

    database_url = (
        f"postgresql+psycopg2://{user}:"
        f"{password}@"
        f"{host}:"
        f"{port}/"
        f"{dbname}"
    )

    return create_engine(
        database_url,
        pool_pre_ping=True,
        echo=echo,
        future=True
    )

def create_oracle_engine(user: str, password: str, dsn: str):
    """
    SQLAlchemy 1.4 compatible Oracle engine.
    """
    if not all([user, password, dsn]):
        raise ValueError("user, password, and dsn must be provided")

    return create_engine(
        f"oracle+cx_oracle://{user}:{password}@{dsn}",
        pool_pre_ping=True,
        future=True,
    )