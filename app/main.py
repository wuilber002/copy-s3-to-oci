from datetime import datetime, timezone
from typing import Generator

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import DateTime, Integer, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
import os


def read_secret(path: str) -> str:
    with open(path, "r", encoding="utf-8") as secret_file:
        return secret_file.read().strip()


database_url = os.environ["DATABASE_URL"]
password = read_secret(os.environ["POSTGRES_PASSWORD_FILE"])
database_url = database_url.replace("migration@", f"migration:{password}@")
engine = create_engine(database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    s3_bucket: Mapped[str] = mapped_column(String(255))
    s3_prefix: Mapped[str] = mapped_column(String(1024), default="")
    aws_region: Mapped[str] = mapped_column(String(64))
    destination_bucket: Mapped[str] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), default="CONFIGURED")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class SourceCreate(BaseModel):
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{1,127}$")
    s3_bucket: str
    s3_prefix: str = ""
    aws_region: str
    destination_bucket: str


app = FastAPI(title="S3 to OCI Migration", version="0.1.0")


@app.on_event("startup")
def create_schema() -> None:
    Base.metadata.create_all(engine)


def get_session() -> Generator[Session, None, None]:
    with SessionLocal() as session:
        yield session


@app.get("/healthz")
def healthcheck(session: Session = Depends(get_session)) -> dict:
    session.execute(select(1))
    return {"status": "ok"}


@app.get("/api/sources")
def list_sources(session: Session = Depends(get_session)) -> list[dict]:
    return [
        {"id": source.id, "name": source.name, "s3_bucket": source.s3_bucket, "s3_prefix": source.s3_prefix,
         "aws_region": source.aws_region, "destination_bucket": source.destination_bucket, "status": source.status}
        for source in session.scalars(select(Source).order_by(Source.id))
    ]


@app.post("/api/sources", status_code=201)
def create_source(payload: SourceCreate, session: Session = Depends(get_session)) -> dict:
    if session.scalar(select(Source).where(Source.name == payload.name)):
        raise HTTPException(status_code=409, detail="Source name already exists")
    source = Source(**payload.model_dump())
    session.add(source)
    session.commit()
    session.refresh(source)
    return {"id": source.id, "name": source.name, "status": source.status}
