import asyncio
import json
from typing import Set, Dict, List, Any
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, delete, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from datetime import datetime
from pydantic import BaseModel, Field, validator
from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

# Ініціалізація додатку FastAPI
app = FastAPI()

# Налаштування двигуна бази даних SQLAlchemy
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Визначення схеми таблиці ProcessedAgentData в базі даних
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)

# Конфігурація sessionmaker для операцій з базою даних
SessionLocal = sessionmaker(bind=engine)

# Опис моделі SQLAlchemy для взаємодії з базою даних
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime

# Моделі FastAPI для отримання даних з запитів
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float

class GpsData(BaseModel):
    latitude: float
    longitude: float

class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @validator("timestamp", pre=True)
    def check_timestamp(cls, value):
        """Перевірка правильності формату часу"""
        if not isinstance(value, datetime):
            try:
                return datetime.fromisoformat(value)
            except (TypeError, ValueError):
                raise ValueError("Невірний формат часу. Очікується формат ISO 8601.")
        return value

class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData

# Зберігання підписок на WebSocket за допомогою словника
subscriptions: Dict[int, Set[WebSocket]] = {}

# Визначення кінцевої точки WebSocket для комунікації в реальному часі
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    """Обробляє з'єднання та повідомлення WebSocket для реальних оновлень"""
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)

# Функція для відправлення даних всім підписчикам WebSocket для користувача
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))

# CRUDL-кінцеві точки для взаємодії з ProcessedAgentData
@app.post(
        "/processed_agent_data/"
        )
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    """Створення записів у таблиці ProcessedAgentData з отриманих даних"""
    with SessionLocal() as db:
        for data_item in data:
            values = get_values_from_data(data_item)
            db.execute(processed_agent_data.insert().values(values))
            db.commit()
    await send_data_to_subscribers(data[0].agent_data.user_id, data)

@app.get(
        "/processed_agent_data/{processed_agent_data_id}", 
        response_model=ProcessedAgentDataInDB
        )
def read_processed_agent_data(processed_agent_data_id: int):
    """Отримання конкретного запису з таблиці ProcessedAgentData"""
    with SessionLocal() as db:
        record = find_record_in_db(
            processed_agent_data_id, 
            db
            )
        return ProcessedAgentDataInDB(**record._asdict())

@app.get(
        "/processed_agent_data/", 
        response_model=list[ProcessedAgentDataInDB]
        )
def list_processed_agent_data():
    """Перелік усіх записів з таблиці ProcessedAgentData"""
    with SessionLocal() as db:
        result = db.execute(
            select(
                processed_agent_data
                )).fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Не знайдено")
        return [ProcessedAgentDataInDB(**row._asdict()) for row in result]

@app.put(
        "/processed_agent_data/{processed_agent_data_id}", 
        response_model=ProcessedAgentDataInDB
        )
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    """Оновлення конкретного запису в таблиці ProcessedAgentData"""
    with SessionLocal() as db:
        find_record_in_db(
            processed_agent_data_id, 
            db
            )
        values = get_values_from_data(data)
        db.execute(update(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id).values(values))
        db.commit()
        record = find_record_in_db(processed_agent_data_id, db)
        return ProcessedAgentDataInDB(**record._asdict())

@app.delete(
        "/processed_agent_data/{processed_agent_data_id}", 
        response_model=ProcessedAgentDataInDB
        )
def delete_processed_agent_data(processed_agent_data_id: int):
    """Видалення конкретного запису з таблиці ProcessedAgentData"""
    with SessionLocal() as db:
        record = find_record_in_db(
            processed_agent_data_id, 
            db
            )
        db.execute(delete(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id))
        db.commit()
        return ProcessedAgentDataInDB(**record._asdict())

# Допоміжні функції для операцій з базою даних
def find_record_in_db(id: int, database):
    """Отримання окремого запису за ідентифікатором з бази даних"""
    result = database.execute(select(processed_agent_data).where(processed_agent_data.c.id == id)).fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Не знайдено")
    return result

def get_values_from_data(data):
    """Витягнення значень з моделі даних для вставки в базу даних"""
    return {
        "road_state": data.road_state,
        "user_id": data.agent_data.user_id,
        "x": data.agent_data.accelerometer.x,
        "y": data.agent_data.accelerometer.y,
        "z": data.agent_data.accelerometer.z,
        "latitude": data.agent_data.gps.latitude,
        "longitude": data.agent_data.gps.longitude,
        "timestamp": data.agent_data.timestamp.isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
