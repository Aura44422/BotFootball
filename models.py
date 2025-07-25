from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import os
import datetime
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = "sqlite+aiosqlite:///football_bot.db"
engine = create_async_engine(DATABASE_URL, echo=True)
Base = declarative_base()
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True)
    username = Column(String, nullable=True)
    first_name = Column(String)
    last_name = Column(String, nullable=True)
    registration_date = Column(DateTime, default=datetime.datetime.utcnow)
    trial_messages_left = Column(Integer, default=3)
    subscriptions = relationship("Subscription", back_populates="user")

class Subscription(Base):
    __tablename__ = "subscriptions"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    start_date = Column(DateTime, default=datetime.datetime.utcnow)
    end_date = Column(DateTime)
    subscription_type = Column(String)  # "week", "two_weeks", "month"
    price_paid = Column(Float)
    payment_id = Column(String, unique=True)
    user = relationship("User", back_populates="subscriptions")

class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True)
    home_team = Column(String)
    away_team = Column(String)
    competition = Column(String)
    match_time = Column(DateTime)
    odds_1 = Column(Float)
    odds_x = Column(Float)
    odds_2 = Column(Float)
    notification_sent = Column(Boolean, default=False)
    match_url = Column(String, nullable=True)

class PaymentLink(Base):
    __tablename__ = "payment_links"

    id = Column(Integer, primary_key=True)
    telegram_user_id = Column(Integer)
    unique_id = Column(String, unique=True)
    subscription_type = Column(String)
    amount = Column(Float)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    paid = Column(Boolean, default=False)

class Stats(Base):
    __tablename__ = "stats"

    id = Column(Integer, primary_key=True)
    week_start = Column(DateTime, unique=True)
    week_end = Column(DateTime)
    week_number = Column(Integer)
    year = Column(Integer)
    active_subscriptions = Column(Integer, default=0)
    inactive_users = Column(Integer, default=0)
    new_subscriptions = Column(Integer, default=0)
    most_popular_subscription = Column(String, nullable=True)
    week_subscription_data = Column(String)  # JSON string with subscription counts

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def get_session():
    async with async_session() as session:
        yield session 