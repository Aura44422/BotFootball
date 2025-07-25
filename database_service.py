import os
import logging
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, select, and_, text, or_, desc, func
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
import threading

# --- Автоматическая подгрузка .env ---
class EnvWatcher:
    def __init__(self, env_path=None, poll_interval=5):
        self.env_path = env_path or find_dotenv()
        self.poll_interval = poll_interval
        self.last_mtime = None
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._watch, daemon=True)
        self._thread.start()
    def _watch(self):
        while not self._stop_event.is_set():
            try:
                if os.path.exists(self.env_path):
                    mtime = os.path.getmtime(self.env_path)
                    if self.last_mtime is None or mtime != self.last_mtime:
                        load_dotenv(self.env_path, override=True)
                        self.last_mtime = mtime
            except Exception as e:
                logging.error(f"EnvWatcher error: {e}")
            self._stop_event.wait(self.poll_interval)
    def stop(self):
        self._stop_event.set()
        self._thread.join()
_env_watcher = EnvWatcher()

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True)
    username = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    registration_date = Column(DateTime, default=datetime.utcnow)
    trial_messages_left = Column(Integer, default=3)

class Subscription(Base):
    __tablename__ = "subscriptions"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    subscription_type = Column(String)
    price_paid = Column(Float)
    payment_id = Column(String)

class PaymentLink(Base):
    __tablename__ = "payment_links"
    id = Column(Integer, primary_key=True)
    telegram_user_id = Column(Integer)
    unique_id = Column(String, unique=True)
    subscription_type = Column(String)
    amount = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    paid = Column(Boolean, default=False)

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
    match_url = Column(String)

class DatabaseService:
    """
    Абстракция для работы с БД. Автоматически подгружает конфиги, поддерживает разные БД, 
    асинхронная работа, пул соединений, автоматическое создание и миграция схемы, логирование, fallback.
    """
    def __init__(self):
        self.engine = None
        self.async_session = None
        self.dsn = None
        self._load_config()
    def _load_config(self):
        self.dsn = os.getenv("DB_DSN") or f"sqlite+aiosqlite:///{os.getenv('DB_PATH', 'football_bot.db')}"
    async def initialize(self):
        self._load_config()
        self.engine = create_async_engine(self.dsn, echo=False, future=True, pool_pre_ping=True)
        self.async_session = sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)
        # Автоматически создаём все таблицы
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    async def close(self):
        if self.engine:
            await self.engine.dispose()
    async def get_user_by_telegram_id(self, telegram_id):
        async with self.async_session() as session:
            result = await session.execute(select(User).where(User.telegram_id == telegram_id))
            return result.scalars().first()
    async def get_or_create_user(self, telegram_id, username, first_name, last_name):
        async with self.async_session() as session:
            user = await self.get_user_by_telegram_id(telegram_id)
            if user:
                return user
            user = User(
                telegram_id=telegram_id,
                username=username,
                first_name=first_name,
                last_name=last_name,
                registration_date=datetime.utcnow(),
                trial_messages_left=3
            )
            session.add(user)
            await session.commit()
            await session.refresh(user)
            return user
    async def has_active_subscription(self, user_id):
        async with self.async_session() as session:
            now = datetime.utcnow()
            result = await session.execute(
                select(Subscription).where(
                    and_(Subscription.user_id == user_id, Subscription.end_date >= now)
                )
            )
            return result.scalars().first() is not None
    async def decrement_trial_message(self, user_id):
        async with self.async_session() as session:
            user = await session.get(User, user_id)
            if user and user.trial_messages_left > 0:
                user.trial_messages_left -= 1
                await session.commit()
                return user.trial_messages_left
            return 0
    async def create_payment_link(self, telegram_user_id, subscription_type, amount):
        import uuid
        unique_id = uuid.uuid4().hex[:10]
        async with self.async_session() as session:
            payment_link = PaymentLink(
                telegram_user_id=telegram_user_id,
                unique_id=unique_id,
                subscription_type=subscription_type,
                amount=amount
            )
            session.add(payment_link)
            await session.commit()
            await session.refresh(payment_link)
            return payment_link
    async def get_payment_link(self, unique_id):
        async with self.async_session() as session:
            result = await session.execute(
                select(PaymentLink).where(PaymentLink.unique_id == unique_id)
            )
            return result.scalars().first()
    async def mark_payment_as_paid(self, unique_id):
        async with self.async_session() as session:
            payment_link = await session.execute(
                select(PaymentLink).where(PaymentLink.unique_id == unique_id)
            )
            payment_link = payment_link.scalars().first()
            if payment_link and not payment_link.paid:
                payment_link.paid = True
                await session.commit()
                return payment_link
            return None
    async def create_subscription(self, user_id, subscription_type, amount, payment_id):
        async with self.async_session() as session:
            now = datetime.utcnow()
            if subscription_type == "week":
                end_date = now + timedelta(weeks=1)
            elif subscription_type == "two_weeks":
                end_date = now + timedelta(weeks=2)
            elif subscription_type == "month":
                end_date = now + timedelta(days=31)
            else:
                end_date = now + timedelta(weeks=1)
            subscription = Subscription(
                user_id=user_id,
                start_date=now,
                end_date=end_date,
                subscription_type=subscription_type,
                price_paid=amount,
                payment_id=payment_id
            )
            session.add(subscription)
            await session.commit()
            await session.refresh(subscription)
            return subscription
    @staticmethod
    async def admin_create_subscription(username, subscription_type):
        async with self.async_session() as session:
            user_result = await session.execute(select(User).where(User.username == username))
            user = user_result.scalars().first()
            
            if not user:
                return None
            
            end_date = datetime.utcnow()
            if subscription_type == "week":
                end_date += timedelta(days=7)
                price = 650
            elif subscription_type == "two_weeks":
                end_date += timedelta(days=14)
                price = 1300
            elif subscription_type == "month":
                end_date += timedelta(days=30)
                price = 2500
            
            subscription = Subscription(
                user_id=user.id,
                end_date=end_date,
                subscription_type=subscription_type,
                price_paid=price,
                payment_id=f"admin_{uuid.uuid4().hex[:10]}"
            )
            
            session.add(subscription)
            await session.commit()
            await session.refresh(subscription)
            return subscription, user.telegram_id

    @staticmethod
    async def revoke_subscription(username):
        async with self.async_session() as session:
            user_result = await session.execute(select(User).where(User.username == username))
            user = user_result.scalars().first()
            
            if not user:
                return None
                
            subscription_result = await session.execute(
                select(Subscription)
                .where(and_(
                    Subscription.user_id == user.id,
                    Subscription.end_date >= datetime.utcnow()
                ))
            )
            
            subscription = subscription_result.scalars().first()
            
            if not subscription:
                return None
                
            subscription.end_date = datetime.utcnow() - timedelta(minutes=1)
            await session.commit()
            return user.telegram_id

    @staticmethod
    async def add_match(home_team, away_team, competition, match_time, odds_1, odds_x, odds_2, match_url=None):
        async with self.async_session() as session:
            match = Match(
                home_team=home_team,
                away_team=away_team,
                competition=competition,
                match_time=match_time,
                odds_1=odds_1,
                odds_x=odds_x,
                odds_2=odds_2,
                match_url=match_url
            )
            session.add(match)
            await session.commit()
            await session.refresh(match)
            return match

    @staticmethod
    async def get_matches_with_target_odds():
        async with self.async_session() as session:
            result = await session.execute(
                select(Match).where(
                    or_(
                        and_(
                            func.round(Match.odds_1, 2) == 4.25,
                            func.round(Match.odds_2, 3) == 1.225,
                            Match.notification_sent == False,
                            Match.match_time >= datetime.utcnow()
                        ),
                        and_(
                            func.round(Match.odds_1, 2) == 4.22,
                            func.round(Match.odds_2, 3) == 1.225,
                            Match.notification_sent == False,
                            Match.match_time >= datetime.utcnow()
                        )
                    )
                )
            )
            return result.scalars().all()

    @staticmethod
    async def mark_match_as_notified(match_id):
        async with self.async_session() as session:
            result = await session.execute(
                select(Match).where(Match.id == match_id)
            )
            match = result.scalars().first()
            if match:
                match.notification_sent = True
                await session.commit()

    async def get_weekly_stats(self):
        async with self.async_session() as session:
            today = datetime.utcnow().date()
            # Get current week's start (Monday) and end (Sunday)
            start_of_week = today - timedelta(days=today.weekday())
            end_of_week = start_of_week + timedelta(days=6)
            start_datetime = datetime.combine(start_of_week, datetime.min.time())
            end_datetime = datetime.combine(end_of_week, datetime.max.time())

            # Count active subscriptions
            active_subs_result = await session.execute(
                select(func.count(Subscription.id)).where(
                    Subscription.end_date >= datetime.utcnow()
                )
            )
            active_subscriptions = active_subs_result.scalar()
            
            # Count users without active subscriptions
            total_users_result = await session.execute(select(func.count(User.id)))
            total_users = total_users_result.scalar()
            inactive_users = total_users - active_subscriptions
            
            # Count new subscriptions this week
            new_subs_result = await session.execute(
                select(func.count(Subscription.id)).where(
                    Subscription.start_date.between(start_datetime, end_datetime)
                )
            )
            new_subscriptions = new_subs_result.scalar()
            
            # Get the most popular subscription type this week
            sub_types_result = await session.execute(
                select(
                    Subscription.subscription_type,
                    func.count(Subscription.id).label('count')
                )
                .where(Subscription.start_date.between(start_datetime, end_datetime))
                .group_by(Subscription.subscription_type)
                .order_by(desc('count'))
            )
            
            subscription_counts = {}
            for row in sub_types_result:
                subscription_counts[row[0]] = row[1]
            
            most_popular = None
            if subscription_counts:
                most_popular = max(subscription_counts.items(), key=lambda x: x[1])[0]
            
            # Create stats record
            stats = Stats(
                week_start=start_datetime,
                week_end=end_datetime,
                week_number=start_of_week.isocalendar()[1],
                year=start_of_week.year,
                active_subscriptions=active_subscriptions,
                inactive_users=inactive_users,
                new_subscriptions=new_subscriptions,
                most_popular_subscription=most_popular,
                week_subscription_data=json.dumps(subscription_counts)
            )
            
            # Check if stats already exist for this week
            existing_stats_result = await session.execute(
                select(Stats).where(Stats.week_start == start_datetime)
            )
            existing_stats = existing_stats_result.scalars().first()
            
            if existing_stats:
                existing_stats.active_subscriptions = active_subscriptions
                existing_stats.inactive_users = inactive_users
                existing_stats.new_subscriptions = new_subscriptions
                existing_stats.most_popular_subscription = most_popular
                existing_stats.week_subscription_data = json.dumps(subscription_counts)
            else:
                session.add(stats)
            
            await session.commit()
            
            return {
                "active_subscriptions": active_subscriptions,
                "inactive_users": inactive_users,
                "new_subscriptions": new_subscriptions,
                "most_popular_subscription": most_popular,
                "subscription_counts": subscription_counts,
                "week_start": start_of_week.strftime("%d.%m.%Y"),
                "week_end": end_of_week.strftime("%d.%m.%Y")
            } 
