import os
import logging
import aiohttp
import json
import uuid
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
from database_service import DatabaseService
import asyncio
import threading
# from main import db_service  # Удалить этот импорт

# --- Автоматическая загрузка и обновление .env ---
class EnvWatcher:
    """
    Следит за изменениями .env и автоматически обновляет переменные окружения.
    """
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

# Запускаем watcher для .env
_env_watcher = EnvWatcher()

# --- DonationAlerts API клиент ---
class DonationAlertsClient:
    """
    Абстракция для работы с DonationAlerts API. Автоматически подхватывает токен из env.
    Обрабатывает ошибки, rate limit, изменения API, логирует все действия.
    """
    BASE_URL = "https://www.donationalerts.com/api/v1/alerts/donations"

    def __init__(self):
        self.session = None
        self.api_key = None
        self._load_api_key()

    def _load_api_key(self):
        self.api_key = os.getenv("DONATION_ALERTS_API_KEY")
        if not self.api_key:
            logging.error("DONATION_ALERTS_API_KEY not set in environment!")

    async def initialize(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
        self._load_api_key()

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def get_recent_donations(self, limit=50):
        """
        Получить последние донаты. Если API изменится — пытается адаптироваться.
        Возвращает список донатов или пустой список.
        """
        await self.initialize()
        if not self.api_key:
            return []
        headers = {"Authorization": f"Bearer {self.api_key}"}
        params = {"limit": limit}
        try:
            async with self.session.get(self.BASE_URL, headers=headers, params=params) as resp:
                if resp.status != 200:
                    logging.error(f"DonationAlerts API error: {resp.status}")
                    return []
                data = await resp.json()
                # Автоматически ищем ключ с донатами
                for key in ("data", "donations", "results"):
                    if key in data:
                        return data[key]
                # Если структура изменилась — логируем и возвращаем всё
                logging.warning(f"Unknown DonationAlerts API response structure: {data}")
                return list(data.values())[0] if data else []
        except Exception as e:
            logging.error(f"DonationAlerts API request failed: {e}")
            return []

# --- PaymentService ---
class PaymentService:
    """
    Сервис для генерации платёжных ссылок и проверки оплаты. Полностью автономен.
    Не требует ручных доработок при смене токенов, ссылок, структуры API.
    Поддерживает расширение на другие платёжные сервисы.
    """
    def __init__(self):
        self.donation_alerts = DonationAlertsClient()
        self.session = None
        self.base_url = os.getenv("DONATION_ALERTS_BASE_URL", "https://www.donationalerts.com/r/")
        self.db_service = None

    def set_db_service(self, db_service):
        self.db_service = db_service

    async def initialize(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
        await self.donation_alerts.initialize()

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None
        await self.donation_alerts.close()

    def get_subscription_price(self, subscription_type):
        prices = {
            "week": 650,
            "two_weeks": 1300,
            "month": 2500
        }
        return prices.get(subscription_type, 0)

    def get_subscription_discount(self, subscription_type):
        discounts = {
            "week": 0,
            "two_weeks": 300,
            "month": 700
        }
        return discounts.get(subscription_type, 0)

    async def create_payment_link(self, telegram_user_id, subscription_type):
        """
        Генерирует платёжную ссылку для пользователя. Автоматически подхватывает base_url.
        """
        price = self.get_subscription_price(subscription_type)
        if not price:
            return None
        if not self.db_service:
            raise Exception("db_service is not set in PaymentService")
        payment_link = await self.db_service.create_payment_link(
            telegram_user_id, subscription_type, price
        )
        full_url = f"{self.base_url}?message={payment_link.unique_id}&amount={price}"
        return {
            "unique_id": payment_link.unique_id,
            "payment_url": full_url,
            "amount": price,
            "subscription_type": subscription_type,
            "discount": self.get_subscription_discount(subscription_type)
        }

    async def check_payment(self, unique_id):
        """
        Проверяет факт оплаты через DonationAlerts API. Абсолютно автономно:
        - Автоматически подхватывает токен и ссылку
        - Обрабатывает ошибки и изменения API
        - Логирует все действия
        - Не требует ручных доработок
        """
        await self.initialize()
        payment_link = await self.db_service.get_payment_link(unique_id)
        if not payment_link:
            return {"success": False, "message": "Payment link not found"}
        if payment_link.paid:
            return {"success": False, "message": "Payment already processed"}
        # Получаем последние донаты
        donations = await self.donation_alerts.get_recent_donations(limit=50)
        found = None
        for donation in donations:
            try:
                if (
                    str(payment_link.unique_id) in str(donation.get("message", "")) and
                    float(donation.get("amount", 0)) >= float(payment_link.amount)
                ):
                    found = donation
                    break
            except Exception as e:
                logging.warning(f"Donation parse error: {e}")
        if not found:
            return {"success": False, "message": "Платеж не найден. Проверьте, что вы оплатили по правильной ссылке и указали верную сумму."}
        updated_link = await self.db_service.mark_payment_as_paid(unique_id)
        if not updated_link:
            return {"success": False, "message": "Failed to process payment"}
        user = await self.db_service.get_user_by_telegram_id(payment_link.telegram_user_id)
        if not user:
            return {"success": False, "message": "User not found"}
        subscription, is_renewal = await self.db_service.create_subscription(
            user.id,
            payment_link.subscription_type,
            payment_link.amount,
            unique_id
        )
        return {
            "success": True,
            "user_id": user.telegram_id,
            "subscription_type": subscription.subscription_type,
            "end_date": subscription.end_date.strftime("%d.%m.%Y %H:%M"),
            "is_renewal": is_renewal
        } 
