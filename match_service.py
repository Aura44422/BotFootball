import os
import logging
import asyncio
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

class FootballApiClient:
    """
    Абстракция для работы с внешним API футбольных матчей. Автоматически подхватывает ключи из env.
    Обрабатывает ошибки, логирует все действия, поддерживает fallback.
    """
    def __init__(self):
        self.api_key = os.getenv("THE_ODDS_API_KEY")
        self.base_url = os.getenv("FOOTBALL_API_URL", "https://api.the-odds-api.com/v4/sports/soccer_epl/odds/")
    async def fetch_matches(self):
        import aiohttp
        if not self.api_key:
            logging.error("THE_ODDS_API_KEY not set!")
            return []
        params = {"apiKey": self.api_key, "regions": "eu", "markets": "h2h"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params) as resp:
                    if resp.status != 200:
                        logging.error(f"Football API error: {resp.status}")
                        return []
                    data = await resp.json()
                    return data
        except Exception as e:
            logging.error(f"Football API request failed: {e}")
            return []

class MatchService:
    """
    Сервис для работы с футбольными матчами. Автоматически подгружает конфиги, поддерживает разные источники,
    кэширует данные, fallback, логирование, расширяемость.
    """
    def __init__(self):
        self.api_client = FootballApiClient()
        self.cache = []
        self.last_update = None
        self.cache_ttl = int(os.getenv("MATCH_CACHE_TTL", 600))  # 10 минут по умолчанию
    async def fetch_matches(self):
        """
        Загружает свежие матчи из внешнего API. Если API недоступен — возвращает кэш.
        """
        now = datetime.utcnow()
        if self.last_update and (now - self.last_update).total_seconds() < self.cache_ttl:
            return self.cache
        matches = await self.api_client.fetch_matches()
        if matches:
            self.cache = matches
            self.last_update = now
            return matches
        logging.warning("Using cached matches due to API failure.")
        return self.cache
    async def check_for_matches_with_target_odds(self, min_odds=1.5, max_odds=5.0):
        """
        Возвращает матчи с коэффициентами в заданном диапазоне и которые еще не завершились.
        Работает с кэшем и API.
        """
        matches = await self.fetch_matches()
        result = []
        now = datetime.utcnow() # Получаем текущее время по UTC
        
        for match in matches:
            try:
                # Преобразуем время матча в объект datetime (предполагая, что match_time находится в UTC)
                # API The Odds API обычно возвращает время в ISO 8601 формате, которое datetime.fromisoformat может обработать
                match_time_str = match.get("commence_time") # Используем "commence_time" из API
                if not match_time_str:
                    logging.warning(f"Match missing commence_time: {match.get('id')}")
                    continue

                match_time = datetime.fromisoformat(match_time_str.replace('Z', '+00:00')) # Убедимся, что формат совместим с fromisoformat
                
                # Фильтруем матчи, которые уже прошли
                if match_time < now:
                    continue

                odds_1 = float(match.get("odds_1", 0))
                odds_2 = float(match.get("odds_2", 0))
                
                if min_odds <= odds_1 <= max_odds or min_odds <= odds_2 <= max_odds:
                    result.append(match)
            except Exception as e:
                logging.warning(f"Match parse or time comparison error: {e}")
        return result
    async def mark_match_as_notified(self, match_id):
        """
        Помечает матч как уведомлённый (можно реализовать через БД или кэш).
        """
        # Здесь можно реализовать запись в БД или кэш, если нужно
        pass 
