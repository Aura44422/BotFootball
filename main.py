import asyncio
import logging
import os
from datetime import datetime, timedelta
import json
import uuid
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, 
    ContextTypes, MessageHandler, filters
)
from telegram.constants import ParseMode
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import atexit
import signal
from aiohttp import web

from models import init_db, User, Subscription, async_session
from database_service import DatabaseService
from match_service import MatchService
from payment_service import PaymentService
from sqlalchemy import select, and_

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Initialize services
db_service = DatabaseService()
match_service = MatchService()
payment_service = PaymentService()
payment_service.set_db_service(db_service)

# Admin IDs from environment variables
ADMIN_IDS = []
for i in range(1, 6):
    admin_id = os.getenv(f"ADMIN_ID_{i}")
    if admin_id and admin_id.isdigit():
        ADMIN_IDS.append(int(admin_id))

async def is_admin(user_id):
    """Check if a user is an admin"""
    return user_id in ADMIN_IDS

async def is_user_subscribed(user_id):
    """Check if a user has an active subscription or trial messages"""
    user = await db_service.get_user_by_telegram_id(user_id)
    if not user:
        return False
    has_subscription = await db_service.has_active_subscription(user.id)
    has_trial = user.trial_messages_left > 0
    return has_subscription or has_trial

async def decrement_trial_message(user_id):
    """Decrement a trial message if user is on trial"""
    return await db_service.decrement_trial_message(user_id)

async def send_beautiful_message(update, context, text, reply_markup=None):
    """Send a premium, beautifully formatted message"""
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = await db_service.get_or_create_user(
        user.id, user.username, user.first_name, user.last_name
    )
    welcome_text = (
        "*OddFury — бот для поиска футбольных матчей по уникальным коэффициентам.*\n\n"
        "Бот ищет только такие сочетания коэффициентов:\n"
        "• 4.25 и 1.225\n"
        "• 4.22 и 1.225\n\n"
        f"Осталось бесплатных запросов: *{db_user.trial_messages_left}*\n\n"
        "Для полного доступа оформите подписку."
    )
    keyboard = [
        [InlineKeyboardButton("🔎 Найти матчи", callback_data="find_matches")],
        [InlineKeyboardButton("💳 Оформить подписку", callback_data="buy_subscription")]
    ]
    if await is_admin(user.id):
        keyboard.append([InlineKeyboardButton("🔒 Админ-панель", callback_data="admin_panel")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await send_beautiful_message(update, context, welcome_text, reply_markup)

async def find_matches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await is_user_subscribed(user_id):
        await subscription_required(update, context)
        return
    loading_msg = await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Поиск матчей...",
        parse_mode=ParseMode.MARKDOWN
    )
    matches = await match_service.check_for_matches_with_target_odds()
    user = await db_service.get_user_by_telegram_id(user_id)
    is_on_trial = user and user.trial_messages_left > 0 and not await db_service.has_active_subscription(user.id)

    if not matches:
        if is_on_trial:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=loading_msg.message_id,
                text=f"Поиск завершён. Осталось бесплатных запросов: *{user.trial_messages_left}*\n\nВ данный момент нет подходящих матчей.\nВы получите уведомление, как только они появятся!",
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=loading_msg.message_id,
                text="Поиск завершён.\n\nВ данный момент нет подходящих матчей.\nВы получите уведомление, как только они появятся!",
                parse_mode=ParseMode.MARKDOWN
            )
        await send_beautiful_message(
            update, context,
            "В данный момент нет подходящих матчей.\nВы получите уведомление, как только они появятся.",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data="start")]])
        )
        return

    # Only decrement trial message if matches are found
    if is_on_trial:
        remaining = await decrement_trial_message(user_id)
        trial_msg = f"\n\nОсталось бесплатных запросов: *{remaining}*"
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=loading_msg.message_id,
            text=f"Поиск завершён!{trial_msg}",
            parse_mode=ParseMode.MARKDOWN
        )
    else:
        await context.bot.edit_message_text(
            chat_id=update.effective_chat.id,
            message_id=loading_msg.message_id,
            text="Поиск завершён!",
            parse_mode=ParseMode.MARKDOWN
        )

    for match in matches:
        await send_match_info(context.bot, update.effective_chat.id, match)
        await match_service.mark_match_as_notified(match.id)
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Показаны все найденные матчи.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data="start")]])
    )

async def subscription_required(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("💳 Оформить подписку", callback_data="buy_subscription")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="start")]
    ]
    await send_beautiful_message(
        update, context,
        "Для доступа к этой функции необходима подписка или бесплатные запросы.",
        InlineKeyboardMarkup(keyboard)
    )

async def buy_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("1 неделя — 650₽", callback_data="sub_week")],
        [InlineKeyboardButton("2 недели — 1300₽ (экономия 300₽)", callback_data="sub_two_weeks")],
        [InlineKeyboardButton("1 месяц — 2500₽ (экономия 700₽)", callback_data="sub_month")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="start")]
    ]
    await send_beautiful_message(
        update, context,
        "*Выберите тариф OddFury:*",
        InlineKeyboardMarkup(keyboard)
    )

async def process_subscription_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    sub_type = query.data.replace("sub_", "")
    user_id = update.effective_user.id
    payment_info = await payment_service.create_payment_link(user_id, sub_type)
    if not payment_info:
        await send_beautiful_message(
            update, context,
            "Ошибка при создании ссылки на оплату. Попробуйте позже.",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data="start")]])
        )
        return
    sub_names = {"week": "1 неделя", "two_weeks": "2 недели", "month": "1 месяц"}
    sub_name = sub_names.get(sub_type, sub_type)
    payment_text = (
        f"*Оплата подписки OddFury: {sub_name}*\n\n"
        f"Сумма: *{payment_info['amount']}₽*\n"
        + (f"Экономия: *{payment_info['discount']}₽*\n" if payment_info['discount'] > 0 else "") +
        "\nНужно установить нужную сумму выбранной подписки и оплатить. Если платёж будет меньше указанной суммы — подписка не активируется. Будьте внимательны!\n\n"
        "Для оплаты перейдите по ссылке ниже. После оплаты подписка активируется автоматически.\n\n"
        f"ID платежа: `{payment_info['unique_id']}`"
    )
    keyboard = [
        [InlineKeyboardButton("💳 Оплатить", url=payment_info["payment_url"])],
        [InlineKeyboardButton("✅ Я оплатил", callback_data=f"check_payment_{payment_info['unique_id']}")],
        [InlineKeyboardButton("⬅️ Отмена", callback_data="start")]
    ]
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=payment_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def check_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    unique_id = query.data.replace("check_payment_", "")
    payment_result = await payment_service.check_payment(unique_id)
    if payment_result["success"]:
        sub_types = {"week": "1 неделя", "two_weeks": "2 недели", "month": "1 месяц"}
        sub_type_name = sub_types.get(payment_result["subscription_type"], payment_result["subscription_type"])
        success_text = (
            f"*Подписка активирована!*\n\n"
            f"Тип: {sub_type_name}\n"
            f"Действует до: {payment_result['end_date']}\n\n"
            f"Спасибо, что выбрали OddFury — сервис поиска футбольных матчей по коэффициентам."
        )
        await send_beautiful_message(update, context, success_text)
        loading_msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Ищем для вас лучшие матчи...",
            parse_mode=ParseMode.MARKDOWN
        )
        await asyncio.sleep(2)
        matches = await match_service.check_for_matches_with_target_odds()
        if matches:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=loading_msg.message_id,
                text="Найдены матчи!",
                parse_mode=ParseMode.MARKDOWN
            )
            for match in matches:
                await send_match_info(context.bot, update.effective_chat.id, match)
                await match_service.mark_match_as_notified(match.id)
        else:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=loading_msg.message_id,
                text="Пока нет подходящих матчей. Вы получите уведомление, как только они появятся!",
                parse_mode=ParseMode.MARKDOWN
            )
    else:
        error_text = (
            "Оплата не найдена или ещё не обработана.\n"
            "Пожалуйста, подождите пару минут и попробуйте снова."
        )
        keyboard = [
            [InlineKeyboardButton("Проверить ещё раз", callback_data=f"check_payment_{unique_id}")],
            [InlineKeyboardButton("⬅️ В меню", callback_data="start")]
        ]
        await send_beautiful_message(update, context, error_text, InlineKeyboardMarkup(keyboard))

async def send_match_info(bot, chat_id, match, is_notification=False):
    match_time = match.match_time.strftime("%d.%m.%Y %H:%M")
    prefix = "НОВЫЙ МАТЧ!\n" if is_notification else ""
    match_text = (
        f"{prefix}*{match.home_team} — {match.away_team}*\n"
        f"{match.competition}\n"
        f"{match.match_time.strftime('%d.%m.%Y %H:%M')}\n\n"
        f"Коэффициенты:\n"
        f"1: {match.odds_1:.2f}   X: {match.odds_x:.2f}   2: {match.odds_2:.3f}"
    )
    if match.match_url:
        keyboard = [[InlineKeyboardButton("🔗 Ссылка на матч", url=match.match_url)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
    else:
        reply_markup = None
    await bot.send_message(
        chat_id=chat_id,
        text=match_text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=reply_markup
    )

async def notify_users_about_new_matches(context: ContextTypes.DEFAULT_TYPE):
    """Check for new matches and notify subscribed users"""
    try:
        # Fetch new matches first
        await match_service.fetch_matches()
        
        # Get matches with target odds
        matches = await match_service.check_for_matches_with_target_odds()
        
        if not matches:
            logger.info("No new matches with target odds found")
            return
        
        # Get all users with active subscriptions
        async with async_session() as session:
            # Find users with active subscriptions
            result = await session.execute(
                select(User).join(Subscription).where(
                    Subscription.end_date >= datetime.utcnow()
                ).distinct()
            )
            subscribed_users = result.scalars().all()
            
            for match in matches:
                logger.info(f"Notifying users about match: {match.home_team} vs {match.away_team}")
                
                # Notify each user with active subscription
                for user in subscribed_users:
                    try:
                        await send_match_info(context.bot, user.telegram_id, match, is_notification=True)
                    except Exception as e:
                        logger.error(f"Failed to notify user {user.telegram_id}: {e}")
                
                # Mark match as notified
                await match_service.mark_match_as_notified(match.id)
                
        logger.info(f"Notified {len(subscribed_users)} users about {len(matches)} matches")
    
    except Exception as e:
        logger.error(f"Error in notify_users_about_new_matches: {e}")

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await send_beautiful_message(
            update, context,
            "⛔️ У вас нет доступа к премиум-админ-панели.",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ В меню", callback_data="start")]])
        )
        return
    keyboard = [
        [InlineKeyboardButton("Статистика 📊", callback_data="admin_stats")],
        [InlineKeyboardButton("Выдать подписку ➕", callback_data="admin_give_sub")],
        [InlineKeyboardButton("Аннулировать подписку ➖", callback_data="admin_revoke_sub")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="start")]
    ]
    await send_beautiful_message(
        update, context,
        "🔒 *Премиум-админ-панель OddFury*",
        InlineKeyboardMarkup(keyboard)
    )

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await send_beautiful_message(update, context, "⛔️ У вас нет доступа к этой функции.")
        return
    stats = await db_service.get_weekly_stats()
    stats_text = (
        f"📊 *Статистика OddFury за неделю*\n"
        f"Период: {stats['week_start']} — {stats['week_end']}\n\n"
        f"👥 Активных подписок: {stats['active_subscriptions']}\n"
        f"👤 Пользователей без подписки: {stats['inactive_users']}\n"
        f"🆕 Новых подписок: {stats['new_subscriptions']}\n"
    )
    if stats['most_popular_subscription']:
        sub_types = {"week": "1 неделя", "two_weeks": "2 недели", "month": "1 месяц"}
        most_popular = sub_types.get(stats['most_popular_subscription'], stats['most_popular_subscription'])
        stats_text += f"🔝 Самая популярная подписка: {most_popular}"
    else:
        stats_text += "🔝 Самая популярная подписка: нет данных"
    keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="admin_panel")]]
    await send_beautiful_message(update, context, stats_text, InlineKeyboardMarkup(keyboard))

async def admin_give_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await send_beautiful_message(update, context, "⛔️ У вас нет доступа к этой функции.")
        return
    context.user_data["admin_give_sub"] = True
    await send_beautiful_message(
        update, context,
        "👤 Введите username пользователя (без @), которому вы хотите выдать премиум-подписку:",
        InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Отмена", callback_data="admin_panel")]])
    )

async def handle_admin_give_sub_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.message.text.strip().replace("@", "")
    context.user_data["sub_username"] = username
    keyboard = [
        [InlineKeyboardButton("1 неделя", callback_data="admin_give_week")],
        [InlineKeyboardButton("2 недели", callback_data="admin_give_two_weeks")],
        [InlineKeyboardButton("1 месяц", callback_data="admin_give_month")],
        [InlineKeyboardButton("⬅️ Отмена", callback_data="admin_panel")]
    ]
    await send_beautiful_message(
        update, context,
        "⏱️ Выберите срок премиум-подписки для @{username}:",
        InlineKeyboardMarkup(keyboard)
    )
    context.user_data.pop("admin_give_sub", None)

async def admin_process_give_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    sub_type = query.data.replace("admin_give_", "")
    username = context.user_data.get("sub_username", "").strip()
    if not username:
        await send_beautiful_message(
            update, context,
            "❌ Не указано имя пользователя.",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_panel")]])
        )
        return
    result = await db_service.admin_create_subscription(username, sub_type)
    if not result:
        await send_beautiful_message(
            update, context,
            f"❌ Пользователь @{username} не найден.",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_panel")]])
        )
        return
    subscription, user_telegram_id = result
    sub_types = {"week": "1 неделя", "two_weeks": "2 недели", "month": "1 месяц"}
    sub_name = sub_types.get(sub_type, sub_type)
    end_date = subscription.end_date.strftime("%d.%m.%Y %H:%M")
    admin_text = (
        f"*Подписка успешно выдана пользователю @{username}*\n"
        f"Тип: {sub_name}\nДействует до: {end_date}"
    )
    await send_beautiful_message(
        update, context,
        admin_text,
        InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_panel")]])
    )
    user_text = (
        f"*Вам выдана подписка OddFury!*\n\n"
        f"Тип: {sub_name}\nДействует до: {end_date}\n\n"
        f"Спасибо, что выбрали OddFury."
    )
    try:
        await context.bot.send_message(
            chat_id=user_telegram_id,
            text=user_text,
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Failed to send notification to user {user_telegram_id}: {e}")
    if "sub_username" in context.user_data:
        del context.user_data["sub_username"]

async def admin_revoke_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await is_admin(user_id):
        await send_beautiful_message(update, context, "⛔️ У вас нет доступа к этой функции.")
        return
    context.user_data["admin_revoke_sub"] = True
    await send_beautiful_message(
        update, context,
        "👤 Введите username пользователя (без @) для аннулирования премиум-подписки:",
        InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Отмена", callback_data="admin_panel")]])
    )

async def handle_admin_revoke_sub_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.message.text.strip().replace("@", "")
    user_telegram_id = await db_service.revoke_subscription(username)
    if not user_telegram_id:
        await send_beautiful_message(
            update, context,
            f"❌ Пользователь @{username} не найден или у него нет активной подписки.",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_panel")]])
        )
        return
    admin_text = f"*Подписка пользователя @{username} аннулирована.*"
    await send_beautiful_message(
        update, context,
        admin_text,
        InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin_panel")]])
    )
    user_text = (
        "*Ваша подписка OddFury была аннулирована администратором.*\n\n"
        "Если вы считаете это ошибкой — свяжитесь с поддержкой OddFury."
    )
    try:
        await context.bot.send_message(
            chat_id=user_telegram_id,
            text=user_text,
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Failed to send notification to user {user_telegram_id}: {e}")
    context.user_data.pop("admin_revoke_sub", None)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle text messages for admin operations"""
    # Check if we're waiting for a username for giving subscription
    if context.user_data.get("admin_give_sub"):
        await handle_admin_give_sub_username(update, context)
        return
        
    # Check if we're waiting for a username for revoking subscription
    if context.user_data.get("admin_revoke_sub"):
        await handle_admin_revoke_sub_username(update, context)
        return
        
    # Default response for other messages
    await start(update, context)

async def button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries from inline keyboards"""
    query = update.callback_query
    await query.answer()
    
    callback_data = query.data
    
    if callback_data == "start":
        await start(update, context)
    elif callback_data == "find_matches":
        await find_matches(update, context)
    elif callback_data == "buy_subscription":
        await buy_subscription(update, context)
    elif callback_data.startswith("sub_"):
        await process_subscription_selection(update, context)
    elif callback_data.startswith("check_payment_"):
        await check_payment(update, context)
    elif callback_data == "admin_panel":
        await admin_panel(update, context)
    elif callback_data == "admin_stats":
        await admin_stats(update, context)
    elif callback_data == "admin_give_sub":
        await admin_give_subscription(update, context)
    elif callback_data == "admin_revoke_sub":
        await admin_revoke_subscription(update, context)
    elif callback_data.startswith("admin_give_"):
        await admin_process_give_subscription(update, context)

async def weekly_stats_job(context: ContextTypes.DEFAULT_TYPE):
    """Send weekly stats to all admins"""
    # Get statistics
    stats = await db_service.get_weekly_stats()
    
    stats_text = f"📊 *Еженедельный отчет ({stats['week_start']} - {stats['week_end']})*\n\n"
    stats_text += f"👥 Активных подписок: {stats['active_subscriptions']}\n"
    stats_text += f"👤 Пользователей без подписки: {stats['inactive_users']}\n"
    stats_text += f"🆕 Новых подписок за неделю: {stats['new_subscriptions']}\n"
    
    if stats['most_popular_subscription']:
        sub_types = {
            "week": "1 неделя",
            "two_weeks": "2 недели",
            "month": "1 месяц"
        }
        most_popular = sub_types.get(stats['most_popular_subscription'], stats['most_popular_subscription'])
        stats_text += f"🔝 Самая популярная подписка: {most_popular}"
    else:
        stats_text += "🔝 Самая популярная подписка: нет данных"
    
    # Send to all admins
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=stats_text,
                parse_mode=ParseMode.MARKDOWN
            )
        except Exception as e:
            logger.error(f"Failed to send weekly stats to admin {admin_id}: {e}")

async def fetch_matches_job(context: ContextTypes.DEFAULT_TYPE):
    """Job to fetch new football matches periodically"""
    try:
        await match_service.fetch_matches()
        logger.info("Scheduled match fetching completed")
    except Exception as e:
        logger.error(f"Error in scheduled match fetching: {e}")

async def send_subscription_expiry_notification(context: ContextTypes.DEFAULT_TYPE):
    """Send notification to users whose subscription is about to expire"""
    try:
        # Notify users 1 day before expiry
        expiry_threshold = datetime.utcnow() + timedelta(days=1)
        
        async with async_session() as session:
            # Find subscriptions that expire in approximately 24 hours
            result = await session.execute(
                select(Subscription, User).join(User).where(
                    and_(
                        Subscription.end_date <= expiry_threshold,
                        Subscription.end_date >= datetime.utcnow()
                    )
                )
            )
            
            expiring_subscriptions = result.all()
            
            for subscription, user in expiring_subscriptions:
                # Format the expiry date
                expiry_date = subscription.end_date.strftime("%d.%m.%Y %H:%M")
                
                # Subscription type
                sub_types = {
                    "week": "1 неделя",
                    "two_weeks": "2 недели",
                    "month": "1 месяц"
                }
                sub_type = sub_types.get(subscription.subscription_type, subscription.subscription_type)
                
                # Create expiry notification message
                expiry_text = f"⚠️ *Внимание! Срок вашей подписки заканчивается*\n\n"
                expiry_text += f"Тип подписки: {sub_type}\n"
                expiry_text += f"Действительна до: {expiry_date}\n\n"
                expiry_text += "Чтобы продолжить получать информацию о матчах, пожалуйста, продлите подписку."
                
                # Create inline keyboard for renewal
                keyboard = [
                    [InlineKeyboardButton("💰 Продлить подписку", callback_data="buy_subscription")],
                    [InlineKeyboardButton("🔙 Вернуться в меню", callback_data="start")]
                ]
                
                # Send notification
                try:
                    await context.bot.send_message(
                        chat_id=user.telegram_id,
                        text=expiry_text,
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                    logger.info(f"Sent expiry notification to user {user.telegram_id}")
                except Exception as e:
                    logger.error(f"Failed to send expiry notification to user {user.telegram_id}: {e}")
    
    except Exception as e:
        logger.error(f"Error in send_subscription_expiry_notification: {e}")

# Глобальный error handler
async def error_handler(update, context):
    """Глобальный обработчик ошибок Telegram. Логирует и уведомляет админа."""
    logger.error(f"Exception: {context.error}", exc_info=True)
    # Уведомление админу (если задан)
    if ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=ADMIN_IDS[0],
                text=f"❗️ Exception: {context.error}\n{getattr(update, 'effective_user', None)}"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin: {e}")

# Healthcheck endpoint
async def healthcheck(request):
    return web.Response(text="OK", status=200)

def run_healthcheck_server():
    loop = asyncio.new_event_loop()  # Создаем новый event loop для этого потока
    asyncio.set_event_loop(loop)     # Устанавливаем его как текущий event loop
    app = web.Application()
    app.router.add_get('/health', healthcheck)
    runner = web.AppRunner(app)
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    loop.run_until_complete(site.start())
    loop.run_forever()

# Graceful shutdown
should_exit = False
def handle_signal(sig, frame):
    global should_exit
    logger.info(f"Received signal {sig}, shutting down...")
    should_exit = True

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

async def async_init():
    """Асинхронная инициализация сервисов и БД."""
    await db_service.initialize()
    await match_service.api_client.fetch_matches()  # warmup
    await payment_service.initialize()

def main():
    """Главная точка входа. Запускает Telegram-бота, healthcheck и планировщик задач."""
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_init())
    application = Application.builder().token(os.getenv("BOT_TOKEN")).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_click))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_error_handler(error_handler)
    scheduler = AsyncIOScheduler(event_loop=loop)
    scheduler.add_job(
        lambda: asyncio.create_task(fetch_matches_job(application)),
        'interval', 
        hours=3, 
        id='fetch_matches'
    )
    scheduler.add_job(
        lambda: asyncio.create_task(notify_users_about_new_matches(application)),
        'interval', 
        hours=1, 
        id='notify_new_matches'
    )
    scheduler.add_job(
        lambda: asyncio.create_task(send_subscription_expiry_notification(application)),
        'cron',
        hour=10,
        minute=0,
        id='subscription_expiry_notification'
    )
    scheduler.add_job(
        lambda: asyncio.create_task(weekly_stats_job(application)), 
        'cron', 
        day_of_week='mon', 
        hour=9, 
        minute=0, 
        id='weekly_stats'
    )
    scheduler.start()
    import threading
    threading.Thread(target=run_healthcheck_server, daemon=True).start()
    import atexit
    def cleanup():
        loop.run_until_complete(match_service.api_client.fetch_matches())
        loop.run_until_complete(payment_service.close())
        loop.run_until_complete(db_service.close())
    atexit.register(cleanup)
    application.run_polling()
    # Graceful shutdown loop
    global should_exit
    while not should_exit:
        try:
            loop.run_until_complete(asyncio.sleep(1))
        except KeyboardInterrupt:
            break
    logger.info("Bot stopped.")

if __name__ == "__main__":
    main() 
