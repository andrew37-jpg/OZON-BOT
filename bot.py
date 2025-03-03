import asyncio
import logging
import os
import random
import sqlite3
from itertools import islice
import aiohttp
import pymorphy2
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from dotenv import load_dotenv

# Загрузка переменных окружения из файла .env
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Настройка переменных
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
NOTIFICATION_CHANNEL_ID: str = os.getenv("NOTIFICATION_CHANNEL_ID", "")
OZON_API_URL: str = "https://api-seller.ozon.ru"
OZON_TOKEN: str = os.getenv("OZON_TOKEN", "")
CLIENT_ID: str = os.getenv("CLIENT_ID", "")
CHECK_INTERVAL: int = int(os.getenv("CHECK_INTERVAL", 300))
MAX_USERS: int = 5  # Максимальное количество пользователей

# Инициализация бота и диспетчера
bot: Bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp: Dispatcher = Dispatcher()
router: Router = Router()  # Создаем маршрутизатор

# Состояния для FSM
class UserStates(StatesGroup):
    waiting_for_start = State()


def init_db() -> None:
    """Инициализация базы данных и создание таблиц."""
    conn = sqlite3.connect("ozon_reviews.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_reviews (
            review_id TEXT PRIMARY KEY,
            sku TEXT,
            product_name TEXT,
            user_name TEXT,
            review_text TEXT,
            rating INTEGER,
            response_text TEXT,
            comment_id TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            is_active INTEGER DEFAULT 1
        )
    """)
    conn.commit()
    conn.close()


def save_review_to_db(review_id: str, sku: str, product_name: str, user_name: str, review_text: str, rating: int, response_text: str, comment_id: str) -> None:
    """Сохранить обработанный отзыв в базу данных."""
    conn = sqlite3.connect("ozon_reviews.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO processed_reviews (review_id, sku, product_name, user_name, review_text, rating, response_text, comment_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (review_id, sku, product_name, user_name, review_text, rating, response_text, comment_id))
    conn.commit()
    conn.close()


def is_review_processed(review_id: str) -> bool:
    """Проверить, обработан ли отзыв ранее."""
    conn = sqlite3.connect("ozon_reviews.db")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_reviews WHERE review_id = ?", (review_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None


def add_user(user_id: int, username: str) -> bool:
    """Добавить пользователя в базу данных."""
    conn = sqlite3.connect("ozon_reviews.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
    active_users = cursor.fetchone()[0]
    if active_users >= MAX_USERS:
        conn.close()
        return False
    cursor.execute("""
        INSERT OR IGNORE INTO users (user_id, username, is_active)
        VALUES (?, ?, 1)
    """, (user_id, username))
    conn.commit()
    conn.close()
    return True


def get_active_users() -> list:
    """Получить список активных пользователей."""
    conn = sqlite3.connect("ozon_reviews.db")
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE is_active = 1")
    users = cursor.fetchall()
    conn.close()
    return [user[0] for user in users]


async def notify_users(message: str) -> None:
    """Отправить уведомление всем активным пользователям."""
    active_users = get_active_users()
    for user_id in active_users:
        try:
            await bot.send_message(user_id, message, parse_mode="HTML")
            logging.info(f"Уведомление отправлено пользователю {user_id}")
        except Exception as e:
            logging.error(f"Ошибка отправки уведомления пользователю {user_id}: {e}")


@router.message(Command(commands=["start", "help"]))
async def send_welcome(message: Message, state: FSMContext) -> None:
    """Обработчик команды /start и /help."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Start", callback_data="start_bot")]
    ])
    await message.answer("Привет! Нажмите кнопку Start, чтобы активировать бот и получать уведомления.", reply_markup=keyboard)
    await state.set_state(UserStates.waiting_for_start)



@router.callback_query(UserStates.waiting_for_start)
async def handle_start_button(callback: CallbackQuery, state: FSMContext) -> None:
    """Обработка нажатия кнопки Start."""
    user_id = callback.from_user.id
    username = callback.from_user.username or str(user_id)

    if add_user(user_id, username):
        await callback.message.answer("✅ Вы успешно активировали бота! Теперь вы будете получать уведомления о новых отзывах.")
    else:
        await callback.message.answer("❌ Извините, достигнуто максимальное количество пользователей (5).")

    await state.clear()


async def get_product_info_from_card(sku: int, session: aiohttp.ClientSession) -> dict:
    """
    Получает информацию о товаре через Ozon API по SKU.

    Использует метод /v3/product/info/list для получения подробных данных.

    Args:
        sku (int): Идентификатор товара в системе Ozon.
        session (aiohttp.ClientSession): Сессия для выполнения HTTP-запросов.

    Returns:
        dict: Информация о товаре. Пустой словарь, если запрос завершился с ошибкой.
    """
    if not sku:
        logging.warning("SKU отсутствует, запрос пропущен.")
        return {}

    # Конечная точка для запроса информации о товарах
    url = f"{OZON_API_URL}/v3/product/info/list"
    headers = {
        "Client-Id": CLIENT_ID,
        "Api-Key": OZON_TOKEN,
        "Content-Type": "application/json",
    }
    payload = {"sku": [int(sku)]}  # В тело передается массив SKU

    try:
        async with session.post(url, json=payload, headers=headers) as response:
            logging.info(f"Запрос к Ozon API: статус ответа {response.status}")
            result = await response.json()

            if response.status == 200 and "items" in result and len(result["items"]) > 0:
                return result["items"][0]  # Возвращаем данные первого товара в списке
            else:
                logging.error(f"Ошибка при получении информации о товаре SKU {sku}: {result}")
    except aiohttp.ClientError as e:
        logging.error(f"Ошибка запроса к Ozon API для SKU {sku}: {e}")

    return {}


async def get_product_name_and_brand_by_sku(sku: int, session: aiohttp.ClientSession) -> tuple[str, str]:
    """Получение информации о названии товара и бренде по SKU с использованием Ozon API."""
    product_info = await get_product_info_from_card(sku, session)
    product_name = product_info.get("name", "")
    brand_name = product_info.get("brand", "").strip()

    if not brand_name:
        brand_name = "Guten Morgen"

    return product_name, brand_name


def get_brand_name(brand: str) -> str:
    """Извлекает название бренда из строки."""
    brand = brand.strip().lower()

    if "|" in brand:
        brand_name = brand.split("|")[0].strip()
    elif "/" in brand:
        brand_name = brand.split("/")[0].strip()
    else:
        brand_name = brand

    brand_mapping = {
        "diana store": "Diana Store",
        "guten morgen": "Guten Morgen",
        "ooo guten morgen": "Guten Morgen",
        "dianastore": "Diana Store",
    }

    for key in brand_mapping:
        if brand_name.startswith(key):
            return brand_mapping[key]

    return "Guten Morgen"


async def get_unprocessed_reviews(session: aiohttp.ClientSession) -> list:
    url = f"{OZON_API_URL}/v1/review/list"
    headers = {
        "Client-Id": CLIENT_ID,
        "Api-Key": OZON_TOKEN,
        "Content-Type": "application/json",
    }
    payload = {"limit": 50, "sort_dir": "ASC", "status": "UNPROCESSED"}

    try:
        async with session.post(url, json=payload, headers=headers) as response:
            logging.info(f"Статус ответа: {response.status}")
            result = await response.json()
            logging.info(f"Ответ API: {result}")
            if response.status == 200:
                return result.get("reviews", [])
            else:
                logging.error(f"Ошибка при запросе отзывов: {result}")
                return []
    except aiohttp.ClientError as e:
        logging.error(f"Ошибка сети при запросе API: {e}")
        return []


async def post_comment(review_id: str, text: str, session: aiohttp.ClientSession) -> str:
    """Отправить комментарий на отзыв через Ozon API."""
    url = f"{OZON_API_URL}/v1/review/comment/create"
    headers = {
        "Client-Id": CLIENT_ID,
        "Api-Key": OZON_TOKEN,
        "Content-Type": "application/json",
    }
    payload = {"mark_review_as_processed": True, "review_id": review_id, "text": text}

    try:
        async with session.post(url, json=payload, headers=headers) as response:
            result = await response.json()
            if response.status == 200:
                comment_id = result.get("comment_id", "")
                if comment_id:
                    logging.info(f"Комментарий успешно отправлен на отзыв ID: {review_id}, comment_id: {comment_id}")
                else:
                    logging.warning(f"Комментарий отправлен, но comment_id не получен для отзыва ID: {review_id}")
                return comment_id
            else:
                logging.error(f"Ошибка при отправке комментария: {result}")
    except aiohttp.ClientError as e:
        logging.error(f"Ошибка сети при отправке комментария: {e}")
    return ""


async def notify_channel(sku: int, response_text: str, rating: int, product_name: str, user_name: str, review_text: str) -> None:
    """Отправить уведомление в канал о новом комментарии."""
    user_name = user_name if user_name else "Аноним"

    def determine_brand(product_name: str) -> str:
        product_name_lower = product_name.lower() if product_name else ""
        if "diana" in product_name_lower:
            return "Diana"
        elif "guten morgen" in product_name_lower:
            return "Guten Morgen"
        elif "gm" in product_name_lower:
            return "Guten Morgen"
        else:
            return "Unknown"

    brand_name = determine_brand(product_name)
    if brand_name == "Unknown":
        brand_name = "Guten Morgen"

    if brand_name == "Diana":
        product_url = f"https://www.ozon.ru/product/polotentse-mahrovoe-diana-1-sht-50h90-visdom-hlopok-100-450-g-m2-{sku}/?at=PjtJn4mrrcpDJKlxi71M2m3Ux8Y9MYc7"
    elif brand_name == "Guten Morgen":
        product_url = f"https://www.ozon.ru/product/polotentse-mahrovoe-guten-morgen-1-sht-50h90-visdom-hlopok-100-450-g-m2-{sku}/?at=PjtJn4mrrcpDJKlxi71M2m3Ux8Y9MYc7"
    else:
        product_url = f"https://www.ozon.ru/product/unknown-brand-{sku}/?at=PjtJn4mrrcpDJKlxi71M2m3Ux8Y9MYc7"

    response_text = generate_response(rating, brand_name, product_name)

    review_message = (
        f"Бренд: {brand_name}\n"
        f"⭐️{'⭐️' * (rating - 1)}\n"
        f"Артикул Ozon: {sku} (<a href='{product_url}'>Ссылка на карточку товара</a>)\n"
        f"Товар: {product_name}\n\n"
        f"💬 {user_name}\n{review_text}\n\n"
        f"✅ Отправлен ответ:\n{response_text}"
    )

    try:
        await bot.send_message(NOTIFICATION_CHANNEL_ID, review_message, parse_mode="HTML")
        logging.info(f"Сообщение отправлено в канал Telegram: {NOTIFICATION_CHANNEL_ID}")
    except Exception as e:
        logging.error(f"Ошибка отправки сообщения в Telegram: {e}")


def generate_response(rating: int, brand_name: str, product_name: str) -> str:
    """Генерация шаблонного ответа, включая исправление склонений для рейтинга 1 звезда."""
    morph = pymorphy2.MorphAnalyzer()

    # Извлекаем первое ключевое слово из названия товара
    first_word = product_name.split()[0].lower() if product_name else "продукт"
    first_word_parsed = morph.parse(first_word)[0]

    # Попытка склонения в родительном падеже
    try:
        first_word_genitive = first_word_parsed.inflect({'gent'}).word if first_word_parsed.inflect({'gent'}) else first_word
    except:
        first_word_genitive = first_word  # Значение по умолчанию

    # Определяем род и согласуем грамматику (нашего/нашей/наших)
    try:
        gender = first_word_parsed.tag.gender
        number = first_word_parsed.tag.number  # Единственное или множественное число

        if number == 'plur':  # Множественное число
            our_word = "наших"
        elif gender == 'masc':  # Мужской род
            our_word = "нашего"
        elif gender == 'femn':  # Женский род
            our_word = "нашей"
        elif gender == 'neut':  # Средний род
            our_word = "нашего"
        else:
            our_word = "нашей"  # Значение по умолчанию
    except:
        our_word = "нашей"  # Значение по умолчанию

    # Выбираем шаблоны ответов в зависимости от рейтинга
    responses = {
        5: [
            f"Здравствуйте!Благодарим вас за позитивный отзыв! Надеемся и дальше видеть вас в числе постоянных покупателей Торговой Марки {brand_name}! Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! C уважением, команда {brand_name}.",
            f"Здравствуйте!Спасибо, что выбрали нас и оценили качество нашей продукции. Благодарим за покупку! Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! C уважением, команда {brand_name}.",
            f"Здравствуйте!Благодарим Вас за  отзыв! Мы рады, что вам все понравилось! Желаем приятных покупок. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! C уважением, команда {brand_name}.",
            f"Здравствуйте!Спасибо за ваш прекрасный отзыв! Мы очень рады, что наш товар Вам понравился и оставил такое приятное впечатление. Желаем приятных покупок! Будем рады видеть Вас в числе наших постоянных покупателей. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! C уважением, команда {brand_name}.",
            f"Здравствуйте!Благодарим за то, что нашли время, чтобы оценить наш товар и написать отзыв. Будем рады видеть Вас в числе наших постоянных покупателей. Желаем приятных покупок. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! C уважением, команда {brand_name}.",
            f"Здравствуйте! Спасибо за выбор нашего товара. Нам очень приятно, что Вы по достоинству оценили качество нашей продукции. Благодарим за покупку! Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! C уважением, команда {brand_name}."
        ],
        4: [
            f"Здравствуйте! Благодарим за Ваш отзыв! Ваше мнение действительно важно для нас и помогает в совершенствовании наших услуг. Мы работаем над тем, чтобы каждый Ваш визит был удачным. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте! Спасибо за Вашу честную обратную связь. Для нас ценно знать, что Вы оценили наш сервис. Мы стремимся не только поддерживать, но и превосходить Ваши ожидания, поэтому будем признательны за любые рекомендации, которые помогут нам улучшиться. Желаем Вам приятных и удачных покупок. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, команда {brand_name}",
        ],
        3: [
            f"Здравствуйте! Благодарим за отзыв. Извините за доставленные неудобства. Ваши замечания — ценный вклад в наше стремление к совершенству, и мы сделаем все возможное, чтобы избежать подобных ситуаций в будущем. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда{brand_name}.",
            f"Здравствуйте! Спасибо за ваше мнение. Нам жаль, что не всё прошло гладко. Ваше доверие — это наша главная ценность, и мы уже рассматриваем все возможности для улучшения на основе Ваших замечаний. Мы надеемся, что будущие взаимодействия будут удовлетворять Вас на все 100%. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, команда {brand_name}",
        ],
        2: [
            f"Здравствуйте! Благодарим за отзыв и приносим извинения за неудобства, с которыми Вы столкнулись. Ваша обратная связь позволяет нам улучшать наши услуги, и мы будем рады предоставить Вам лучший опыт в будущем. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте! Спасибо за ваш отзыв. Мы искренне извиняемся за все неудобства, которые могли возникнуть. Ваше мнение крайне важно для нас, и мы уверены, что с вашей помощью сможем выявить и устранить причины произошедшего. Ваш комфорт и удовлетворённость — наш приоритет. С уважением, команда {brand_name}",
        ],
        1: [  # Особое внимание для рейтинга 1
            f"Здравствуйте! Спасибо за Ваш отзыв. Нам жаль, что у Вас остались негативные впечатления. Мы внимательно рассмотрим Ваши комментарии, чтобы улучшить качество и предоставить Вам лучший опыт в будущем. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда.{brand_name}.",
            f"Здравствуйте! Приносим извинения за доставленные неудобства и благодарим за Ваши замечания. Мы стремимся к высочайшему уровню обслуживания и надеемся, что Вы дадите нам шанс на исправление. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением и наилучшими пожеланиями, команда {brand_name}."

        ],
    }

    # Берем случайный ответ из шаблонного списка
    return random.choice(responses.get(rating, ["Спасибо за Вашу оценку!"]))


async def get_unprocessed_reviews_count(session: aiohttp.ClientSession) -> int:
    url = f"{OZON_API_URL}/v1/review/list"
    headers = {
        "Client-Id": CLIENT_ID,
        "Api-Key": OZON_TOKEN,
        "Content-Type": "application/json",
    }
    payload = {
        "limit": 20,  # Минимально допустимое значение
        "status": "UNPROCESSED",
    }

    try:
        async with session.post(url, json=payload, headers=headers) as response:
            result = await response.json()
            logging.info(f"Полный ответ от API для подсчета отзывов: {result}")  # Логируем полный ответ
            if response.status == 200:
                total_reviews = result.get("total", 0)  # Поле для общего количества
                logging.info(f"Общее количество необработанных отзывов: {total_reviews}")
                return int(total_reviews)
            else:
                logging.error(f"Ошибка при запросе количества отзывов: {result}")
    except aiohttp.ClientError as e:
        logging.error(f"Ошибка сети при запросе количества отзывов: {e}")
    return 0


async def handle_reviews(session: aiohttp.ClientSession) -> None:
    """Основной процесс обработки отзывов."""
    logging.info("⏳ Проверка новых отзывов...")

    # Получаем общее количество необработанных отзывов
    try:
        total_unprocessed_reviews = await get_unprocessed_reviews_count(session)
    except Exception as e:
        logging.error(f"Ошибка при получении количества необработанных отзывов: {e}")
        return

    # Уведомляем в канал, если есть необработанные отзывы
    if total_unprocessed_reviews > 0:
        message = f"📋 На платформе Ozon есть <b>{total_unprocessed_reviews}</b> необработанных отзывов."
        await notify_users(message)

    # Получаем сами необработанные отзывы
    reviews = await get_unprocessed_reviews(session)
    if not reviews:
        logging.info("Нет новых отзывов для обработки.")
        return

    for review in islice(reviews, 5):  # Обрабатываем первые 5 отзывов
        review_id = review.get("id")

        # Проверяем, был ли отзыв уже обработан
        if is_review_processed(review_id):
            logging.info(f"Отзыв {review_id} уже обработан, пропускаем.")
            continue

        review_text = review.get("text", "Отзыв отсутствует")
        rating = review.get("rating", 0) or 1
        sku = review.get("sku")
        product_name = review.get("product_name")

        # Получаем бренд из API, если отсутствует информация в отзыве
        if not product_name and sku:
            product_name, brand_name = await get_product_name_and_brand_by_sku(sku, session)
        else:
            brand_name = review.get("brand", "Guten Morgen")  # Используем бренд, если он указан в отзыве.

        if not product_name:
            logging.warning(f"Товар для SKU {sku} не найден, пропускаем обработку отзыва ID {review_id}.")
            continue

        # Генерация шаблонного ответа
        response_text = generate_response(rating, brand_name, product_name)

        # Отправляем комментарий
        comment_id = await post_comment(review_id, response_text, session)
        if comment_id:
            logging.info(f"Комментарий отправлен для отзыва ID: {review_id}")
            await notify_channel(sku, response_text, rating, product_name, "Аноним", review_text)

            # Сохраняем отзыв в базу данных как обработанный
            save_review_to_db(review_id, str(sku), product_name, "Аноним", review_text, int(rating), response_text, str(comment_id))
        else:
            logging.error(f"Не удалось отправить комментарий для отзыва ID: {review_id}")


async def scheduled_task(session: aiohttp.ClientSession) -> None:
    """Планировщик обработки отзывов (обрабатывает 5 отзывов каждые 5 минут)."""
    while True:
        try:
            await handle_reviews(session)
        except Exception as e:
            logging.error(f"Ошибка при обработке отзывов: {e}")
        await asyncio.sleep(CHECK_INTERVAL)


async def main() -> None:
    init_db()
    async with aiohttp.ClientSession() as session:
        dp.include_router(router)
        asyncio.create_task(scheduled_task(session))
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


