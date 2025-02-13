import asyncio
import logging
import random
import aiohttp
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command
from aiogram.types import Message
import os
from dotenv import load_dotenv
import sqlite3
import pymorphy2
from itertools import islice

# Загрузка переменных окружения из файла .env
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Настройка переменных
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
NOTIFICATION_CHANNEL_ID = os.getenv("NOTIFICATION_CHANNEL_ID")
OZON_API_URL = "https://api-seller.ozon.ru"
OZON_TOKEN = os.getenv("OZON_TOKEN")
CLIENT_ID = os.getenv("CLIENT_ID")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", 300))

# Проверяем перед запуском, что все токены заданы
if not all([TELEGRAM_BOT_TOKEN, NOTIFICATION_CHANNEL_ID, OZON_TOKEN, CLIENT_ID]):
    raise ValueError("Пожалуйста, задайте TELEGRAM_BOT_TOKEN, NOTIFICATION_CHANNEL_ID, OZON_TOKEN и CLIENT_ID!")

# Инициализация бота и диспетчера
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
router = Router()  # Создаем маршрутизатор


def init_db():
    """Инициализация базы данных и создание таблиц."""
    if os.path.exists("ozon_reviews.db"):
        os.remove("ozon_reviews.db")

    conn = sqlite3.connect("ozon_reviews.db")
    cursor = conn.cursor()
    # Создаем таблицу для хранения обработанных отзывов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_reviews (
            review_id TEXT PRIMARY KEY,  -- Используем TEXT для хранения UUID
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
    conn.commit()
    conn.close()


def save_review_to_db(review_id, sku, product_name, user_name, review_text, rating, response_text, comment_id):
    """Сохранить обработанный отзыв в базу данных."""
    conn = sqlite3.connect("ozon_reviews.db")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO processed_reviews (review_id, sku, product_name, user_name, review_text, rating, response_text, comment_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (review_id, sku, product_name, user_name, review_text, rating, response_text, comment_id))
    conn.commit()
    conn.close()


def is_review_processed(review_id):
    """Проверить, обработан ли отзыв ранее."""
    conn = sqlite3.connect("ozon_reviews.db")
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM processed_reviews WHERE review_id = ?", (review_id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None


async def get_unprocessed_reviews():
    """Получение списка необработанных отзывов через Ozon Seller API."""
    url = f"{OZON_API_URL}/v1/review/list"
    headers = {
        "Client-Id": CLIENT_ID,
        "Api-Key": OZON_TOKEN,
        "Content-Type": "application/json",
    }
    payload = {"limit": 50, "sort_dir": "ASC", "status": "UNPROCESSED"}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                logging.info(f"Статус ответа: {response.status}")
                result = await response.json()
                if response.status == 200:
                    return result.get("reviews", [])
                else:
                    logging.error(f"Ошибка при запросе отзывов: {result}")
                    return []
        except aiohttp.ClientError as e:
            logging.error(f"Ошибка сети при запросе API: {e}")
            return []


async def get_product_name_by_sku(sku):
    """Получение информации о товаре через SKU с использованием Ozon API."""
    if not sku:
        logging.warning("SKU отсутствует, запрос пропущен.")
        return None

    url = f"{OZON_API_URL}/v2/product/info"
    headers = {
        "Client-Id": CLIENT_ID,
        "Api-Key": OZON_TOKEN,
        "Content-Type": "application/json",
    }
    payload = {"sku": sku}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                result = await response.json()
                if response.status == 200 and "result" in result:
                    return result["result"].get("name")
                else:
                    logging.error(f"Ошибка при получении информации о товаре {sku}: {result}")
        except aiohttp.ClientError as e:
            logging.error(f"Ошибка сети при запросе информации о товаре {sku}: {e}")
    return None


async def post_comment(review_id, text):
    """Отправить комментарий на отзыв через Ozon API."""
    url = f"{OZON_API_URL}/v1/review/comment/create"
    headers = {
        "Client-Id": CLIENT_ID,
        "Api-Key": OZON_TOKEN,
        "Content-Type": "application/json",
    }
    payload = {"mark_review_as_processed": True, "review_id": review_id, "text": text}
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                result = await response.json()
                if response.status == 200:
                    return result.get("comment_id")
                else:
                    logging.error(f"Ошибка при отправке комментария: {result}")
        except aiohttp.ClientError as e:
            logging.error(f"Ошибка сети при отправке комментария: {e}")
    return None



async def notify_channel(sku, response_text, rating, product_name, user_name, review_text):
    """Отправить уведомление в канал о новом комментарии."""
    user_name = user_name if user_name else "Аноним"
    review_message = (
        f"ООО Гутен Морген\n"
        f"⭐️{'⭐️' * (rating - 1)}\n"
        f"Артикул Ozon: {sku} (https://www.ozon.ru/product/polotentse-mahrovoe-guten-morgen-1-sht-50h90-visdom-hlopok-100-450-g-m2-{sku}/?at=PjtJn4mrrcpDJKlxi71M2m3Ux8Y9MYc7Kkov3cK66R4g&avtc=1&avte=4&avts=1739353793&keywords=%D0%9F%D0%9C%D0%94%D0%92%D0%B8%D1%81%D0%B4-30-50)\n"
        f"Товар: {product_name}\n\n"
        f"💬 {user_name}\n{review_text}\n\n"
        f"✅ Отправлен ответ:\n{response_text}"
    )
    try:
        await bot.send_message(NOTIFICATION_CHANNEL_ID, review_message, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Ошибка отправки уведомления в канал: {e}")


def generate_response(rating, brand_name, product_name):
    """Генерация шаблонного ответа."""
    morph = pymorphy2.MorphAnalyzer()

    first_word = product_name.split()[0] if product_name else "продукт"
    first_word_parsed = morph.parse(first_word)[0]
    first_word_genitive = first_word_parsed.inflect({'gent'}).word if first_word_parsed.inflect({'gent'}) else "продукции"

    our_word_parsed = morph.parse("нашей")[0]
    gender = first_word_parsed.tag.gender if first_word_parsed.tag.gender else 'neut'
    our_word_genitive = our_word_parsed.inflect({gender}).word if our_word_parsed.inflect({gender}) else "нашей"

    # Ответы в зависимости от оценки
    responses = {
        5: [
            f"Здравствуйте!Спасибо за Вашу высокую оценку {our_word_genitive} {first_word_genitive}! Мы рады, что Вам понравился наш товар. Будем рады видеть Вас снова в нашем магазине. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте!Благодарим Вас за отличную оценку {our_word_genitive} {first_word_genitive}! Надеемся, что товар полностью оправдал Ваши ожидания. Будем рады видеть Вас снова в нашем магазине. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте!Спасибо за Вашу высокую оценку {our_word_genitive} {first_word_genitive}! Мы рады, что Вам понравился наш товар. Будем рады видеть Вас снова в нашем магазине. Добавляйте бренд {brand_name} в избранное, чтобы всегда быть в курсе акций и новинок! С уважением, Команда {brand_name}."
        ],
        4: [
            f"Здравствуйте!Спасибо за Вашу оценку {our_word_genitive} {first_word_genitive}! Мы рады, что товар Вам подошел. Будем рады видеть Вас снова в нашем магазине. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте!Благодарим Вас за хорошую оценку {our_word_genitive} {first_word_genitive}! Мы будем рады помочь Вам с выбором в будущем. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте!Спасибо за оценку {our_word_genitive} {first_word_genitive}! Если будут предложения, будем рады их услышать. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}."
        ],
        3: [
            f"Здравствуйте!Спасибо за Вашу оценку {our_word_genitive} {first_word_genitive}! Нам важно Ваше мнение, и мы работаем над улучшениями. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте!Благодарим Вас за оценку {our_word_genitive} {first_word_genitive}! Мы постараемся улучшить качество товара в будущем. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте!Спасибо за Вашу оценку {our_word_genitive} {first_word_genitive}! Мы ценим Вашу обратную связь и будем учитывать Ваши пожелания. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}."
        ],
        2: [
            f"Здравствуйте!Благодарим Вас за оценку {our_word_genitive} {first_word_genitive}! Нам жаль, что товар Вам не подошел, мы примем меры. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте!Спасибо за оценку {our_word_genitive} {first_word_genitive}. Мы постараемся улучшить качество нашего товара. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте!Простите за неудобства с {our_word_genitive} {first_word_genitive}! Мы работаем над улучшением качества товара. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}."
        ],
        1: [
            f"Здравствуйте!Очень жаль, что Вам не понравилась {our_word_genitive} {first_word_genitive}. Мы примем все меры для улучшения. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте!Извините за неприятный опыт с {our_word_genitive} {first_word_genitive}. Мы обязательно учтем Ваши замечания. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}.",
            f"Здравствуйте!Приносим извинения за негативный опыт с {our_word_genitive} {first_word_genitive}. Мы будем работать над улучшением качества товара. Добавляйте бренд {brand_name} в список любимых, чтобы быть в курсе акций и новинок! С уважением, Команда {brand_name}."
        ]
    }
    return random.choice(responses.get(rating, ["Спасибо за Вашу оценку!"]))

async def handle_reviews():
    """Основной процесс обработки отзывов."""
    logging.info("⏳ Проверка новых отзывов...")
    reviews = await get_unprocessed_reviews()
    if not reviews:
        logging.info("Нет новых необработанных отзывов.")
        return

    for review in islice(reviews, 5):  # обработка первых 5 отзывов
        review_id = review.get("id")
        review_text = review.get("text", "Отзыв отсутствует")
        rating = review.get("rating", 0) or 1
        product_name = review.get("product_name")
        sku = review.get("sku")

        if is_review_processed(review_id):
            logging.info(f"Отзыв ID: {review_id} уже обработан, пропускаем.")
            continue

        if not product_name and sku:
            product_name = await get_product_name_by_sku(sku)

        if not product_name:
            logging.warning(f"Товар для SKU {sku} не найден, пропускаем обработку отзыва ID {review_id}.")
            continue

        brand_name = "Гутен Морген"
        response_text = generate_response(rating, brand_name, product_name)

        comment_id = await post_comment(review_id, response_text)
        if comment_id:
            logging.info(f"Комментарий отправлен для отзыва ID: {review_id}")
            await notify_channel(sku, response_text, rating, product_name, 'Аноним', review_text)
            save_review_to_db(review_id, str(sku), product_name, 'Аноним', review_text, int(rating), response_text, str(comment_id))
        else:
            logging.error(f"Не удалось отправить комментарий для отзыва ID: {review_id}")





@router.message(Command(commands=["start", "help"]))
async def send_welcome(message: Message):
    """Обработчик команды /start и /help."""
    await message.answer("Привет! Этот бот автоматически отвечает на отзывы Ozon.")


async def scheduled_task():
    """Планировщик обработки отзывов (обрабатывает 5 отзывов каждые 5 минут)."""
    while True:
        try:
            await handle_reviews()
        except Exception as e:
            logging.error(f"Ошибка при обработке отзывов: {e}")
        await asyncio.sleep(CHECK_INTERVAL)


async def main():
    init_db()
    dp.include_router(router)
    asyncio.create_task(scheduled_task())
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
