import asyncio
import json
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from os import environ
from agrigation import Agrigation
import tests
import unittest
import logging
from dotenv import load_dotenv

# Загрузка переменных окружения из файла .env
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Запуск тестов
unittest.TextTestRunner(verbosity=2).run(
    unittest.TestLoader().loadTestsFromModule(tests))

# Настройка бота
bot = Bot(token=environ.get('TELEGRAM_ACCESS_TOKEN'))
dp = Dispatcher()

logger.info('Telegram-бот запущен!')


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(f"Привет, {message.from_user.full_name}!")


@dp.message()
async def get_dataset(message: types.Message):
    try:
        request = json.loads(message.text)
        required_keys = ['dt_from', 'dt_upto', 'group_type']

        # Проверка наличия всех необходимых ключей
        if all(key in request for key in required_keys):
            if all(request[key] for key in required_keys):
                result = await asyncio.to_thread(Agrigation(request).dataset)
                await message.answer(result)
            else:
                raise ValueError('Предоставлены пустые значения')
        else:
            raise ValueError('Отсутствуют необходимые ключи')

    except json.JSONDecodeError:
        await message.answer(
            'Неверный формат JSON. Пример запроса: {"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}')
    except ValueError as e:
        await message.answer(f'Неверный запрос: {str(e)}')
    except Exception as e:
        logger.error(f'Неожиданная ошибка: {str(e)}')
        await message.answer('Произошла неожиданная ошибка. Пожалуйста, попробуйте позже.')


if __name__ == "__main__":
    asyncio.run(dp.start_polling(bot))
