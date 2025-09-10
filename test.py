#!/usr/bin/env python3
import asyncio
import os
import logging
import yt_dlp
import time # Required for throttling progress updates

from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import CommandStart
from aiogram.types import Message, FSInputFile
# from aiogram.utils.markdown import hbold # For formatting, if needed

# --- Configuration ---
BOT_TOKEN = "7819585434:AAEyEQATFTgM098Loh-XsvOIZY9fw_APrMc" 
DOWNLOAD_DIR = "downloads_bot" # Директория для временного хранения видео

# --- Logging ---
# Настройка логирования для вывода информации о работе бота
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# --- yt-dlp Progress Hook (Modified for Bot) ---

async def update_telegram_message(bot: Bot, chat_id: int, message_id: int, text: str, last_sent_texts: dict):
    """
    Вспомогательная функция для редактирования сообщения в Telegram.
    Избегает ошибки "Message not modified" путем проверки, изменился ли текст.
    """
    # Ключ для отслеживания последнего отправленного текста для конкретного сообщения
    message_key = f"{chat_id}_{message_id}"
    if last_sent_texts.get(message_key) != text:
        try:
            await bot.edit_message_text(text, chat_id, message_id, parse_mode=None) # parse_mode=None to avoid issues with special chars from yt-dlp
            last_sent_texts[message_key] = text
        except Exception as e:
            # Игнорируем ошибки типа "message is not modified" или другие мелкие ошибки API
            logger.debug(f"Не удалось отредактировать сообщение ({chat_id}, {message_id}): {e}")


def create_progress_hook(bot: Bot, chat_id: int, status_message_id: int, progress_queue: asyncio.Queue):
    """
    Создает функцию-хук для yt-dlp, которая будет вызываться во время загрузки.
    Эта функция помещает обновления прогресса в очередь asyncio.Queue.
    """
    def _hook(d):
        # Эта функция вызывается из yt-dlp (потенциально в другом потоке)
        logger.debug(f"yt-dlp hook called with status: {d.get('status')}")
        if d['status'] == 'downloading':
            # Извлекаем информацию о прогрессе
            info_dict = d.get('info_dict', {})
            filename = info_dict.get('title', os.path.basename(d.get('filename', 'неизвестный файл')))
            percent_str = d.get('_percent_str', '0.0%').strip()
            speed_str = d.get('_speed_str', 'N/A').strip()
            eta_str = d.get('_eta_str', 'N/A').strip()
            total_bytes = d.get('total_bytes') or info_dict.get('filesize')
            downloaded_bytes = d.get('downloaded_bytes') or info_dict.get('filesize_approx')

            percent_str_clean = ''.join(c for c in percent_str if c.isprintable())

            progress_data = {
                "status": "downloading",
                "filename": filename,
                "percent": percent_str_clean,
                "speed": speed_str,
                "eta": eta_str,
                "downloaded_bytes": downloaded_bytes,
                "total_bytes": total_bytes,
                "message_id": status_message_id
            }
            try:
                progress_queue.put_nowait(progress_data)
            except asyncio.QueueFull:
                logger.warning("Очередь прогресса заполнена. Пропускаем обновление.")

        elif d['status'] == 'finished':
            logger.info(f"yt-dlp hook: finished downloading {d.get('filename')}")
            final_filepath = d.get('info_dict', {}).get('filepath') or d.get('filename')

            progress_data = {
                "status": "finished",
                "filename": final_filepath,
                "message_id": status_message_id
            }
            try:
                progress_queue.put_nowait(progress_data)
            except asyncio.QueueFull:
                 logger.error("Очередь прогресса заполнена при попытке сообщить о завершении.")

        elif d['status'] == 'error':
            logger.error(f"yt-dlp сообщил об ошибке при загрузке: {d.get('filename')}, {d}")
            progress_data = {
                "status": "error",
                "filename": d.get('filename', 'неизвестный файл'),
                "message_id": status_message_id,
                "error_info": str(d)
            }
            try:
                progress_queue.put_nowait(progress_data)
            except asyncio.QueueFull:
                logger.error("Очередь прогресса заполнена при попытке сообщить об ошибке.")
    return _hook


# --- Download Function (Adapted for Bot and Async) ---
async def download_video_for_bot(bot: Bot, chat_id: int, video_url: str, last_sent_texts_global: dict):
    """
    Скачивает видео, отправляет обновления прогресса и итоговое видео в Telegram.
    `last_sent_texts_global` используется для предотвращения ошибок "Message not modified".
    """
    if not os.path.exists(DOWNLOAD_DIR):
        try:
            os.makedirs(DOWNLOAD_DIR)
            logger.info(f"Создана директория: {DOWNLOAD_DIR}")
        except OSError as e:
            logger.error(f"Не удалось создать директорию '{DOWNLOAD_DIR}'. {e}")
            await bot.send_message(chat_id, f"Ошибка: Не удалось создать директорию для загрузок. Обратитесь к администратору.")
            return

    status_message = await bot.send_message(chat_id, f"Обрабатываю ссылку: {video_url}\nПодготовка к загрузке...")
    progress_queue = asyncio.Queue(maxsize=30)

    # Настройки для yt-dlp
    ydl_opts = {
        'outtmpl': os.path.join(DOWNLOAD_DIR, '%(title)s.%(ext)s'), # Save with original extension first
        'format': 'best',  # MODIFIED: Try to get the best single pre-merged file
        'quiet': True,
        'noprogress': True,
        'noplaylist': True,
        'progress_hooks': [create_progress_hook(bot, chat_id, status_message.message_id, progress_queue)],
        'postprocessors': [{
            'key': 'FFmpegVideoConvertor',
            'preferedformat': 'mp4', # Convert to MP4 if necessary (requires FFmpeg)
        }],
        'logger': logger,
        'encoding': 'utf-8',
        # 'verbose': True, # Uncomment for detailed yt-dlp debugging
    }

    download_thread_task = None
    filepath_to_send = None
    download_successful = False

    try:
        def ydl_download_blocking(url, opts):
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True)
                # 'filepath' in info_dict should be the final path after postprocessing
                return info.get('filepath') or info.get('requested_downloads', [{}])[0].get('filepath')


        async def progress_updater_task_fn():
            nonlocal filepath_to_send, download_successful
            last_progress_update_time = 0
            while True:
                try:
                    item = await asyncio.wait_for(progress_queue.get(), timeout=120)
                except asyncio.TimeoutError:
                    logger.warning(f"Таймаут ожидания прогресса для {video_url}.")
                    if download_thread_task and download_thread_task.done() and not download_successful:
                        await update_telegram_message(bot, chat_id, status_message.message_id, f"⚠️ Загрузка {video_url} заняла слишком много времени или зависла.", last_sent_texts_global)
                        progress_queue.put_nowait({"status": "error", "message_id": status_message.message_id, "filename": "Таймаут"})
                    elif download_thread_task and download_thread_task.done() and download_successful:
                        pass
                    else:
                        continue # Keep waiting if download task is not done
                except Exception as e:
                    logger.error(f"Ошибка в progress_updater_task_fn при получении из очереди: {e}")
                    break

                if item.get("message_id") != status_message.message_id:
                    logger.debug(f"Получено сообщение о прогрессе для старой загрузки ({item.get('message_id')}), игнорируем.")
                    progress_queue.task_done()
                    continue

                current_time = time.time()
                if item['status'] == 'downloading':
                    if current_time - last_progress_update_time > 2:
                        filename_display = item.get('filename', 'видео')
                        if len(filename_display) > 40: filename_display = "..." + filename_display[-37:]
                        text = (f"⏬ Загрузка \"{filename_display}\": {item['percent']}\n"
                                f"Скорость: {item['speed']} | ETA: {item['eta']}")
                        await update_telegram_message(bot, chat_id, status_message.message_id, text, last_sent_texts_global)
                        last_progress_update_time = current_time
                elif item['status'] == 'finished':
                    filepath_to_send = item['filename']
                    download_successful = True
                    base_filename = os.path.basename(filepath_to_send) if filepath_to_send else "файл"
                    await update_telegram_message(bot, chat_id, status_message.message_id, f"✅ Загрузка завершена: {base_filename}\nПодготовка к отправке...", last_sent_texts_global)
                    progress_queue.task_done()
                    break
                elif item['status'] == 'error':
                    filename_display = item.get('filename', video_url)
                    error_info = item.get('error_info', 'Неизвестная ошибка yt-dlp')
                    logger.error(f"Ошибка от yt-dlp при загрузке {filename_display}: {error_info}")
                    
                    # Enhanced error message for FFmpeg issues
                    user_friendly_error = f"Ошибка загрузки {os.path.basename(filename_display)}."
                    error_text_lower = str(error_info).lower()
                    if "ffmpeg" in error_text_lower and ("not installed" in error_text_lower or "merging" in error_text_lower or "convertor" in error_text_lower):
                        user_friendly_error = (
                            f"Ошибка при обработке видео \"{os.path.basename(filename_display)}\".\n"
                            "Вероятно, для этого видео требуется FFmpeg (для слияния или конвертации), который не установлен на сервере бота.\n"
                            f"Детали: {str(error_info)[:200]}" # Show first 200 chars of original error
                        )

                    await update_telegram_message(bot, chat_id, status_message.message_id, f"⚠️ {user_friendly_error}", last_sent_texts_global)
                    download_successful = False
                    progress_queue.task_done()
                    break

                progress_queue.task_done()

        updater_async_task = asyncio.create_task(progress_updater_task_fn())

        logger.info(f"Запуск загрузки yt-dlp для {video_url} в отдельном потоке.")
        download_thread_task = asyncio.to_thread(ydl_download_blocking, video_url, ydl_opts.copy())
        returned_filepath = await download_thread_task

        if returned_filepath and not filepath_to_send and download_successful: # Should be set by hook if successful
             filepath_to_send = returned_filepath
             logger.info(f"Путь к файлу получен из результата ydl_download_blocking: {filepath_to_send}")
        elif not returned_filepath and download_successful: # Hook set filepath_to_send
            logger.info(f"Путь к файлу {filepath_to_send} установлен хуком 'finished'.")


        await updater_async_task
        logger.info(f"Задачи загрузки и обновления прогресса для {video_url} завершены. Успех: {download_successful}")

        if download_successful and filepath_to_send and os.path.exists(filepath_to_send):
            base_filename = os.path.basename(filepath_to_send)
            # Ensure the file has .mp4 extension if conversion was successful
            if not base_filename.lower().endswith('.mp4') and 'FFmpegVideoConvertor' in str(ydl_opts.get('postprocessors')):
                 logger.warning(f"Файл {base_filename} не имеет расширения .mp4 после конвертации. Проверьте логи FFmpeg.")
                 # Attempt to send anyway, or handle as an error

            await bot.send_message(chat_id, f"📤 Отправляю \"{base_filename}\" в чат...")
            try:
                file_size_bytes = os.path.getsize(filepath_to_send)
                file_size_mb = file_size_bytes / (1024 * 1024)

                if file_size_mb > 49.5:
                     await bot.send_message(chat_id, f"⚠️ Видео \"{base_filename}\" слишком большое ({file_size_mb:.2f}MB) для отправки. Макс. размер ~50MB.")
                else:
                    video_file = FSInputFile(filepath_to_send, filename=base_filename)
                    await bot.send_video(chat_id, video_file, caption=f"Скачано: {base_filename}")
                await update_telegram_message(bot, chat_id, status_message.message_id, f"✅ Видео \"{base_filename}\" отправлено!", last_sent_texts_global)
            except Exception as e:
                logger.error(f"Ошибка при отправке видео файла {filepath_to_send}: {e}")
                await bot.send_message(chat_id, f"⚠️ Не удалось отправить видео: {e}")
            finally:
                logger.info(f"Удаление временного файла: {filepath_to_send}")
                try:
                    os.remove(filepath_to_send)
                except OSError as e:
                    logger.error(f"Не удалось удалить файл {filepath_to_send}: {e}")

        elif download_successful and not filepath_to_send:
            logger.error(f"Загрузка {video_url} помечена как успешная, но путь к файлу не определен.")
            await bot.send_message(chat_id, f"⚠️ Загрузка прошла, но не удалось найти файл. Проверьте логи.")
        elif not download_successful:
            # Сообщение об ошибке уже должно было быть отправлено
            message_key = f"{chat_id}_{status_message.message_id}"
            if not last_sent_texts_global.get(message_key) or "Ошибка" not in last_sent_texts_global.get(message_key, "") :
                 await update_telegram_message(bot, chat_id, status_message.message_id, f"⚠️ Загрузка {video_url} не удалась. Подробности в логах.", last_sent_texts_global)


    except yt_dlp.utils.DownloadError as e: # This catches errors from ydl.extract_info if it fails before hooks
        logger.error(f"Ошибка yt-dlp DownloadError для {video_url} (вне хука): {e}")
        error_text_from_yt_dlp = str(e)
        user_message = f"Ошибка загрузки: {error_text_from_yt_dlp}"
        error_text_lower = error_text_from_yt_dlp.lower()

        if "ffmpeg" in error_text_lower and ("not installed" in error_text_lower or "merging" in error_text_lower or "convertor" in error_text_lower):
            user_message = (
                f"Произошла ошибка при обработке видео: {error_text_from_yt_dlp}\n\n"
                "Это часто означает, что для скачивания или конвертации этого видео необходим FFmpeg, "
                "который не установлен на сервере бота. Попробуйте другую ссылку или обратитесь к администратору."
            )
        elif "Unsupported URL" in error_text_from_yt_dlp:
             user_message = f"Ссылка не поддерживается: {video_url}"
        
        if len(user_message) > 3000: user_message = user_message[:3000] + "..."
        await update_telegram_message(bot, chat_id, status_message.message_id, f"⚠️ {user_message}", last_sent_texts_global)
    except Exception as e:
        logger.exception(f"Неожиданная ошибка при обработке {video_url}: {e}")
        await update_telegram_message(bot, chat_id, status_message.message_id, f"⚠️ Произошла неожиданная ошибка. Попробуйте позже.", last_sent_texts_global)
    finally:
        if 'updater_async_task' in locals() and updater_async_task and not updater_async_task.done():
            updater_async_task.cancel()
            try:
                await updater_async_task
            except asyncio.CancelledError:
                logger.info("Задача обновления прогресса была отменена.")
        if not download_successful and filepath_to_send and os.path.exists(filepath_to_send):
            logger.info(f"Удаление файла после неудачной загрузки: {filepath_to_send}")
            try:
                os.remove(filepath_to_send)
            except OSError as e:
                 logger.error(f"Не удалось удалить файл {filepath_to_send} после ошибки: {e}")
        elif not download_successful:
            logger.warning(f"Загрузка {video_url} не удалась. Частичные файлы в {DOWNLOAD_DIR} могут требовать ручной очистки.")


# --- Aiogram Handlers ---
router = Router()
LAST_SENT_TEXTS_GLOBAL = {}


@router.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer(
        "Привет! 👋 Отправь мне ссылку на видео с VK, RuTube, YouTube и других сайтов,\n"
        "и я попробую его скачать и отправить тебе.\n\n"
        "Просто вставь ссылку в чат!"
    )

@router.message(F.text)
async def handle_url_message(message: Message, bot: Bot):
    raw_text = message.text.strip()
    potential_urls = [word for word in raw_text.split() if word.startswith("http://") or word.startswith("https://")]

    if not potential_urls:
        if raw_text.lower().startswith("/download"):
             await message.reply("Пожалуйста, укажите ссылку после команды /download.\nНапример: /download https://vk.com/video-xxxx_yyyy")
        return

    video_url = potential_urls[0]
    logger.info(f"Получен URL от чата {message.chat.id}: {video_url}")

    try:
        await download_video_for_bot(bot, message.chat.id, video_url, LAST_SENT_TEXTS_GLOBAL)
    except Exception as e:
        logger.error(f"Критическая ошибка в handle_url_message для {video_url}: {e}", exc_info=True)
        await message.answer("Произошла критическая ошибка при попытке начать загрузку. Пожалуйста, сообщите администратору.")


# --- Main Bot Function ---
async def main():
    if not os.path.exists(DOWNLOAD_DIR):
        try:
            os.makedirs(DOWNLOAD_DIR)
            logger.info(f"Создана директория для загрузок: {DOWNLOAD_DIR}")
        except OSError as e:
            logger.critical(f"Не удалось создать директорию для загрузок '{DOWNLOAD_DIR}': {e}. Бот не может запуститься.")
            return

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)

    logger.info("Запуск бота...")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        logger.info("Бот остановлен.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен пользователем (KeyboardInterrupt/SystemExit).")
    except Exception as e:
        logger.critical(f"Не удалось запустить бота: {e}", exc_info=True)

