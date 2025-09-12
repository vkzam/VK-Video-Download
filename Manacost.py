import telebot
import vk_api
import time
import json
import re
import requests
import logging
import os
import threading
import shutil
import yt_dlp
import io
import hashlib 

from logging.handlers import RotatingFileHandler, MemoryHandler
from bs4 import BeautifulSoup
from telebot import types
from telebot.apihelper import ApiTelegramException
from urllib.parse import urlparse, urljoin # Добавлен urljoin

try:
    import config as config
except ImportError:
    print("Ошибка: Файл config.py не найден. Пожалуйста, создайте его и заполните.")
    exit()

# --- Настройка логирования ---
log_file_path = getattr(config, 'LOG_FILE', 'bot.log')
max_log_size_bytes = getattr(config, 'MAX_LOG_SIZE_MB', 10) * 1024 * 1024
backup_count = getattr(config, 'LOG_BACKUP_COUNT', 5)

log_formatter_debug = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
log_formatter_info = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

rotating_handler = RotatingFileHandler(
    log_file_path, maxBytes=max_log_size_bytes, backupCount=backup_count, encoding='utf-8', delay=True
)
rotating_handler.setFormatter(log_formatter_info) 
rotating_handler.setLevel(logging.DEBUG) 

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter_info)
stream_handler.setLevel(logging.INFO)

memory_handler = MemoryHandler(capacity=200, flushLevel=logging.CRITICAL, target=None, flushOnClose=False)
memory_handler.setFormatter(log_formatter_info)
memory_handler.setLevel(logging.ERROR)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
if logger.hasHandlers(): logger.handlers.clear()
logger.addHandler(rotating_handler)
logger.addHandler(stream_handler)
logger.addHandler(memory_handler)

logging.getLogger("telebot").setLevel(logging.WARNING)
logging.getLogger("vk_api").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("yt_dlp").setLevel(logging.WARNING)

# --- Глобальные переменные и константы ---
DOWNLOAD_DIR = 'vk_videos'
PHOTO_DOWNLOAD_DIR = 'vk_photos_temp' 
CAPTION_LIMIT = 1024
TELEGRAM_PHOTO_SIZE_LIMIT_MB = 10

# --- Инициализация VK и Telegram ---
try:
    bot = telebot.TeleBot(config.TELEGRAM_BOT_TOKEN, parse_mode='Markdown')
except Exception as e:
    logger.critical(f"Критическая ошибка при инициализации Telegram бота: {e}")
    try:
        admin_id = getattr(config, 'ADMIN_CHAT_ID', None)
        if admin_id: bot.send_message(admin_id, f"🚨 КРИТИЧЕСКАЯ ОШИБКА при инициализации Telegram бота: {e}", parse_mode=None)
    except Exception: pass
    exit()

try:
    vk_session = vk_api.VkApi(token=config.VK_SERVICE_TOKEN)
    vk = vk_session.get_api()
except vk_api.AuthError as e:
    logger.critical(f"Ошибка аутентификации VK: {e}")
    try:
        admin_id = getattr(config, 'ADMIN_CHAT_ID', None)
        if admin_id: bot.send_message(admin_id, f"🚨 КРИТИЧЕСКАЯ ОШИБКА аутентификации VK: {e}. Проверьте VK_SERVICE_TOKEN.", parse_mode=None)
    except Exception: pass
    exit()
except Exception as e:
    logger.critical(f"Критическая ошибка при инициализации VK API: {e}")
    try:
        admin_id = getattr(config, 'ADMIN_CHAT_ID', None)
        if admin_id: bot.send_message(admin_id, f"🚨 КРИТИЧЕСКАЯ ОШИБКА при инициализации VK API: {e}", parse_mode=None)
    except Exception: pass
    exit()

# --- Фильтры и состояние постов ---
filter_words = []
filter_words_file_path = getattr(config, 'FILTER_WORDS_FILE', 'filter_words.json')

def load_filter_words():
    global filter_words
    try:
        if os.path.exists(filter_words_file_path):
            with open(filter_words_file_path, 'r', encoding='utf-8') as f: filter_words = json.load(f)
            logger.info(f"Слова-фильтры загружены: {filter_words}")
        else: filter_words = []; logger.info("Файл фильтров не найден.")
    except Exception as e: logger.error(f"Не удалось загрузить слова-фильтры: {e}"); filter_words = []

def save_filter_words():
    global filter_words
    try:
        with open(filter_words_file_path, 'w', encoding='utf-8') as f: json.dump(filter_words, f, ensure_ascii=False, indent=4)
        logger.info(f"Слова-фильтры сохранены: {filter_words}")
    except Exception as e: logger.error(f"Не удалось сохранить слова-фильтры: {e}")

posts_state = {}
post_state_prefix = getattr(config, 'POST_STATE_FILE_PREFIX', 'posts_state')

def load_posts_state(group_key):
    file_path = f"{post_state_prefix}_{group_key}.json"
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f: return json.load(f)
        else: return {}
    except Exception as e: logger.error(f"Не удалось загрузить состояние постов для {group_key}: {e}"); return {}

def save_posts_state(group_key, state_dict):
    file_path = f"{post_state_prefix}_{group_key}.json"
    try:
        with open(file_path, 'w', encoding='utf-8') as f: json.dump(state_dict, f, ensure_ascii=False, indent=4)
        logger.debug(f"Состояние постов для {group_key} сохранено.")
    except Exception as e: logger.error(f"Не удалось сохранить состояние постов для {group_key}: {e}")

# --- Функции отправки сообщений админу и обработки URL ---
def send_error_to_admin(error_message, is_critical=False):
    admin_chat_id = getattr(config, 'ADMIN_CHAT_ID', None)
    if not admin_chat_id:
        logger.critical(f"[IMMEDIATE SEND FAILED - NO ADMIN_CHAT_ID] {error_message}")
        return
    try:
        error_text = str(error_message)
        prefix = "🚨 КРИТИЧЕСКАЯ ОШИБКА: " if is_critical else "⚠️ Ошибка бота: "
        full_message = f"{prefix}\n\n{error_text}"
        if len(full_message) > 4000: full_message = full_message[:4000] + "..."
        logger.error(f"Отправка СРОЧНОГО сообщения админу ({admin_chat_id}): {error_text[:500]}...")
        bot.send_message(admin_chat_id, full_message, parse_mode=None)
    except Exception as e:
        logger.critical(f"Не удалось отправить СРОЧНОЕ сообщение админу ({admin_chat_id}): {e}. Исходное: {error_message[:200]}...")

def send_error_summary_to_admin(error_records):
    admin_chat_id = getattr(config, 'ADMIN_CHAT_ID', None)
    if not admin_chat_id:
        logger.warning("ADMIN_CHAT_ID не настроен. Сводка ошибок не будет отправлена.")
        return
    if not error_records:
        logger.info("Нет ошибок для отправки в сводке.")
        return

    logger.info(f"Подготовка сводки из {len(error_records)} ошибок для админа {admin_chat_id}...")
    summary_header = f"⚠️ Обнаружены ошибки за цикл проверки ({len(error_records)} шт.):\n{'-'*20}\n"
    error_lines = [log_formatter_info.format(record) for record in error_records]
    full_summary_text = summary_header + "\n".join(error_lines)

    max_len = 4096
    if len(full_summary_text) > max_len:
        logger.warning(f"Сводка ошибок слишком длинная ({len(full_summary_text)}), будет обрезана.")
        available_len = max_len - len(summary_header) - 50
        if available_len < 100: available_len = 100
        truncated_errors = "\n".join(error_lines)[:available_len]
        last_newline = truncated_errors.rfind('\n')
        if last_newline > 0: truncated_errors = truncated_errors[:last_newline]
        full_summary_text = summary_header + truncated_errors + "\n\n[...и другие ошибки...]"

    try:
        logger.info(f"Отправка сводки ошибок админу ({admin_chat_id})...")
        bot.send_message(admin_chat_id, full_summary_text, parse_mode=None)
        logger.info("Сводка ошибок успешно отправлена.")
    except Exception as e:
        logger.error(f"Не удалось отправить сводку ошибок админу ({admin_chat_id}): {e}")
        try: bot.send_message(admin_chat_id, f"⚠️ Не удалось отправить сводку ошибок ({len(error_records)} шт.). Ошибка: {e}", parse_mode=None)
        except Exception: logger.critical(f"Не удалось отправить уведомление об ошибке отправки сводки админу {admin_chat_id}.")

def get_unshortened_url(url, max_hops=7, timeout=10):
    """
    Итеративно разворачивает URL, следуя HTTP-редиректам и некоторым HTML-редиректам.
    """
    current_url = url.strip().strip("'\"")
    visited_urls = {current_url} 
    headers = {'User-Agent': 'Mozilla/5.0'}
    logger.info(f"Начало разворачивания URL: {current_url}")

    for hop_count in range(max_hops):
        logger.debug(f"Попытка {hop_count + 1}/{max_hops}: Запрос к {current_url}")
        try:
            response = requests.get(current_url, timeout=timeout, allow_redirects=False, headers=headers)
            time.sleep(0.3) 
            response.raise_for_status()

            if response.status_code in (301, 302, 303, 307, 308) and 'Location' in response.headers:
                next_url = response.headers['Location'].strip().strip("'\"")
                if not urlparse(next_url).scheme: 
                    next_url = urljoin(current_url, next_url)
                
                logger.debug(f"Обнаружен HTTP редирект: {current_url} -> {next_url}")
                if next_url in visited_urls:
                    logger.warning(f"Обнаружен цикл редиректа на {next_url}. Прерывание.")
                    return current_url 
                current_url = next_url
                visited_urls.add(current_url)
                continue 

            if response.status_code == 200:
                final_url_from_request = response.url.strip().strip("'\"")
                soup = BeautifulSoup(response.text, 'lxml')
                
                meta_refresh = soup.find('meta', attrs={'http-equiv': re.compile(r'refresh', re.IGNORECASE)})
                if meta_refresh and meta_refresh.get('content'):
                    content_value = meta_refresh['content']
                    match = re.search(r'url\s*=\s*([\'"]?)(.*?)\1(?:;|$)', content_value, re.IGNORECASE)
                    if match:
                        next_url = match.group(2).strip().strip("'\"")
                        if not urlparse(next_url).scheme:
                            next_url = urljoin(final_url_from_request, next_url)
                        
                        logger.debug(f"Обнаружен Meta refresh: {final_url_from_request} -> {next_url}")
                        if next_url in visited_urls:
                            logger.warning(f"Обнаружен цикл редиректа (meta) на {next_url}. Прерывание.")
                            return final_url_from_request 
                        current_url = next_url
                        visited_urls.add(current_url)
                        continue 

                input_tag = soup.find('input', {'type': 'hidden', 'id': 'redirect_url', 'name': 'to'})
                if input_tag and input_tag.get('value'):
                    next_url = input_tag.get('value').strip().strip("'\"")
                    if not urlparse(next_url).scheme:
                        next_url = urljoin(final_url_from_request, next_url)

                    logger.debug(f"Обнаружен URL в input-теге: {final_url_from_request} -> {next_url}")
                    if next_url in visited_urls:
                        logger.warning(f"Обнаружен цикл редиректа (input) на {next_url}. Прерывание.")
                        return final_url_from_request
                    current_url = next_url
                    visited_urls.add(current_url)
                    continue 
                
                logger.info(f"Конечный URL после {hop_count + 1} попыток: {final_url_from_request}")
                return final_url_from_request

            logger.warning(f"Неожиданный статус-код {response.status_code} для {current_url} на попытке {hop_count + 1}.")
            return current_url 

        except requests.exceptions.Timeout:
            logger.error(f"Таймаут при запросе к {current_url} на попытке {hop_count + 1}.")
            return current_url 
        except requests.exceptions.RequestException as e:
            logger.error(f"Сетевая ошибка при запросе к {current_url} на попытке {hop_count + 1}: {e}")
            return current_url
        except Exception as e:
            logger.error(f"Неизвестная ошибка при обработке {current_url} на попытке {hop_count + 1}: {e}", exc_info=True)
            return current_url

    logger.warning(f"Превышено максимальное количество переходов ({max_hops}) для исходного URL: {url}. Возвращается последний известный URL: {current_url}")
    return current_url

def prepare_text(text):
    """Подготавливает текст поста для отправки в Telegram."""
    if not text: return ""
    processed_text = text
    logger.debug(f"Исходный текст для prepare_text: {text[:100]}...")

    # Шаг 1: Разворачивание vk.cc ссылок
    try:
        # Ищем все http/https ссылки, чтобы потом проверить, не vk.cc ли они
        # Используем более общее регулярное выражение для поиска URL-ов
        potential_urls = re.findall(r'https?://[^\s<>"\'`\]\[()*]+', processed_text)
        processed_vk_cc_links = set()

        for p_url in potential_urls:
            if 'vk.cc/' in p_url and p_url not in processed_vk_cc_links:
                # Извлекаем именно vk.cc часть, если она часть большего URL (маловероятно, но для безопасности)
                vk_cc_match = re.search(r'(https?://vk\.cc/[a-zA-Z0-9]+)', p_url)
                if vk_cc_match:
                    actual_vk_cc_url = vk_cc_match.group(1)
                    if actual_vk_cc_url in processed_vk_cc_links:
                        continue

                    logger.debug(f"Найдена vk.cc ссылка для обработки: {actual_vk_cc_url}")
                    full_url = get_unshortened_url(actual_vk_cc_url) # Используем улучшенную функцию
                    if full_url != actual_vk_cc_url:
                        # Экранируем обратные слеши в full_url перед использованием в re.sub, если они там есть
                        replacement_url = full_url.replace('\\', '\\\\')
                        processed_text = re.sub(re.escape(actual_vk_cc_url), replacement_url, processed_text)
                        logger.info(f"Замена vk.cc в тексте: {actual_vk_cc_url} -> {full_url}")
                    else:
                        logger.debug(f"Ссылка vk.cc не изменилась или не удалось развернуть: {actual_vk_cc_url}")
                    processed_vk_cc_links.add(actual_vk_cc_url)
    except Exception as e: 
        logger.error(f"Ошибка замены vk.cc в тексте: {e}", exc_info=True)


    # Шаг 2: Обработка VK-специфичных ссылок [url|text] и [id|text]
    try:
        def escape_md_brackets(match):
            link_text_raw = match.group(2)
            url_raw = match.group(1)
            link_text_escaped = link_text_raw.replace('\\', '\\\\').replace('[', '\\[').replace(']', '\\]')
            url_escaped_for_md_syntax = url_raw.replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')
            logger.debug(f"Замена [url|text]: [{url_raw}|{link_text_raw}] -> [{link_text_escaped}]({url_escaped_for_md_syntax})")
            return f'[{link_text_escaped}]({url_escaped_for_md_syntax})'
        processed_text = re.sub(r'\[(https?://[^\|\]]+)\|([^\]]+)\]', escape_md_brackets, processed_text)

        def escape_mention(match):
            mention_text_raw = match.group(1)
            mention_text_escaped = mention_text_raw.replace('\\', '\\\\').replace('[', '\\[').replace(']', '\\]')
            mention_text_escaped = mention_text_escaped.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
            logger.debug(f"Замена [id|text]: [{match.group(0)}] -> {mention_text_escaped}")
            return mention_text_escaped
        processed_text = re.sub(r'\[(?:id|club)\d+\|([^\]]+)\]', escape_mention, processed_text)
    except Exception as e: logger.error(f"Ошибка замены [url|text] или [id|text]: {e}")

    # Шаг 3: Экранирование остальных символов Markdown (*, _, `) и начальных точек, ИЗБЕГАЯ URL
    try:
        # Обновленный паттерн для URL, чтобы он был более "жадным" и корректно обрабатывал скобки в URL, если они не являются частью Markdown-разметки ссылки
        url_pattern_for_escape = re.compile(
            r'(\bhttps?://(?:www\.)?[a-zA-Z0-9@:%._\+~#=/-]{2,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&/=]*))' # Более общий URL pattern
            r'|(\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b)' # Email pattern
            r'|(\[[^\]]+\]\([^)]+\))' # Markdown links [text](url) - их не трогаем
        )
        
        parts = []
        last_end = 0
        for match in url_pattern_for_escape.finditer(processed_text):
            start, end = match.span()
            
            pre_text = processed_text[last_end:start]
            # Экранируем только *, _, ` вне URL и не в Markdown ссылках
            pre_text_escaped = re.sub(r'(?<![\\`*_])([*_`])(?![`*_])', r'\\\1', pre_text)
            pre_text_escaped = re.sub(r'^\.', r'\\.', pre_text_escaped, flags=re.MULTILINE)
            parts.append(pre_text_escaped)
            
            # URL, email или Markdown-ссылку добавляем как есть
            parts.append(match.group(0))
            
            last_end = end
        
        post_text = processed_text[last_end:]
        post_text_escaped = re.sub(r'(?<![\\`*_])([*_`])(?![`*_])', r'\\\1', post_text)
        post_text_escaped = re.sub(r'^\.', r'\\.', post_text_escaped, flags=re.MULTILINE)
        parts.append(post_text_escaped)
        
        processed_text = "".join(parts)
        logger.debug("Markdown символы (*, _, `) и начальные точки экранированы с учетом URL и Markdown-ссылок.")

    except Exception as e:
        logger.error(f"Ошибка экранирования Markdown с учетом URL: {e}", exc_info=True)

    logger.debug(f"Результат prepare_text: {processed_text[:100]}...")
    return processed_text.strip()


# --- Функции очистки папок скачивания ---
def clear_download_folder(folder_path):
    if not folder_path: return
    logger.info(f"Очистка папки: {folder_path}")
    if not os.path.exists(folder_path): logger.info(f"Папка {folder_path} не существует."); return
    if not os.path.isdir(folder_path): logger.error(f"Путь {folder_path} не папка."); return
    try:
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path): os.unlink(file_path); logger.debug(f"Удален файл: {file_path}")
                elif os.path.isdir(file_path): shutil.rmtree(file_path); logger.debug(f"Удалена папка: {file_path}")
            except Exception as e: logger.error(f"Не удалось удалить {file_path}: {e}")
        logger.info(f"Папка {folder_path} очищена.")
    except OSError as e: logger.error(f"Ошибка доступа/очистки папки {folder_path}: {e}")

# --- Функция скачивания фото ---
def download_photo_to_file(photo_url, output_dir=PHOTO_DOWNLOAD_DIR, max_size_mb=TELEGRAM_PHOTO_SIZE_LIMIT_MB):
    logger.debug(f"Попытка скачивания фото в файл: {photo_url} -> {output_dir}")
    if not os.path.exists(output_dir):
        try: os.makedirs(output_dir); logger.info(f"Создана папка для временных фото: {output_dir}")
        except OSError as e: logger.exception(f"Не удалось создать папку '{output_dir}': {e}"); return None

    try:
        response = requests.get(photo_url, stream=True, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()

        content_length = response.headers.get('content-length')
        max_bytes = max_size_mb * 1024 * 1024
        if content_length:
            try:
                file_size = int(content_length)
                file_size_mb = file_size / (1024 * 1024)
                logger.debug(f"Размер фото (Content-Length): {file_size_mb:.2f} MB")
                if file_size_mb > max_size_mb:
                    logger.warning(f"Фото {photo_url} ({file_size_mb:.2f} MB) превышает лимит {max_size_mb} MB. Скачивание отменено.")
                    return None
            except ValueError: logger.warning(f"Не удалось распознать Content-Length: {content_length}")

        try:
            parsed_url = urlparse(photo_url)
            base_name = os.path.basename(parsed_url.path)
            name_part, ext_part = os.path.splitext(base_name)
            if not ext_part: 
                 content_type = response.headers.get('content-type')
                 if content_type and 'image/' in content_type:
                      ext_part = '.' + content_type.split('image/')[-1].split(';')[0] 
                 else: ext_part = '.jpg' 
            url_hash = hashlib.md5(photo_url.encode()).hexdigest()[:8]
            safe_name_part = re.sub(r'[^\w\-]+', '_', name_part)[:50] 
            filename = f"{url_hash}_{safe_name_part}{ext_part}"
        except Exception as e_fname:
            logger.warning(f"Ошибка генерации имени файла для {photo_url}: {e_fname}. Используем только хэш.")
            filename = f"{hashlib.md5(photo_url.encode()).hexdigest()}.jpg"

        file_path = os.path.join(output_dir, filename)
        logger.debug(f"Фото будет сохранено как: {file_path}")

        bytes_downloaded = 0
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                bytes_downloaded += len(chunk)
                if bytes_downloaded > max_bytes:
                     logger.warning(f"Фото {photo_url} превышает лимит {max_size_mb} MB во время скачивания в файл. Скачивание прервано.")
                     f.close() 
                     try: os.remove(file_path); logger.info(f"Удален частично скачанный файл: {file_path}") 
                     except OSError as del_err: logger.error(f"Не удалось удалить частично скачанный файл {file_path}: {del_err}")
                     return None
                f.write(chunk)

        final_size_mb = bytes_downloaded / (1024 * 1024)
        logger.info(f"Фото успешно скачано и сохранено: {file_path} ({final_size_mb:.2f} MB)")
        return file_path

    except requests.exceptions.Timeout: logger.error(f"Таймаут при скачивании фото {photo_url}"); return None
    except requests.exceptions.RequestException as e: logger.error(f"Ошибка сети при скачивании фото {photo_url}: {e}"); return None
    except Exception as e: logger.exception(f"Неизвестная ошибка при скачивании фото {photo_url} в файл: {e}"); return None

# --- Функция скачивания видео ---
def download_vk_video(video_url, output_dir=DOWNLOAD_DIR):
    logger.info(f"Скачивание видео: {video_url} -> {output_dir}")
    if not os.path.exists(output_dir):
        try: os.makedirs(output_dir); logger.info(f"Создана папка: {output_dir}")
        except OSError as e: logger.exception(f"Не удалось создать папку '{output_dir}': {e}"); return None

    output_template = os.path.join(output_dir, '%(id)s_%(title).100s.%(ext)s')
    telegram_max_mb = 50
    ydl_opts = {
        'outtmpl': output_template,
        # Возвращаемся к формату, который не требует ffmpeg для слияния,
        # но при этом старается получить лучший MP4, не превышающий лимит по размеру.
        'format': f'best[ext=mp4][filesize<=?{telegram_max_mb}M]/best[ext=mp4]/best',
        'quiet': True, 'noprogress': True, 'noplaylist': True,
        'logger': logger, 'verbose': False, 'no_warnings': True,
    }

    downloaded_file_path = None
    info_dict = {} # Инициализируем info_dict
    try:
        logger.debug(f"Вызов yt_dlp для {video_url} с опциями: {ydl_opts}")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(video_url, download=True)
            logger.debug(f"yt_dlp info_dict (частично) для {video_url}: id={info_dict.get('id')}, title={info_dict.get('title', 'N/A')[:50]}, filename={info_dict.get('_filename', 'N/A')}, width={info_dict.get('width')}, height={info_dict.get('height')}, duration={info_dict.get('duration')}")

            expected_filename = ydl.prepare_filename(info_dict) if info_dict else None
            logger.debug(f"Ожидаемый путь файла от ydl.prepare_filename: {expected_filename}")
            actual_filepath = info_dict.get('requested_downloads', [{}])[0].get('filepath') or info_dict.get('_filename')
            logger.debug(f"Фактический путь из info_dict (requested_downloads/ _filename): {actual_filepath}")
            final_filename = actual_filepath or expected_filename

            if final_filename and os.path.exists(final_filename):
                 downloaded_file_path = final_filename
                 logger.info(f"Видео скачано/существует: {downloaded_file_path}")
                 try:
                     file_size_mb = os.path.getsize(downloaded_file_path) / (1024 * 1024)
                     if file_size_mb > telegram_max_mb:
                         logger.warning(f"Файл {downloaded_file_path} ({file_size_mb:.2f} MB) > {telegram_max_mb} MB.")
                         try: os.remove(downloaded_file_path); logger.info(f"Удален большой файл: {downloaded_file_path}")
                         except OSError as del_err: logger.error(f"Не удалось удалить большой файл {downloaded_file_path}: {del_err}")
                         return None
                     else: logger.info(f"Размер файла {downloaded_file_path}: {file_size_mb:.2f} MB.")
                 except OSError as size_err:
                      logger.error(f"Ошибка проверки размера {downloaded_file_path}: {size_err}")
                      try: os.remove(downloaded_file_path)
                      except OSError: pass
                      return None
            else:
                 logger.warning(f"Не найден путь скачанного файла для {video_url} ({final_filename}). Поиск по ID...")
                 if info_dict and (video_id := info_dict.get('id')):
                     try:
                         possible = [f for f in os.listdir(output_dir) if f.startswith(str(video_id)) and f.lower().endswith('.mp4')]
                         if possible:
                             found = os.path.join(output_dir, possible[0])
                             logger.debug(f"Найдены возможные файлы по ID {video_id}: {possible}. Выбран: {found}")
                             if os.path.exists(found):
                                 if (fs := os.path.getsize(found) / (1024*1024)) <= telegram_max_mb:
                                     downloaded_file_path = found
                                     logger.info(f"Найден файл по ID: {found} ({fs:.2f} MB)")
                                 else:
                                     logger.warning(f"Файл по ID {found} ({fs:.2f}MB) > {telegram_max_mb} MB.")
                                     try: os.remove(found); logger.info(f"Удален большой файл по ID: {found}")
                                     except OSError as del_err: logger.error(f"Не удалось удалить большой файл по ID {found}: {del_err}")
                             else:
                                 logger.error(f"Файл {found}, найденный по ID, не существует.")
                         else:
                             logger.error(f"Не найден файл по ID {video_id} в {output_dir}.")
                     except Exception as find_err: logger.error(f"Ошибка поиска файла в {output_dir}: {find_err}")
                 else: logger.error("info_dict или video_id отсутствуют для поиска файла по ID.")

    except yt_dlp.utils.DownloadError as e:
        msg = str(e).lower()
        if 'unsupported url' in msg: logger.error(f"yt-dlp: Неподдерживаемый URL: {video_url}")
        elif 'video unavailable' in msg: logger.warning(f"yt-dlp: Видео недоступно: {video_url}")
        elif 'no video formats found' in msg: logger.warning(f"yt-dlp: Нет форматов для {video_url}.")
        elif 'requested format not available' in msg or 'filtered' in msg: logger.warning(f"yt-dlp: Нет подходящего формата (< {telegram_max_mb}MB) для {video_url}.")
        else: logger.error(f"Ошибка скачивания yt-dlp {video_url}: {e}")
    except Exception as e: logger.exception(f"Неизвестная ошибка скачивания {video_url}: {e}")

    logger.debug(f"Результат download_vk_video для {video_url}: {downloaded_file_path}")
    video_metadata = {
        'width': info_dict.get('width'),
        'height': info_dict.get('height'),
        'duration': info_dict.get('duration'),
        'thumbnail': info_dict.get('thumbnail') # Добавляем URL миниатюры
    } if info_dict else {}
    return downloaded_file_path, video_metadata


# --- Вспомогательные функции для отправки ---
def _safe_send_tg_message(func, chat_id, *args, **kwargs):
    func_name = func.__name__
    text_plain = kwargs.pop('text_plain', None)
    caption_plain = kwargs.pop('caption_plain', None)
    current_kwargs = kwargs.copy()

    log_args_repr_list = []
    if args:
        for arg in args:
            if isinstance(arg, (bytes, io.BytesIO, io.BufferedReader)):
                 log_args_repr_list.append(f"<media data {type(arg).__name__}>")
            elif isinstance(arg, list) and all(isinstance(x, types.InputMedia) for x in arg):
                 media_repr = []
                 for item in arg:
                     if hasattr(item, 'media') and isinstance(item.media, (bytes, io.BytesIO, io.BufferedReader, io.TextIOWrapper)):
                          media_repr.append(f"<InputMedia {type(item).__name__} with {type(item.media).__name__}>")
                     elif hasattr(item, 'media') and isinstance(item.media, str) and item.media.startswith('attach://'):
                          media_repr.append(f"<InputMedia {type(item).__name__} with attach://...>")
                     else:
                          media_repr.append(repr(item))
                 log_args_repr_list.append(f"[{', '.join(media_repr)}]")
            else:
                 log_args_repr_list.append(repr(arg))
    log_args_repr = f"args=({', '.join(log_args_repr_list)})" if log_args_repr_list else ""

    log_kwargs_repr_list = []
    if current_kwargs:
        for k, v in current_kwargs.items():
            if k == 'media' and isinstance(v, list):
                 media_repr = []
                 for item in v:
                     if hasattr(item, 'media') and isinstance(item.media, (bytes, io.BytesIO, io.BufferedReader, io.TextIOWrapper)):
                          media_repr.append(f"<InputMedia {type(item).__name__} with {type(item.media).__name__}>")
                     elif hasattr(item, 'media') and isinstance(item.media, str) and item.media.startswith('attach://'):
                           media_repr.append(f"<InputMedia {type(item).__name__} with attach://...>")
                     else:
                          media_repr.append(repr(item))
                 log_kwargs_repr_list.append(f"media=[{', '.join(media_repr)}]")
            elif isinstance(v, (bytes, io.BytesIO, io.BufferedReader)):
                 log_kwargs_repr_list.append(f"{k}=<media data {type(v).__name__}>")
            else:
                 log_kwargs_repr_list.append(f"{k}={repr(v)}")
    log_kwargs_repr = f"kwargs={{{', '.join(log_kwargs_repr_list)}}}" if log_kwargs_repr_list else ""

    logger.debug(f"Попытка вызова {func_name} для chat_id={chat_id}. {log_args_repr} {log_kwargs_repr}. Передаваемые kwargs: {current_kwargs}")

    try:
        message = func(chat_id, *args, **current_kwargs)
        logger.debug(f"Успешно вызван {func_name} для chat_id={chat_id}.")
        return message
    except ApiTelegramException as e_tg:
        if 'parse error' in str(e_tg).lower() or 'can\'t parse entities' in str(e_tg).lower():
            logger.warning(f"Ошибка Markdown ({func_name}, чат {chat_id}): {e_tg}. Попытка без MD.")
            retry_kwargs = kwargs.copy()
            retry_kwargs.pop('parse_mode', None)
            if 'caption' in retry_kwargs and caption_plain is not None:
                 retry_kwargs['caption'] = caption_plain
                 logger.debug("Замена caption на caption_plain для повторной отправки.")
            elif 'text' in retry_kwargs and text_plain is not None:
                 retry_kwargs['text'] = text_plain
                 logger.debug("Замена text на text_plain для повторной отправки.")
            try:
                retry_log_args_repr = f"{args=}" if args else ""
                retry_log_kwargs_repr = f"{retry_kwargs=}" if retry_kwargs else ""
                logger.debug(f"Повторная попытка {func_name} для chat_id={chat_id} без Markdown. {retry_log_args_repr} {retry_log_kwargs_repr}")
                message = func(chat_id, *args, **retry_kwargs)
                logger.info(f"Отправлено без MD ({func_name}, чат {chat_id}).")
                return message
            except Exception as e_plain:
                logger.error(f"Не удалось отправить ({func_name}, чат {chat_id}) даже без MD: {e_plain}")
                return None
        elif 'request entity too large' in str(e_tg).lower() or e_tg.error_code == 413:
             logger.error(f"Файл слишком большой ({func_name}, чат {chat_id}): {e_tg}")
             return None
        elif 'replied message not found' in str(e_tg).lower() or (e_tg.error_code == 400 and 'reply message not found' in e_tg.description.lower()):
            logger.warning(f"Сообщение для ответа не найдено ({func_name}, чат {chat_id}): {e_tg}. Попытка без ответа.")
            retry_kwargs = kwargs.copy()
            retry_kwargs.pop('reply_to_message_id', None)
            try:
                retry_log_args_repr = f"{args=}" if args else ""
                retry_log_kwargs_repr = f"{retry_kwargs=}" if retry_kwargs else ""
                logger.debug(f"Повторная попытка {func_name} для chat_id={chat_id} без ответа. {retry_log_args_repr} {retry_log_kwargs_repr}")
                message = func(chat_id, *args, **retry_kwargs)
                logger.info(f"Отправлено без ответа ({func_name}, чат {chat_id}).")
                return message
            except Exception as e_no_reply:
                logger.error(f"Не удалось отправить ({func_name}, чат {chat_id}) даже без ответа: {e_no_reply}")
                return None
        elif 'webpage_media_empty' in str(e_tg).lower() or (e_tg.error_code == 400 and 'webpage_media_empty' in e_tg.description.lower()):
             media_arg_repr = "N/A"
             try: 
                 media_arg = None
                 if args: media_arg = args[0]
                 elif 'media' in current_kwargs: media_arg = current_kwargs['media']
                 if isinstance(media_arg, list):
                      media_repr_list = []
                      for item in media_arg:
                          if hasattr(item, 'media'):
                              media_repr_list.append(f"{type(item).__name__}(media='{item.media if isinstance(item.media, str) else type(item.media).__name__}', ...)")
                          else:
                              media_repr_list.append(repr(item))
                      media_arg_repr = f"[{', '.join(media_repr_list)}]"
                 elif isinstance(media_arg, str): media_arg_repr = media_arg
                 elif media_arg: media_arg_repr = repr(media_arg)
             except Exception as repr_e: media_arg_repr = f"<ошибка получения репрезентации: {repr_e}>"
             logger.error(f"Ошибка WEBPAGE_MEDIA_EMPTY ({func_name}, чат {chat_id}): {e_tg}. Проблемное медиа (репрезентация): {media_arg_repr}")
             raise e_tg
        else:
             logger.error(f"Неизвестная ошибка Telegram API ({func_name}, чат {chat_id}, код {e_tg.error_code}): {e_tg}")
             return None 
    except Exception as e_other:
        logger.exception(f"Неизвестная ошибка при отправке ({func_name}, чат {chat_id}): {e_other}")
        return None

def safe_send_message(chat_id, text_md, text_plain, **kwargs):
    limit = 4096
    if len(text_md) > limit: safe_limit = text_md.rfind('\n', 0, limit - 4); safe_limit = limit - 4 if safe_limit == -1 else safe_limit; text_md = text_md[:safe_limit] + "..."; logger.warning(f"Текст сообщения для chat_id={chat_id} обрезан до {limit} символов.")
    if len(text_plain) > limit: safe_limit_plain = text_plain.rfind('\n', 0, limit - 4); safe_limit_plain = limit - 4 if safe_limit_plain == -1 else safe_limit_plain; text_plain = text_plain[:safe_limit_plain] + "..."
    return _safe_send_tg_message(bot.send_message, chat_id, text=text_md, parse_mode='Markdown', text_plain=text_plain, **kwargs)

def safe_send_photo(chat_id, photo_data, caption_md=None, caption_plain=None, **kwargs):
    limit = CAPTION_LIMIT
    if caption_md and len(caption_md) > limit: safe_limit = caption_md.rfind('\n', 0, limit - 4); safe_limit = limit - 4 if safe_limit == -1 else safe_limit; caption_md = caption_md[:safe_limit] + "..."; logger.warning(f"Подпись к фото для chat_id={chat_id} обрезана до {limit} символов.")
    if caption_plain and len(caption_plain) > limit: safe_limit_plain = caption_plain.rfind('\n', 0, limit - 4); safe_limit_plain = limit - 4 if safe_limit_plain == -1 else safe_limit_plain; caption_plain = caption_plain[:safe_limit_plain] + "..."
    return _safe_send_tg_message(bot.send_photo, chat_id, photo_data, caption=caption_md, parse_mode='Markdown', caption_plain=caption_plain, **kwargs)

def safe_send_video(chat_id, video_file, caption_md=None, caption_plain=None, video_metadata=None, **kwargs):
    limit = CAPTION_LIMIT
    if caption_md and len(caption_md) > limit: safe_limit = caption_md.rfind('\n', 0, limit - 4); safe_limit = limit - 4 if safe_limit == -1 else safe_limit; caption_md = caption_md[:safe_limit] + "..."; logger.warning(f"Подпись к видео для chat_id={chat_id} обрезана до {limit} символов.")
    if caption_plain and len(caption_plain) > limit: safe_limit_plain = caption_plain.rfind('\n', 0, limit - 4); safe_limit_plain = limit - 4 if safe_limit_plain == -1 else safe_limit_plain; caption_plain = caption_plain[:safe_limit_plain] + "..."
    file_repr = getattr(video_file, 'name', str(video_file))
    logger.debug(f"Попытка отправки видеофайла: {file_repr} в chat_id={chat_id}")

    if video_metadata:
        if 'width' in video_metadata: kwargs['width'] = video_metadata['width']
        if 'height' in video_metadata: kwargs['height'] = video_metadata['height']
        if 'duration' in video_metadata: kwargs['duration'] = video_metadata['duration']
        if 'thumbnail' in video_metadata and video_metadata['thumbnail']:
            try:
                thumbnail_url = video_metadata['thumbnail']
                response = requests.get(thumbnail_url, stream=True, timeout=10)
                response.raise_for_status()
                thumbnail_bytes = io.BytesIO(response.content)
                kwargs['thumb'] = thumbnail_bytes
                logger.debug(f"Миниатюра успешно загружена и добавлена для видео. URL: {thumbnail_url}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Не удалось загрузить миниатюру с URL {thumbnail_url}: {e}")
            except Exception as e:
                logger.exception(f"Неизвестная ошибка при обработке миниатюры {thumbnail_url}: {e}")
        logger.debug(f"Добавлены метаданные видео: {video_metadata}. Итоговые kwargs для _safe_send_tg_message: {kwargs}")

    return _safe_send_tg_message(bot.send_video, chat_id, video_file, caption=caption_md, parse_mode='Markdown', supports_streaming=True, caption_plain=caption_plain, **kwargs)

def safe_send_media_group(chat_id, media_url_list, **kwargs):
    if not isinstance(media_url_list, list) or not all(isinstance(item, types.InputMediaPhoto) and isinstance(item.media, str) for item in media_url_list):
        logger.error(f"Неверный формат media_url_list для safe_send_media_group (ожидался список InputMediaPhoto с URL): {media_url_list}")
        return None

    logger.info(f"Попытка отправки медиагруппы ({len(media_url_list)} фото по URL) в чат {chat_id}...")
    try:
        sent_messages = _safe_send_tg_message(bot.send_media_group, chat_id, media=media_url_list, **kwargs)
        if sent_messages:
            logger.info(f"Медиагруппа ({len(media_url_list)} фото) успешно отправлена по URL.")
            return sent_messages
        else:
            logger.error(f"Не удалось отправить медиагруппу по URL (ошибка не WEBPAGE_MEDIA_EMPTY или не API, см. логи выше).")
            return None
    except ApiTelegramException as e_url:
        if 'webpage_media_empty' in str(e_url).lower() or (e_url.error_code == 400 and 'webpage_media_empty' in e_url.description.lower()):
            logger.warning(f"Ошибка WEBPAGE_MEDIA_EMPTY при отправке по URL. Запуск fallback: скачивание и отправка файлами...")

            media_files_list = []
            download_successful = True
            opened_files = [] 
            for i, item_url in enumerate(media_url_list):
                original_url = item_url.media
                logger.info(f"Fallback: Скачивание фото #{i+1}: {original_url}")
                downloaded_path = download_photo_to_file(original_url, output_dir=PHOTO_DOWNLOAD_DIR)

                if downloaded_path:
                    logger.info(f"Fallback: Фото #{i+1} скачано: {downloaded_path}")
                    try:
                        file_stream = open(downloaded_path, 'rb')
                        opened_files.append(file_stream)
                        media_files_list.append(types.InputMediaPhoto(
                            media=file_stream,
                            caption=item_url.caption,
                            parse_mode=item_url.parse_mode
                        ))
                    except FileNotFoundError:
                         logger.error(f"Fallback: Скачанный файл не найден: {downloaded_path}")
                         download_successful = False; break
                    except Exception as e_create_media:
                         logger.exception(f"Fallback: Ошибка создания InputMediaPhoto из файла {downloaded_path}: {e_create_media}")
                         download_successful = False
                         if 'file_stream' in locals() and file_stream and file_stream in opened_files:
                              try: file_stream.close(); opened_files.remove(file_stream)
                              except Exception: pass
                         break
                else:
                    logger.error(f"Fallback: Не удалось скачать фото #{i+1} ({original_url}). Отправка медиагруппы файлами отменена.")
                    download_successful = False; break

            if not download_successful or not media_files_list:
                logger.error("Fallback: Не удалось подготовить все фото для отправки файлами.")
                for f in opened_files: 
                    if not f.closed:
                        try: f.close()
                        except Exception: pass
                return None

            logger.info(f"Fallback: Попытка отправки медиагруппы ({len(media_files_list)} фото файлами)...")
            sent_messages_files = None
            try:
                sent_messages_files = _safe_send_tg_message(bot.send_media_group, chat_id, media=media_files_list, timeout=120, **kwargs)
                if sent_messages_files:
                    logger.info(f"Fallback: Медиагруппа ({len(media_files_list)} фото) успешно отправлена файлами.")
                    return sent_messages_files
                else:
                    logger.error(f"Fallback: Не удалось отправить медиагруппу файлами (ошибка залогирована в _safe_send_tg_message).")
                    return None
            except Exception as e_files:
                 logger.exception(f"Fallback: Исключение при отправке медиагруппы файлами: {e_files}")
                 return None
            finally:
                 logger.debug("Fallback: Закрытие файловых потоков...")
                 for f in opened_files:
                     if not f.closed:
                         try:
                              f.close()
                              logger.debug(f"Закрыт поток для: {getattr(f, 'name', 'N/A')}")
                         except Exception as close_err:
                              logger.error(f"Ошибка закрытия потока для {getattr(f, 'name', 'N/A')}: {close_err}")
        else:
            logger.error(f"Не удалось отправить медиагруппу по URL (ошибка ApiTelegramException, но не WEBPAGE_MEDIA_EMPTY, код {e_url.error_code}).")
            return None

    except Exception as e_generic:
        logger.exception(f"Непредвиденная ошибка в safe_send_media_group: {e_generic}")
        return None

# --- Основная функция отправки поста ---
def send_post_to_telegram(post, target_chat_id):
    post_id = post.get('id', 'N/A'); owner_id = post.get('owner_id', 'N/A')
    post_link = f"https://vk.com/wall{owner_id}_{post_id}"
    logger.info(f"Обработка поста {post_link} -> {target_chat_id}")
    logger.debug(f"Полные данные поста (начало): {str(post)[:500]}...")

    photo_urls = [] 

    try:
        group_name = "Группа VK"
        try:
            if isinstance(owner_id, int) and owner_id < 0:
                 if group_info_list := vk.groups.getById(group_id=abs(owner_id), fields='name'):
                     group_name = group_info_list[0].get('name', group_name)
                     logger.debug(f"Название группы получено: {group_name}")
        except Exception as e: logger.warning(f"Не удалось получить инфо о группе {owner_id}: {e}")

        escaped_group_name = group_name.replace('\\', '\\\\').replace('[', '\\[').replace(']', '\\]').replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
        escaped_post_link = post_link.replace('(', r'\(').replace(')', r'\)')
        first_text_md = f'[{escaped_group_name}]({escaped_post_link})\n'
        first_text_plain = f'{group_name}: {post_link}\n'
        
        original_text = post.get('text', '')
        prepared_text_plain = original_text 
        prepared_text_md = prepare_text(original_text)


        attachments = post.get('attachments', [])
        logger.debug(f"Найдено вложений: {len(attachments)} для поста {post_link}")
        video_info = []
        docs = []
        downloaded_video_files = []

        for i, att in enumerate(attachments):
            att_type = att.get('type')
            logger.debug(f"Обработка вложения #{i+1} типа '{att_type}' поста {post_link}")
            try:
                if att_type == 'photo':
                    if photo := att.get('photo'):
                        photo_id = photo.get('id', 'N/A')
                        logger.debug(f"Обработка фото ID: {photo_id}")
                        size_priority = ['w', 'z', 'y', 'x', 'r', 'q', 'p', 'o', 'm', 's']
                        available = photo.get('sizes', [])
                        logger.debug(f"Доступные размеры фото {photo_id}: {[s.get('type') for s in available]}")

                        best_url = next((s['url'] for size in size_priority for s in available if s.get('type') == size and s.get('url') and s.get('width',0)<=2560 and s.get('height',0)<=2560 and (s.get('width',0)+s.get('height',0))<=10000), None)
                        if not best_url:
                            valid = [s for s in available if s.get('url') and s.get('width',0)<=2560 and s.get('height',0)<=2560 and (s.get('width',0)+s.get('height',0))<=10000]
                            if valid: best_url = max(valid, key=lambda s: s.get('width', 0) * s.get('height', 0)).get('url')

                        if best_url:
                            logger.debug(f"Выбран URL для фото {photo_id}: {best_url}")
                            photo_urls.append(best_url)
                        else: logger.warning(f"Нет подходящего фото URL в посте {post_link}, вложение: {photo_id}")
                elif att_type == 'video':
                    if video := att.get('video'):
                        vid = video.get('id'); oid = video.get('owner_id'); key = video.get('access_key')
                        title = video.get('title', f'Видео {oid}_{vid}')
                        vk_link = f"https://vk.com/video{oid}_{vid}" + (f"?access_key={key}" if key else "")
                        logger.debug(f"Обработка видео: {vk_link}, Title: {title}")
                        preview = next((s['url'] for s in video.get('image', []) if s.get('url') and s.get('with_padding')), None) \
                               or next((s['url'] for s in sorted(video.get('image', []), key=lambda x: x.get('width', 0), reverse=True) if s.get('url')), None) \
                               or next((video[k] for k in ['photo_1280', 'photo_800', 'photo_640', 'photo_320', 'photo_130'] if k in video and isinstance(video[k], str)), None)
                        logger.debug(f"Превью для видео {vk_link}: {preview}")
                        escaped_title = title.replace('\\', '\\\\').replace('[', '\\[').replace(']', '\\]').replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
                        escaped_url = vk_link.replace('(', r'\(').replace(')', r'\)')
                        video_info.append({'vk_link': vk_link, 'title': escaped_title, 'url': escaped_url, 'preview': preview, 'plain_title': title, 'plain_url': vk_link, 'message_id': None})
                        downloaded_path, video_metadata = download_vk_video(vk_link, DOWNLOAD_DIR)
                        if downloaded_path:
                            downloaded_video_files.append({'path': downloaded_path, 'vk_link': vk_link, 'title': title, 'escaped_title': escaped_title, 'metadata': video_metadata})
                        else: logger.info(f"Видео {vk_link} не будет отправлено файлом.")
                elif att_type == 'doc':
                     if doc := att.get('doc'):
                         title = doc.get('title', 'Документ').replace('\\', '\\\\').replace('[', '\\[').replace(']', '\\]').replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
                         url = doc.get('url', '').replace('(', r'\(').replace(')', r'\)')
                         logger.debug(f"Найден документ: Title: {title}, URL: {url}")
                         if url: docs.append({'title': title, 'url': url})
                elif att_type == 'link':
                     if link_data := att.get('link'): 
                         title = link_data.get('title', link_data.get('caption', 'Ссылка'))
                         url = link_data.get('url')
                         logger.debug(f"Найдена ссылка (из вложения): Title: {title}, URL: {url}")

                         if url:
                             plain_text_url = url
                             if 'vk.cc/' in url:
                                 logger.debug(f"Обнаружена vk.cc ссылка во вложении: {url}. Попытка развернуть...")
                                 full_url = get_unshortened_url(url) 
                                 if full_url and full_url != url:
                                     url = full_url 
                                     plain_text_url = full_url 
                                     logger.info(f"Замена vk.cc (вложение): {link_data.get('url')} -> {full_url}")
                             escaped_title_link = title.replace('\\', '\\\\').replace('[', '\\[').replace(']', '\\]').replace('_', '\\_').replace('*', '\\*').replace('`', '\\`')
                             escaped_url_link = url.replace('(', r'\(').replace(')', r'\)') 
                             link_md_text_to_append = f"\n\n🔗 [{escaped_title_link}]({escaped_url_link})"
                             link_plain_text_to_append = f"\n\n🔗 {title}: {plain_text_url}"
                             prepared_text_md += link_md_text_to_append
                             prepared_text_plain += link_plain_text_to_append
                             logger.debug(f"Ссылка из вложения добавлена в текст поста: {link_plain_text_to_append[:100]}...")
                else:
                     logger.debug(f"Пропуск неподдерживаемого типа вложения: {att_type}")
            except Exception as e: logger.exception(f"Ошибка обработки вложения {att_type} поста {post_link}: {e}")

        sent_something = False
        last_sent_message_id = None
        text_sent_separately = False
        has_media = bool(photo_urls or video_info) 
        full_caption_md = f"{first_text_md}{prepared_text_md}".strip()
        full_caption_plain = f"{first_text_plain}{prepared_text_plain}".strip()
        can_use_full_caption = has_media and len(full_caption_md) <= CAPTION_LIMIT
        logger.debug(f"Пост {post_link}: has_media={has_media} (URL фото: {len(photo_urls)}, видео: {len(video_info)}), len(full_caption_md)={len(full_caption_md)}, CAPTION_LIMIT={CAPTION_LIMIT}, can_use_full_caption={can_use_full_caption}")

        if not can_use_full_caption and (prepared_text_md or not has_media): 
            logger.info(f"Текст поста {post_link} (возможно, с ссылками из вложений) будет отправлен отдельно.")
            text_to_send_md = full_caption_md if prepared_text_md else first_text_md.strip() 
            text_to_send_plain = full_caption_plain if prepared_text_plain else first_text_plain.strip()
            if text_to_send_md: 
                sent_text_msg = safe_send_message(target_chat_id, text_to_send_md, text_to_send_plain, disable_web_page_preview=False) 
                if sent_text_msg:
                    sent_something = True
                    text_sent_separately = True 
                    last_sent_message_id = sent_text_msg.message_id
                    logger.debug(f"Текст поста {post_link} отправлен отдельно, message_id: {last_sent_message_id}")
                else:
                    logger.error(f"Не удалось отправить текст поста {post_link}.")
            elif not has_media and not prepared_text_md : 
                 logger.warning(f"Пост {post_link} не содержит текста для отдельной отправки и нет медиа для подписи.")

        if photo_urls:
            media_urls = [] 
            first_photo_caption_md = None
            first_photo_caption_plain = None
            if can_use_full_caption: 
                first_photo_caption_md = full_caption_md
                first_photo_caption_plain = full_caption_plain
                logger.debug(f"Текст поста {post_link} (с ссылками) будет в подписи к первому фото.")
            elif not text_sent_separately: 
                first_photo_caption_md = first_text_md.strip()
                first_photo_caption_plain = first_text_plain.strip()
                logger.debug(f"Только ссылка на пост VK {post_link} будет в подписи к первому фото (основной текст отправлен или будет отправлен позже, или не поместился).")

            for i, url in enumerate(photo_urls):
                current_caption_md = first_photo_caption_md if i == 0 else None
                try:
                    logger.debug(f"Создание InputMediaPhoto из URL: {url}, caption='{str(current_caption_md)[:50]}...'")
                    media_urls.append(types.InputMediaPhoto(media=url, caption=current_caption_md, parse_mode='Markdown'))
                except Exception as e_mp:
                     logger.error(f"Не удалось создать InputMediaPhoto из URL ({url}) поста {post_link}: {e_mp}")
            
            if media_urls:
                send_kwargs = {}
                sent_media_msgs = safe_send_media_group(target_chat_id, media_urls, **send_kwargs) 
                if sent_media_msgs:
                    sent_something = True
                    if not text_sent_separately and not first_photo_caption_md and prepared_text_md:
                        logger.info(f"Отправка текста поста {post_link} после медиагруппы (не поместился в подпись).")
                        text_after_media_md = prepared_text_md.strip() 
                        text_after_media_plain = prepared_text_plain.strip()
                        if text_after_media_md: 
                            sent_text_msg_after = safe_send_message(target_chat_id, text_after_media_md, text_after_media_plain, disable_web_page_preview=False)
                            if sent_text_msg_after: last_sent_message_id = sent_text_msg_after.message_id
                            else: logger.error(f"Не удалось отправить текст поста {post_link} после медиагруппы.")
                    media_msg_ids = [msg.message_id for msg in sent_media_msgs]
                    last_sent_message_id = media_msg_ids[0]
                else:
                    logger.error(f"Не удалось отправить медиагруппу поста {post_link} (URL и fallback не сработали, ошибки см. выше).")
            else:
                 logger.warning(f"Медиагруппа для поста {post_link} пуста (ошибки создания InputMediaPhoto из URL).")

        if video_info:
            logger.info(f"Отправка {len(video_info)} превью/ссылок на видео поста {post_link}...")
            for i, v in enumerate(video_info):
                sent_preview_msg = None
                current_caption_md = None
                current_caption_plain = None
                is_first_media_overall = (i == 0 and not photo_urls) 
                if can_use_full_caption and is_first_media_overall:
                    current_caption_md = full_caption_md
                    current_caption_plain = full_caption_plain
                    logger.debug(f"Текст поста {post_link} (с ссылками) будет в подписи к первому видео превью.")
                elif not text_sent_separately and is_first_media_overall: 
                     current_caption_md = first_text_md.strip()
                     current_caption_plain = first_text_plain.strip()
                     logger.debug(f"Только ссылка на пост VK {post_link} будет в подписи к первому видео превью.")
                else: 
                    current_caption_md = f"*Видео:* [{v['title']}]({v['url']})"
                    current_caption_plain = f"Видео: {v['plain_title']}: {v['plain_url']}"
                    logger.debug(f"Стандартная подпись для видео превью #{i+1} поста {post_link}.")

                if v['preview']:
                    logger.debug(f"Отправка превью видео {v['plain_url']} через safe_send_photo.")
                    sent_preview_msg = safe_send_photo(target_chat_id, v['preview'], current_caption_md, current_caption_plain)
                    if not sent_preview_msg:
                        logger.error(f"Не удалось отправить превью видео {v['plain_url']}. Попытка отправить текстом.")
                        sent_preview_msg = safe_send_message(target_chat_id, current_caption_md, current_caption_plain, disable_web_page_preview=False)
                else:
                    logger.warning(f"Нет превью для видео '{v['plain_title']}'. Отправка текстом.")
                    sent_preview_msg = safe_send_message(target_chat_id, current_caption_md, current_caption_plain, disable_web_page_preview=False)

                if sent_preview_msg:
                    sent_something = True
                    video_info[i]['message_id'] = sent_preview_msg.message_id
                    last_sent_message_id = sent_preview_msg.message_id
                    if not text_sent_separately and not (can_use_full_caption and is_first_media_overall) and is_first_media_overall and prepared_text_md: 
                        logger.info(f"Отправка текста поста {post_link} после первого видео (не поместился в подпись или не был основной подписью).")
                        text_after_video_md = prepared_text_md.strip()
                        text_after_video_plain = prepared_text_plain.strip()
                        if text_after_video_md:
                             sent_text_msg_after = safe_send_message(target_chat_id, text_after_video_md, text_after_video_plain, disable_web_page_preview=False)
                             if sent_text_msg_after: last_sent_message_id = sent_text_msg_after.message_id 
                             else: logger.error(f"Не удалось отправить текст поста {post_link} после первого видео.")
                    logger.info(f"Информация о видео {v['plain_url']} отправлена, message_id: {last_sent_message_id}")
                else:
                    logger.error(f"Не удалось отправить информацию о видео {v['plain_url']}")
                time.sleep(0.5)

        if downloaded_video_files:
            logger.info(f"Отправка {len(downloaded_video_files)} скачанных видеофайлов поста {post_link}...")
            for vid_file_info in downloaded_video_files:
                path = vid_file_info['path']
                title = vid_file_info['title']
                escaped_title = vid_file_info['escaped_title']
                vk_link = vid_file_info['vk_link']
                reply_to_msg_id = None
                for v_preview in video_info:
                    if v_preview['vk_link'] == vk_link:
                        reply_to_msg_id = v_preview.get('message_id')
                        break
                logger.debug(f"Поиск reply_to_message_id для файла {path} (ссылка {vk_link}): найдено {reply_to_msg_id}")
                caption_md = f"{escaped_title}"
                caption_plain = f"{title}"
                logger.info(f"Отправка видеофайла: {path}" + (f" (в ответ на {reply_to_msg_id})" if reply_to_msg_id else ""))
                send_args = {'timeout': 180}
                if reply_to_msg_id:
                    send_args['reply_to_message_id'] = reply_to_msg_id
                else:
                    logger.warning(f"Не найден message_id для ответа при отправке файла {path}. Отправка без ответа.")
                try:
                    with open(path, 'rb') as vf:
                        sent_video_msg = safe_send_video(target_chat_id, vf, caption_md, caption_plain, video_metadata=vid_file_info['metadata'], **send_args)
                        if sent_video_msg:
                            sent_something = True
                            last_sent_message_id = sent_video_msg.message_id
                            logger.info(f"Видеофайл {path} отправлен, message_id: {last_sent_message_id}")
                        else:
                            logger.error(f"Не удалось отправить видеофайл {path} (ошибка залогирована выше).")
                except FileNotFoundError:
                    logger.error(f"Видеофайл не найден: {path}.")
                except Exception as e:
                    logger.exception(f"Критическая ошибка при отправке видеофайла {path}: {e}")
                time.sleep(1)

        if docs:
            sup_md, sup_plain = "", ""
            if docs:
                doc_md_list = [f"- [{d['title']}]({d['url']})" for d in docs]
                doc_plain_list = []
                for d in docs:
                    processed_title = d['title'].replace('\\\\','\\').replace('\\[','[').replace('\\]',']').replace('\\_','_').replace('\\*','*').replace('\\`','`')
                    processed_url = d['url'].replace('\\(','(').replace('\\)',')')
                    doc_plain_list.append(f"- {processed_title}: {processed_url}")
                sup_md += "\n\n*Документы:*\n" + "\n".join(doc_md_list)
                sup_plain += "\n\nДокументы:\n" + "\n".join(doc_plain_list)
            
            if sup_md: 
                 final_sup_md = sup_md.strip()
                 final_sup_plain = sup_plain.strip()
                 if not sent_something: 
                     logger.info(f"Отправка доп. информации (документы) С ССЫЛКОЙ НА ПОСТ {post_link}...")
                     final_sup_md = f"{first_text_md.strip()}\n{final_sup_md}"
                     final_sup_plain = f"{first_text_plain.strip()}\n{final_sup_plain}"
                 else: 
                      logger.info(f"Отправка доп. информации (документы) поста {post_link}...")
                 sent_sup_msg = safe_send_message(target_chat_id, final_sup_md, final_sup_plain, disable_web_page_preview=True) 
                 if sent_sup_msg:
                     sent_something = True
                     last_sent_message_id = sent_sup_msg.message_id
                     logger.info(f"Доп. информация (документы) поста {post_link} отправлена, message_id: {last_sent_message_id}")
                 else:
                     logger.error(f"Не удалось отправить доп. инфо (документы) поста {post_link}.")

        if not sent_something and (not prepared_text_md or (prepared_text_md and not has_media and not text_sent_separately)):
             logger.warning(f"Пост {post_link} без контента/медиа или ничего не удалось отправить. Отправка fallback-сообщения.")
             final_fallback_md = full_caption_md if prepared_text_md else first_text_md.strip()
             final_fallback_plain = full_caption_plain if prepared_text_plain else first_text_plain.strip()
             if final_fallback_md: 
                 sent_link_msg = safe_send_message(target_chat_id, final_fallback_md, final_fallback_plain, disable_web_page_preview=False) 
                 if sent_link_msg:
                     sent_something = True
                     last_sent_message_id = sent_link_msg.message_id
                     logger.info(f"Fallback-сообщение для поста {post_link} отправлено, message_id: {last_sent_message_id}")
                 else:
                     logger.error(f"Не удалось отправить даже fallback-сообщение для поста {post_link}.")
             else:
                 logger.error(f"Fallback-сообщение для поста {post_link} пустое, отправка отменена.")

        if sent_something:
            logger.info(f"Обработка поста {post_link} успешно завершена.");
            return True
        else:
            logger.error(f"Не удалось отправить никакую информацию для поста {post_link} (ошибки см. выше).");
            return False

    except Exception as e:
        logger.exception(f"Критическая ошибка при обработке поста {post_link}: {e}")
        return False
    finally:
        pass

def check_and_send_vk_posts(group_id, group_key, target_chat_id):
    logger.info(f"Проверка группы {group_key} (ID: {group_id}) -> {target_chat_id}...")
    group_owner_id = int(f"-{group_id}")
    processed_posts = load_posts_state(group_key)
    max_history = getattr(config, 'MAX_POST_HISTORY', 1000)
    if len(processed_posts) > max_history:
         try:
             valid_ids = [int(k) for k, v in processed_posts.items() if k.lstrip('-').isdigit()]
             processed_posts = {k: v for k, v in processed_posts.items() if k.lstrip('-').isdigit()}
             sorted_ids = sorted(valid_ids, reverse=True)
             processed_posts = {str(pid): processed_posts[str(pid)] for pid in sorted_ids[:max_history]}
             logger.debug(f"История {group_key} сокращена до {len(processed_posts)}.")
         except Exception as e_sort: logger.warning(f"Не удалось сократить историю {group_key}: {e_sort}")

    new_posts_found = 0
    try:
        posts_to_fetch = getattr(config, 'VK_POSTS_COUNT', 20)
        logger.debug(f"Запрос {posts_to_fetch} постов для owner_id={group_owner_id}")
        response = vk.wall.get(owner_id=group_owner_id, count=posts_to_fetch, extended=1, filter='owner')
        logger.debug(f"Ответ VK API для {group_key} получен (items: {'items' in response})")

        if 'items' not in response:
            error_detail = response.get('error', {}).get('error_msg', str(response))
            logger.error(f"VK API для группы {group_id} без 'items'. Детали: {error_detail}"); return

        posts = [p for p in response['items'] if not p.get('marked_as_ads') and p.get('post_type') == 'post']
        logger.debug(f"Получено {len(response['items'])}, после фильтрации {len(posts)} постов для {group_key}.")

        for post in reversed(posts):
            post_id = str(post.get('id'))
            post_link = f"https://vk.com/wall{group_owner_id}_{post_id}"
            logger.debug(f"Проверка поста {post_link} ({group_key})...")

            if post.get('owner_id') != group_owner_id:
                 logger.debug(f"Пост {post_link} пропущен (не со стены группы, owner_id: {post.get('owner_id')}).")
                 continue

            if post_id in processed_posts:
                 logger.debug(f"Пост {post_link} уже обработан ({processed_posts[post_id]}). Пропуск.")
                 continue

            post_text_lower = post.get('text','').lower()
            # Используем копию filter_words для итерации, если планируется его изменение в другом потоке
            current_filter_words = list(filter_words) 
            if current_filter_words and any(word.lower() in post_text_lower for word in current_filter_words):
                logger.info(f"Пост {post_link} ({group_key}) отфильтрован по словам.")
                processed_posts[post_id] = f"filtered_{time.time()}"; continue

            if post.get('copy_history'):
                 logger.info(f"Пост {post_link} ({group_key}) - репост, пропуск.")
                 processed_posts[post_id] = f"repost_skipped_{time.time()}"; continue

            logger.info(f"Новый пост {post_link} ({group_key}). Отправка в {target_chat_id}...")
            if send_post_to_telegram(post, target_chat_id):
                processed_posts[post_id] = f"sent_{time.time()}"; new_posts_found += 1
                logger.info(f"Пост {post_link} успешно отправлен.")
                time.sleep(getattr(config, 'DELAY_BETWEEN_POSTS', 3))
            else:
                logger.warning(f"Отправка поста {post_link} ({group_key}) не удалась.")
                processed_posts[post_id] = f"failed_{time.time()}"

    except vk_api.ApiError as e:
        logger.error(f"Ошибка VK API группы {group_id} (код {e.code}): {e}")
        if e.code == 29: logger.warning("Лимит VK API достигнут. Пауза..."); time.sleep(300)
        elif e.code == 5: send_error_to_admin(f"Ошибка авторизации VK (группа {group_id})? Проверьте токен.", is_critical=True)
        elif e.code == 15: logger.warning(f"Доступ к контенту запрещен (группа {group_id}, код 15): {e}")
        elif e.code == 100: logger.error(f"Ошибка параметров VK API (группа {group_id}, код 100): {e}")
    except requests.exceptions.RequestException as e: logger.error(f"Сетевая ошибка при запросе к VK API ({group_id}): {e}")
    except Exception as e: logger.exception(f"Непредвиденная ошибка при проверке группы {group_id}: {e}")
    finally:
        save_posts_state(group_key, processed_posts)
        logger.info(f"Проверка группы {group_key} завершена. Отправлено новых постов: {new_posts_found}.")

def admin_only(func):
    def wrapped(message):
        admin_id_str = str(getattr(config, 'ADMIN_CHAT_ID', None))
        user_chat_id_str = str(message.chat.id)
        command_name = func.__name__
        logger.debug(f"Попытка вызова команды /{command_name} пользователем chat_id={user_chat_id_str}, user={message.from_user.username or message.from_user.id}")

        if not admin_id_str or user_chat_id_str != admin_id_str:
            logger.warning(f"Доступ к команде /{command_name} запрещен для chat_id={user_chat_id_str}")
            if command_name != 'send_welcome':
                try: bot.reply_to(message, "⛔ Доступ запрещен. Эта команда только для администратора.", parse_mode=None)
                except Exception: pass
            return
        logger.info(f"Администратор ({user_chat_id_str}) вызвал команду /{command_name}")
        return func(message)
    wrapped.__name__ = func.__name__; wrapped.__doc__ = func.__doc__; return wrapped

@bot.message_handler(commands=['start', 'help'])
@admin_only
def send_welcome(message):
    help_text = """
👋 *Привет! Я бот для пересылки постов из VK в Telegram.*

Я слежу за указанными группами VK и отправляю новые посты в целевой чат (или администратору для вторичных групп).

*Доступные команды (только для админа):*
`/filter слово` - Добавить слово/фразу в фильтр.
`/remove слово` - Удалить слово/фразу из фильтра.
`/list_filter` - Показать текущий список слов/фраз в фильтре.
`/log [N]` - Показать последние N строк лог-файла (по умолчанию 10).
`/set_loglevel [DEBUG|INFO|WARNING|ERROR]` - Установить уровень логирования для файла.
`/clear_videos` - Очистить папку скачанных видео (`vk_videos`).
`/clear_photos` - Очистить папку временных фото (`vk_photos_temp`).
`/help` или `/start` - Показать это справочное сообщение.

Настройки бота задаются в файле `config.py`.
"""
    try: bot.reply_to(message, help_text, parse_mode='Markdown')
    except Exception:
        try: bot.reply_to(message, help_text.replace('*','').replace('`',''), parse_mode=None)
        except Exception as e: logger.error(f"Не удалось отправить /start или /help: {e}")

@bot.message_handler(commands=['filter'])
@admin_only
def handle_filter(message):
    global filter_words
    try:
        if len(parts := message.text.split(maxsplit=1)) > 1 and (new_filter := parts[1].strip().lower()):
            escaped_new_filter = new_filter.replace('`','\\`')
            reply = ""
            if new_filter not in filter_words:
                filter_words.append(new_filter)
                save_filter_words()
                reply = f"✅ Фильтр `{escaped_new_filter}` добавлен."
                logger.info(f"Фильтр добавлен администратором: '{new_filter}'")
            else:
                reply = f"⚠️ Фильтр `{escaped_new_filter}` уже существует."

            escaped_list = [f.replace('_','\\_').replace('*','\\*').replace('`','\\`') for f in filter_words]
            reply += f"\n\n*Текущие фильтры ({len(filter_words)}):*\n`{', '.join(escaped_list) if escaped_list else 'Список пуст'}`"
        else:
            reply = "⚠️ Использование: `/filter слово или фраза`"
        bot.reply_to(message, reply, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Ошибка при выполнении /filter: {e}")
        try: bot.reply_to(message, f"❌ Произошла ошибка при добавлении фильтра: {e}", parse_mode=None)
        except Exception: pass

@bot.message_handler(commands=['remove'])
@admin_only
def handle_remove(message):
    global filter_words
    try:
        if len(parts := message.text.split(maxsplit=1)) > 1 and (filter_to_remove := parts[1].strip().lower()):
            escaped_filter_to_remove = filter_to_remove.replace('`','\\`')
            reply = ""
            if filter_to_remove in filter_words:
                filter_words.remove(filter_to_remove)
                save_filter_words()
                reply = f"✅ Фильтр `{escaped_filter_to_remove}` удален."
                logger.info(f"Фильтр удален администратором: '{filter_to_remove}'")
            else:
                reply = f"⚠️ Фильтр `{escaped_filter_to_remove}` не найден в списке."

            escaped_list = [f.replace('_','\\_').replace('*','\\*').replace('`','\\`') for f in filter_words]
            reply += f"\n\n*Текущие фильтры ({len(filter_words)}):*\n`{', '.join(escaped_list) if escaped_list else 'Список пуст'}`"
        else:
            reply = "⚠️ Использование: `/remove слово или фраза`"
        bot.reply_to(message, reply, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Ошибка при выполнении /remove: {e}")
        try: bot.reply_to(message, f"❌ Произошла ошибка при удалении фильтра: {e}", parse_mode=None)
        except Exception: pass

@bot.message_handler(commands=['list_filter'])
@admin_only
def handle_list_filter(message):
    try:
        current_filter_words = list(filter_words) # Копируем для безопасной итерации
        if current_filter_words:
            escaped = [f.replace('_','\\_').replace('*','\\*').replace('`','\\`') for f in current_filter_words]
            filter_list_str = "\n".join([f"{i+1}. `{item}`" for i, item in enumerate(escaped)])
            reply = f"*Текущие фильтры ({len(current_filter_words)}):*\n{filter_list_str}"
            mode = 'Markdown'
        else:
            reply = "ℹ️ Список фильтров пуст."
            mode = None
        bot.reply_to(message, reply, parse_mode=mode)
    except Exception as e:
        logger.error(f"Ошибка при выполнении /list_filter: {e}")
        try: bot.reply_to(message, f"❌ Произошла ошибка при показе фильтров: {e}", parse_mode=None)
        except Exception: pass

@bot.message_handler(commands=['log'])
@admin_only
def handle_get_log(message):
    try:
        count = 10
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            try:
                requested_count = int(parts[1].strip())
                if requested_count > 0: count = min(requested_count, 1000)
                else: bot.reply_to(message,"⚠️ Количество строк должно быть положительным.", parse_mode=None); return
            except ValueError: bot.reply_to(message,"⚠️ Неверный формат. Укажите число строк: `/log 50`", parse_mode='Markdown'); return

        logger.info(f"Администратор запросил последние {count} строк лога.")
        if not os.path.exists(log_file_path): bot.reply_to(message,"⚠️ Файл лога не найден.", parse_mode=None); return

        try:
            log_lines = []
            try:
                with open(log_file_path, 'r', encoding='utf-8') as f: log_lines.extend(f.readlines())
            except FileNotFoundError: logger.warning(f"Основной лог-файл {log_file_path} не найден при чтении для /log.")
            except Exception as read_err: logger.error(f"Ошибка чтения основного лог-файла {log_file_path}: {read_err}"); bot.reply_to(message, f"❌ Ошибка чтения основного лог-файла: {read_err}", parse_mode=None); return

            needed = count - len(log_lines)
            backup_index = 1
            while needed > 0 and os.path.exists(f"{log_file_path}.{backup_index}"):
                 backup_file = f"{log_file_path}.{backup_index}"
                 logger.debug(f"Чтение бэкапа лога: {backup_file} (нужно еще {needed} строк)")
                 try:
                     with open(backup_file, 'r', encoding='utf-8') as bf:
                         backup_lines = bf.readlines()
                         log_lines = backup_lines + log_lines
                         needed = count - len(log_lines)
                 except Exception as backup_read_err: logger.error(f"Ошибка чтения бэкапа лога {backup_file}: {backup_read_err}")
                 backup_index += 1

        except Exception as read_err: logger.error(f"Ошибка чтения лог-файлов: {read_err}"); bot.reply_to(message, f"❌ Не удалось прочитать лог-файлы: {read_err}", parse_mode=None); return

        last_lines = log_lines[-count:]
        if not last_lines: bot.reply_to(message,"ℹ️ Логи пусты.", parse_mode=None); return

        output = "".join(last_lines).strip()
        reply_header = f"Последние {len(last_lines)} из запрошенных {count} строк лога:\n"
        max_output_len = 4096 - len(reply_header) - 10 # Оставляем место для ```Markdown``` и заголовка
        if len(output) > max_output_len:
            # Обрезаем с начала, чтобы сохранить самые последние логи
            start_index = len(output) - max_output_len
            # Ищем ближайший перенос строки, чтобы не обрывать строку на середине
            newline_before_truncate = output.rfind('\n', 0, start_index)
            if newline_before_truncate != -1:
                 start_index = newline_before_truncate + 1

            truncated_output = "...\n" + output[start_index:]
            reply = f"{reply_header}```\n{truncated_output}\n```"
            logger.warning(f"Вывод лога ({len(output)} символов) был обрезан до ~{max_output_len} символов.")
        else:
            reply = f"{reply_header}```\n{output}\n```"

        try:
             bot.reply_to(message, reply, parse_mode='Markdown')
        except ApiTelegramException as e_md:
             if 'parse error' in str(e_md).lower() or "can't parse entities" in str(e_md).lower():
                  logger.warning("Ошибка Markdown при отправке лога. Отправка без форматирования.")
                  reply_plain = f"{reply_header}{output}" # Без ```
                  if len(reply_plain) > 4096: 
                      # Обрезаем текст, если он все еще слишком длинный
                      plain_max_len = 4096 - len(reply_header) - 20 # Запас для "..."
                      start_index_plain = len(output) - plain_max_len
                      newline_plain = output.rfind('\n', 0, start_index_plain)
                      if newline_plain != -1:
                          start_index_plain = newline_plain + 1
                      reply_plain = f"{reply_header}...\n{output[start_index_plain:]}"

                  bot.reply_to(message, reply_plain, parse_mode=None)
             else: raise e_md # Перебрасываем другие ошибки API

    except Exception as e:
        logger.error(f"Ошибка при выполнении /log: {e}", exc_info=True)
        try: bot.reply_to(message, f"❌ Произошла ошибка при получении лога: {e}", parse_mode=None)
        except Exception: pass

@bot.message_handler(commands=['set_loglevel'])
@admin_only
def handle_set_loglevel(message):
    global rotating_handler, log_formatter_debug, log_formatter_info
    allowed_levels = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR}
    current_level_name = logging.getLevelName(rotating_handler.level)
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            new_level_name = parts[1].strip().upper()
            if new_level_name in allowed_levels:
                new_level = allowed_levels[new_level_name]
                rotating_handler.setLevel(new_level)
                if new_level == logging.DEBUG:
                    rotating_handler.setFormatter(log_formatter_debug)
                    logger.info(f"Уровень логирования файла изменен на DEBUG (с детальным форматом).")
                    bot.reply_to(message, f"✅ Уровень логирования файла установлен на `{new_level_name}` (детальный формат).", parse_mode='Markdown')
                else:
                    rotating_handler.setFormatter(log_formatter_info)
                    logger.info(f"Уровень логирования файла изменен на {new_level_name} (стандартный формат).")
                    bot.reply_to(message, f"✅ Уровень логирования файла установлен на `{new_level_name}` (стандартный формат).", parse_mode='Markdown')
            else:
                bot.reply_to(message, f"⚠️ Неверный уровень логирования. Доступные: {', '.join(allowed_levels.keys())}. Текущий: `{current_level_name}`.", parse_mode='Markdown')
        else:
            bot.reply_to(message, f"ℹ️ Текущий уровень логирования файла: `{current_level_name}`.\nИспользование: `/set_loglevel [DEBUG|INFO|WARNING|ERROR]`", parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Ошибка при выполнении /set_loglevel: {e}", exc_info=True)
        try: bot.reply_to(message, f"❌ Произошла ошибка при установке уровня логирования: {e}", parse_mode=None)
        except Exception: pass

@bot.message_handler(commands=['clear_videos'])
@admin_only
def handle_clear_videos(message):
    logger.info(f"Администратор инициировал очистку папки {DOWNLOAD_DIR}.")
    try:
        clear_download_folder(DOWNLOAD_DIR)
        bot.reply_to(message, f"✅ Папка `{DOWNLOAD_DIR}` успешно очищена.", parse_mode='Markdown')
        logger.info(f"Папка {DOWNLOAD_DIR} очищена по команде администратора.")
    except Exception as e:
        logger.error(f"Ошибка при выполнении /clear_videos: {e}", exc_info=True)
        try: bot.reply_to(message, f"❌ Произошла ошибка при очистке папки `{DOWNLOAD_DIR}`: {e}", parse_mode='Markdown')
        except Exception:
             try: bot.reply_to(message, f"❌ Произошла ошибка при очистке папки {DOWNLOAD_DIR}: {e}", parse_mode=None)
             except Exception: pass

@bot.message_handler(commands=['clear_photos'])
@admin_only
def handle_clear_photos(message):
    folder_to_clear = PHOTO_DOWNLOAD_DIR
    logger.info(f"Администратор инициировал очистку папки {folder_to_clear}.")
    try:
        clear_download_folder(folder_to_clear)
        bot.reply_to(message, f"✅ Папка `{folder_to_clear}` успешно очищена.", parse_mode='Markdown')
        logger.info(f"Папка {folder_to_clear} очищена по команде администратора.")
    except Exception as e:
        logger.error(f"Ошибка при выполнении /clear_photos: {e}", exc_info=True)
        try: bot.reply_to(message, f"❌ Произошла ошибка при очистке папки `{folder_to_clear}`: {e}", parse_mode='Markdown')
        except Exception:
             try: bot.reply_to(message, f"❌ Произошла ошибка при очистке папки {folder_to_clear}: {e}", parse_mode=None)
             except Exception: pass

def vk_check_loop():
    logger.info("Запуск основного цикла проверки VK...")
    check_interval = getattr(config, 'VK_CHECK_INTERVAL_SECONDS', 60)
    primary_group_id_str = str(getattr(config, 'PRIMARY_VK_GROUP_ID', ''))
    secondary_groups = getattr(config, 'SECONDARY_VK_GROUPS', {})
    admin_chat_id = getattr(config, 'ADMIN_CHAT_ID', None)
    target_chat_id = getattr(config, 'TARGET_TELEGRAM_CHAT_ID', None)
    delay_between_groups = getattr(config, 'DELAY_BETWEEN_GROUPS', 5)

    if not primary_group_id_str and not secondary_groups:
        logger.critical("Критическая ошибка конфигурации: Не указаны ID групп VK. Бот остановлен.")
        send_error_to_admin("Критическая ошибка конфигурации: Не указаны ID групп VK для проверки. Бот остановлен.", is_critical=True); return
    if primary_group_id_str and not target_chat_id:
        logger.critical("Критическая ошибка конфигурации: Указан PRIMARY_VK_GROUP_ID, но не указан TARGET_TELEGRAM_CHAT_ID. Бот остановлен.")
        send_error_to_admin("Критическая ошибка конфигурации: Не указан TARGET_TELEGRAM_CHAT_ID для основной группы. Бот остановлен.", is_critical=True); return
    if not admin_chat_id:
        logger.warning("ADMIN_CHAT_ID не указан в config.py. Уведомления об ошибках и посты из вторичных групп не будут отправляться.")

    while True:
        loop_start_time = time.time()
        logger.info(f"--- Начало цикла проверки VK ({time.strftime('%Y-%m-%d %H:%M:%S')}) ---")
        memory_handler.buffer.clear(); logger.debug("Буфер ошибок в памяти очищен.")

        try:
            clear_download_folder(DOWNLOAD_DIR)
            clear_download_folder(PHOTO_DOWNLOAD_DIR) 

            if primary_group_id_str and target_chat_id:
                logger.info(f"Начало проверки основной группы: {primary_group_id_str}")
                try:
                    primary_group_id_int = int(primary_group_id_str)
                    check_and_send_vk_posts(primary_group_id_int, f"primary_{primary_group_id_int}", target_chat_id)
                    logger.info(f"Завершение проверки основной группы: {primary_group_id_str}. Пауза {delay_between_groups} сек...")
                    time.sleep(delay_between_groups)
                except ValueError:
                    logger.error(f"Некорректный PRIMARY_VK_GROUP_ID: '{primary_group_id_str}'.")
                    send_error_to_admin(f"Ошибка конфигурации: Некорректный PRIMARY_VK_GROUP_ID '{primary_group_id_str}'. Проверка основной группы пропущена.", is_critical=True)
                except Exception as e_primary:
                    logger.exception(f"Непредвиденная ошибка при проверке основной группы {primary_group_id_str}: {e_primary}")

            if isinstance(secondary_groups, dict) and admin_chat_id:
                 logger.info(f"Начало проверки {len(secondary_groups)} вторичных групп...")
                 groups_processed_count = 0
                 for key, group_id_str in secondary_groups.items():
                     logger.info(f"Начало проверки вторичной группы: {key} (ID: {group_id_str})")
                     try:
                         group_id_int = int(group_id_str)
                         check_and_send_vk_posts(group_id_int, str(key), admin_chat_id)
                         groups_processed_count += 1
                         logger.info(f"Завершение проверки вторичной группы: {key} (ID: {group_id_str}).")
                         if groups_processed_count < len(secondary_groups):
                             logger.debug(f"Пауза {delay_between_groups} сек перед следующей вторичной группой...")
                             time.sleep(delay_between_groups)
                     except ValueError:
                         logger.error(f"Некорректный ID '{group_id_str}' для ключа '{key}' в SECONDARY_VK_GROUPS.")
                         send_error_to_admin(f"Ошибка конфигурации: Некорректный ID '{group_id_str}' для вторичной группы '{key}'. Группа пропущена.")
                     except Exception as e_secondary:
                         logger.exception(f"Непредвиденная ошибка при проверке вторичной группы {key} ({group_id_str}): {e_secondary}")
                 logger.info(f"Завершена проверка {groups_processed_count} из {len(secondary_groups)} вторичных групп.")
            elif not isinstance(secondary_groups, dict) and secondary_groups:
                 logger.warning("Формат SECONDARY_VK_GROUPS некорректен. Должен быть словарь.")
                 send_error_to_admin("Ошибка конфигурации: Неверный формат SECONDARY_VK_GROUPS.")
            elif not secondary_groups:
                 logger.info("Вторичные группы (SECONDARY_VK_GROUPS) не настроены.")

            if memory_handler.buffer:
                logger.info(f"Обнаружено {len(memory_handler.buffer)} ошибок в буфере. Отправка сводки админу...")
                send_error_summary_to_admin(list(memory_handler.buffer))
                memory_handler.buffer.clear()
            else:
                logger.debug("Буфер ошибок пуст, сводка не требуется.")

            loop_duration = time.time() - loop_start_time
            wait_time = max(0, check_interval - loop_duration)
            logger.info(f"--- Цикл проверки VK завершен за {loop_duration:.2f} сек. Следующий запуск через ~{wait_time:.0f} сек. ---")
            time.sleep(wait_time)

        except Exception as e_loop:
            logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА в основном цикле vk_check_loop: {e_loop}", exc_info=True)
            send_error_to_admin(f"КРИТИЧЕСКАЯ ОШИБКА в основном цикле проверки VK: {e_loop}. Бот может работать нестабильно.", is_critical=True)
            if memory_handler.buffer:
                logger.warning("Отправка накопленных ошибок перед аварийной паузой...")
                send_error_summary_to_admin(list(memory_handler.buffer)); memory_handler.buffer.clear()
            logger.info("Аварийная пауза 300 секунд после критической ошибки в цикле...")
            time.sleep(300)

if __name__ == '__main__':
    logger.info("================ ЗАПУСК БОТА ================")
    admin_chat_id = getattr(config, 'ADMIN_CHAT_ID', None)
    if admin_chat_id:
        try:
            bot.send_message(admin_chat_id, "🚀 Бот успешно запущен!", parse_mode=None)
            logger.info(f"Уведомление о запуске отправлено администратору ({admin_chat_id}).")
        except Exception as e_start:
            logger.error(f"Не удалось отправить уведомление о запуске администратору ({admin_chat_id}): {e_start}")
    else:
        logger.warning("ADMIN_CHAT_ID не указан в config.py. Уведомление о запуске не отправлено.")

    load_filter_words()

    for dir_path in [DOWNLOAD_DIR, PHOTO_DOWNLOAD_DIR]: 
        if not os.path.exists(dir_path):
            try:
                os.makedirs(dir_path)
                logger.info(f"Создана папка: {dir_path}")
            except OSError as e:
                logger.critical(f"Не удалось создать папку {dir_path}: {e}. Работа зависимых функций будет невозможна.")
                send_error_to_admin(f"Критическая ошибка: Не удалось создать папку {dir_path}.", is_critical=True)

    vk_thread = threading.Thread(target=vk_check_loop, name="VKCheckLoop", daemon=True)
    vk_thread.start()
    logger.info("Поток проверки постов VK запущен в фоновом режиме.")

    logger.info("Запуск основного цикла опроса Telegram (polling)...")
    retries = 0
    max_retries = 5
    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=30)
            logger.info("bot.polling завершился штатно.")
            break
        except requests.exceptions.ReadTimeout as e:
            logger.warning(f"Таймаут чтения от Telegram API: {e}. Перезапуск polling через 5 секунд...")
            time.sleep(5); retries = 0
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Ошибка соединения с Telegram API: {e}. Перезапуск polling через 60 секунд...")
            time.sleep(60); retries = 0
        except ApiTelegramException as e:
             logger.error(f"Ошибка Telegram API в polling (код {e.error_code}): {e}.")
             if "Unauthorized" in str(e) or e.error_code == 401:
                  logger.critical(f"Критическая ошибка авторизации Telegram (401): {e}. Неверный токен? Бот остановлен.")
                  send_error_to_admin(f"КРИТИЧЕСКАЯ ОШИБКА АВТОРИЗАЦИИ TELEGRAM (401): {str(e)}. Проверьте TELEGRAM_BOT_TOKEN. Бот остановлен.", is_critical=True)
                  break
             elif e.error_code == 409:
                  logger.warning(f"Конфликт polling (409): {e}. Возможно, запущен другой экземпляр бота? Перезапуск через 60 секунд...")
                  time.sleep(60); retries = 0
             else:
                  logger.warning(f"Неизвестная ошибка Telegram API ({e.error_code}). Пауза 30 секунд...")
                  time.sleep(30); retries += 1
        except Exception as e:
            logger.critical(f"КРИТИЧЕСКАЯ НЕПРЕДВИДЕННАЯ ОШИБКА в bot.polling: {e}", exc_info=True)
            send_error_to_admin(f"КРИТИЧЕСКАЯ НЕПРЕДВИДЕННАЯ ОШИБКА POLLING: {str(e)}. Бот остановлен.", is_critical=True)
            break

        if retries >= max_retries:
            logger.critical(f"Достигнуто максимальное количество ({max_retries}) быстрых перезапусков polling из-за ошибок API. Бот остановлен.")
            send_error_to_admin(f"Критическая ошибка: Polling перезапускался {max_retries} раз подряд из-за ошибок API. Бот остановлен.", is_critical=True)
            break

    logger.info("================ БОТ ОСТАНОВЛЕН ================")