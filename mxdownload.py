#!/usr/bin/env python3
import os
import sys
import yt_dlp
import threading # Keep threading if you want concurrent downloads

# Original comments/examples:
# https://vk.com/video-87011294_456249654 | example for vk.com
# https://vkvideo.ru/video-50804569_456239864 | example for vkvideo.ru
# https://my.mail.ru/v/hi-tech_mail/video/_groupvideo/437.html | example for my.mail.ru
# https://rutube.ru/video/a16f1e575e114049d0e4d04dc7322667/ | example for rutube.ru
# FromRussiaWithLove | Mons (https://github.com/blyamur/VK-Video-Download/) | ver. 1.5 CLI Mod | "non-commercial use only, for personal use"

# --- Progress Hook ---
# Эта функция будет вызываться yt-dlp для отображения прогресса скачивания
def my_hook(d):
    """yt-dlp progress hook to print status to console."""
    if d['status'] == 'downloading':
        # Получаем основную информацию, предоставляя значения по умолчанию, если ключи отсутствуют
        filename = d.get('filename', 'unknown file')
        percent_str = d.get('_percent_str', '0.0%').strip()
        speed_str = d.get('_speed_str', 'N/A').strip()
        eta_str = d.get('_eta_str', 'N/A').strip()

        # Очищаем строку процента (удаляем возможные ANSI коды)
        percent_str_clean = ''.join(c for c in percent_str if c.isprintable())

        # Выводим прогресс в той же строке
        # Используем срез имени файла, чтобы предотвратить слишком длинные строки
        short_filename = os.path.basename(filename)
        if len(short_filename) > 40:
            short_filename = "..." + short_filename[-37:]

        # Выводим прогресс, перезаписывая предыдущую строку (\r)
        sys.stdout.write(
            f"\rDownloading \"{short_filename}\": {percent_str_clean} | Speed: {speed_str} | ETA: {eta_str}   "
        )
        sys.stdout.flush() # Гарантируем немедленное отображение вывода

    elif d['status'] == 'finished':
        filename = d.get('filename', 'unknown file')
        short_filename = os.path.basename(filename)
        # Выводим новую строку после завершения, чтобы не перезаписать финальный статус
        # Проверяем, был ли файл действительно скачан (а не уже существовал)
        total_bytes = d.get('total_bytes')
        downloaded_bytes = d.get('downloaded_bytes')
        # Проверяем, скачан ли файл или он уже существовал
        if d.get('already_downloaded'):
             sys.stdout.write(f"\n\"{short_filename}\" already downloaded.\n")
        elif total_bytes and downloaded_bytes and total_bytes == downloaded_bytes:
             sys.stdout.write(f"\nFinished downloading \"{short_filename}\".\n")
        else:
             # Если статус 'finished', но файл не был скачан (например, только извлечение информации)
             sys.stdout.write(f"\nFinished processing \"{short_filename}\".\n")
        sys.stdout.flush()

    elif d['status'] == 'error':
        filename = d.get('filename', 'unknown file')
        short_filename = os.path.basename(filename)
        sys.stdout.write(f"\nError downloading \"{short_filename}\".\n")
        sys.stdout.flush()

# --- Download Function ---
def download_video(video_url, output_dir="downloads"):
    """Downloads a single video from the given URL using yt-dlp."""
    print(f"\nProcessing URL: {video_url}")

    # --- Create Output Directory ---
    if not os.path.exists(output_dir):
        try:
            print(f"Creating directory: {output_dir}")
            os.makedirs(output_dir)
        except OSError as e:
            print(f"Error: Could not create directory '{output_dir}'. {e}")
            return # Останавливаемся, если директорию создать не удалось

    # --- yt-dlp Options ---
    ydl_opts = {
        # Сохраняем в папку 'downloads', используя заголовок видео как имя файла.
        # yt-dlp автоматически добавит расширение (.mp4, .webm, etc.)
        'outtmpl': os.path.join(output_dir, '%(title)s.%(ext)s'),
        # *** ИЗМЕНЕНО ***
        # Упрощенная спецификация формата:
        # Сначала ищем лучший формат MP4 (часто содержит и видео, и аудио).
        # Если MP4 нет, выбираем просто лучший доступный формат (видео+аудио).
        # Это должно избежать ошибки 'Invalid filter specification' и по-прежнему
        # стараться избегать необходимости слияния через ffmpeg.
        'format': 'best[ext=mp4]/best',
        'quiet': False,         # Показывать собственные сообщения yt-dlp
        'progress_hooks': [my_hook], # Использовать нашу функцию для отображения прогресса
        'noplaylist': True,     # Важно: Скачивать только видео, а не плейлист
        'noprogress': True,     # Отключить стандартный индикатор прогресса yt-dlp
        # 'verbose': True,      # Раскомментируйте для детального вывода отладки от yt-dlp
    }

    # --- Execute Download ---
    try:
        print("Starting download process...")
        # Использование 'with' гарантирует правильное освобождение ресурсов yt-dlp
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Выполняем скачивание
            ydl.download([video_url])
        # Сообщение об успехе обрабатывается статусом 'finished' в my_hook

    except yt_dlp.utils.DownloadError as e:
        # Обрабатываем ошибки, специфичные для скачивания
        print(f"\nError downloading {video_url}. Reason: {e}")
        # Добавляем проверку на сообщение об отсутствии ffmpeg, хотя мы пытаемся его избежать
        if 'ffmpeg' in str(e).lower() or 'ffprobe' in str(e).lower():
            print("Note: The selected format might still require ffmpeg for processing or extraction.")
            print("If errors persist, installing ffmpeg is the most reliable solution.")
            print("Alternatively, try a different video URL if possible.")
    except Exception as e:
        # Обрабатываем другие неожиданные ошибки
        print(f"\nAn unexpected error occurred while processing {video_url}: {e}")

# --- Main Execution Block ---
if __name__ == "__main__":
    print("VK/RU Video Downloader (CLI Version - No Merge)")
    print("-" * 30)

    # --- Get URL(s) from User ---
    url_input = input("Enter the video URL (or multiple URLs separated by commas):\n> ")

    if not url_input:
        print("No URL entered. Exiting.")
        sys.exit(1) # Выход с кодом ошибки

    # --- Process URLs ---
    # Разделяем введенную строку по запятым, убираем пробелы по краям
    # и отфильтровываем пустые строки, которые могут появиться из-за лишних запятых.
    video_urls = [url.strip() for url in url_input.split(',') if url.strip()]

    if not video_urls:
        print("No valid URLs found after processing input. Exiting.")
        sys.exit(1)

    print(f"\nFound {len(video_urls)} URL(s) to download.")

    # --- Download Concurrently using Threads (Опция для параллельной загрузки) ---
    threads = []
    for url in video_urls:
        # Создаем поток для каждой задачи скачивания
        thread = threading.Thread(target=download_video, args=(url,))
        threads.append(thread)
        thread.start() # Запускаем поток

    # Ждем завершения всех потоков перед выходом из основного скрипта
    for thread in threads:
        thread.join()
    # --- End Option 2 ---

    print("\n" + "-" * 30)
    print("All download tasks finished.")
    sys.exit(0) # Успешный выход
