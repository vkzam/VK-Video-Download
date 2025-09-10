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
# This function will be called by yt-dlp to report download progress
def my_hook(d):
    """yt-dlp progress hook to print status to console."""
    if d['status'] == 'downloading':
        # Get essential info, providing defaults if keys are missing
        filename = d.get('filename', 'unknown file')
        percent_str = d.get('_percent_str', '0.0%').strip()
        speed_str = d.get('_speed_str', 'N/A').strip()
        eta_str = d.get('_eta_str', 'N/A').strip()

        # Clean percentage string (remove potential ANSI codes)
        percent_str_clean = ''.join(c for c in percent_str if c.isprintable())

        # Print progress on the same line
        # Use a slice of the filename to prevent overly long lines
        short_filename = os.path.basename(filename)
        if len(short_filename) > 40:
            short_filename = "..." + short_filename[-37:]

        # Print progress, overwriting the previous line (\r)
        sys.stdout.write(
            f"\rDownloading \"{short_filename}\": {percent_str_clean} | Speed: {speed_str} | ETA: {eta_str}   "
        )
        sys.stdout.flush() # Ensure the output is displayed immediately

    elif d['status'] == 'finished':
        filename = d.get('filename', 'unknown file')
        short_filename = os.path.basename(filename)
        # Print a newline after finishing to avoid overwriting the final status
        sys.stdout.write(f"\nFinished downloading \"{short_filename}\".\n") # Removed "Processing..." as merging is less likely
        sys.stdout.flush()

    elif d['status'] == 'error':
        filename = d.get('filename', 'unknown file')
        short_filename = os.path.basename(filename)
        sys.stdout.write(f"\nError downloading \"{short_filename}\".\n")
        sys.stdout.flush()

# --- Download Function ---
def download_video(video_url, output_dir="downloads"):
    """Downloads a single video from the given URL using yt-dlp, prioritizing formats that don't require merging."""
    print(f"\nProcessing URL: {video_url}")

    # --- Create Output Directory ---
    if not os.path.exists(output_dir):
        try:
            print(f"Creating directory: {output_dir}")
            os.makedirs(output_dir)
        except OSError as e:
            print(f"Error: Could not create directory '{output_dir}'. {e}")
            return # Stop if directory can't be created

    # --- yt-dlp Options ---
    ydl_opts = {
        'outtmpl': os.path.join(output_dir, '%(title)s.mp4'), # Save as .mp4 in 'downloads' folder

        # --- FORMAT SELECTION CHANGE ---
        # The 'format' option is REMOVED.
        # This lets yt-dlp use its default format selection, which often prefers
        # pre-merged formats (video+audio together) if available.
        # This avoids the need for FFmpeg for merging in many cases, similar to the original script.
        # Note: This might result in lower quality downloads compared to merging bestvideo+bestaudio.
        # 'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best', # <--- This line is removed/commented out

        # Alternative format option prioritizing pre-merged MP4s:
        # 'format': 'best[ext=mp4][vcodec!=none][acodec!=none]/best[vcodec!=none][acodec!=none]',

        'quiet': False,         # Shows yt-dlp's own messages
        'progress_hooks': [my_hook], # Use our custom progress function
        'noplaylist': True,     # Download only the video, not the playlist
        'noprogress': True,     # Disable yt-dlp's default progress bar
        # Postprocessor for metadata might still rely on FFmpeg, commenting out to be safe
        # 'postprocessors': [{
        #     'key': 'FFmpegMetadata',
        #     'add_metadata': True,
        # }],
        # 'verbose': True,      # Uncomment for detailed debugging output from yt-dlp
    }

    # --- Execute Download ---
    try:
        print("Starting download process (prioritizing non-merging formats)...")
        # Using 'with' ensures yt-dlp resources are cleaned up properly
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Perform the download
            ydl.download([video_url])
        # Success message is handled by the 'finished' status in my_hook

    except yt_dlp.utils.DownloadError as e:
        # Handle errors specifically related to downloading
        # Check if the error is specifically about FFmpeg missing, even with changed format options
        if 'ffmpeg not found' in str(e).lower() or 'ffmpeg is not installed' in str(e).lower():
             print(f"\nError downloading {video_url}. Reason: {e}")
             print("FFmpeg might still be required for processing or remuxing this specific video format.")
        else:
            print(f"\nError downloading {video_url}. Reason: {e}")
    except Exception as e:
        # Handle other unexpected errors during the process
        print(f"\nAn unexpected error occurred while processing {video_url}: {e}")

# --- Main Execution Block ---
if __name__ == "__main__":
    print("VK/RU Video Downloader (CLI Version - No FFmpeg Priority)")
    print("-" * 30)

    # --- Get URL(s) from User ---
    url_input = input("Enter the video URL (or multiple URLs separated by commas):\n> ")

    if not url_input:
        print("No URL entered. Exiting.")
        sys.exit(1) # Exit with an error code

    # --- Process URLs ---
    video_urls = [url.strip() for url in url_input.split(',') if url.strip()]

    if not video_urls:
        print("No valid URLs found after processing input. Exiting.")
        sys.exit(1)

    print(f"\nFound {len(video_urls)} URL(s) to download.")

    # --- Download Concurrently using Threads ---
    threads = []
    for url in video_urls:
        # Create a thread for each download task
        thread = threading.Thread(target=download_video, args=(url,))
        threads.append(thread)
        thread.start() # Start the thread

    # Wait for all threads to complete before exiting the main script
    for thread in threads:
        thread.join()
    # --- End Concurrency ---

    print("\n" + "-" * 30)
    print("All download tasks finished.")
    sys.exit(0) # Exit successfully
