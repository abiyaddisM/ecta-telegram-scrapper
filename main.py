from telethon import TelegramClient
import os
import json
import random
import string
import time
import re
import asyncio
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from slug import generate_slug
from upload_to_bunny import upload_file_to_bunny, UploadProps

load_dotenv()

api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
session_name = os.getenv('SESSION_NAME')

channel_username = os.getenv('CHANNEL_USERNAME')

UPLOAD_TO_SERVER = True

API_BASE_URL = os.getenv('API_BASE_URL')

CHECK_INTERVAL_SECONDS = 600
LOOKBACK_MINUTES = 10
MAX_IMAGES_PER_GROUP = 10
MAX_TIME_DIFF_SECONDS = 120
DEFAULT_THUMBNAIL = os.getenv('DEFAULT_THUMBNAIL')

client = TelegramClient(session_name, api_id, api_hash)

URL_PATTERN = re.compile(r'https?://\S+|www\.\S+')


def generate_random_id(length=12):
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def is_valid_news_group(group):
   
    for msg in group['media_msgs']:
        if msg.video or (msg.document and msg.document.mime_type.startswith('video/')):
            return False

    text = group['body'].strip()

    if not text and not group['media_msgs']:
        return False

    if text:
        if len(text) < 20:
            return False

        text_without_urls = URL_PATTERN.sub('', text).strip()
        if not text_without_urls:
            return False

        if len(text_without_urls) < 10:
            return False

    return True


async def download_media_entry(msg):

    random_suffix = generate_random_id(4)
    file_prefix = f"{msg.id}_{random_suffix}"

    download_dir = "downloads"
    os.makedirs(download_dir, exist_ok=True)

    mime_type = "application/octet-stream"
    original_name = f"photo_{msg.date.strftime('%Y-%m-%d_%H-%M-%S')}.jpg"
    file_size = 0

    if msg.photo:
        mime_type = "image/jpeg"
    elif msg.document:
        mime_type = msg.document.mime_type
        file_size = msg.document.size
        for attr in msg.document.attributes:
            if hasattr(attr, 'file_name') and attr.file_name:
                original_name = attr.file_name

    path = await msg.download_media(file=os.path.join(download_dir, file_prefix))

    if path and os.path.exists(path):
        file_size = os.path.getsize(path)
    else:
        path = None

    return {
        "url": path,
        "name": original_name,
        "size": file_size,
        "type": mime_type,
        "error": "" if path else "Download failed",
        "status": "complete" if path else "failed"
    }


async def process_batch():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Checking for new messages...")
    print(f"  > Mode: {'PRODUCTION (Upload & API)' if UPLOAD_TO_SERVER else 'TESTING (Local Only)'}")

    channel = await client.get_entity(channel_username)

    scan_limit = 100
    raw_msgs = []
    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)

    async for msg in client.iter_messages(channel, limit=scan_limit):
        if msg.date < cutoff_time:
            break
        raw_msgs.append(msg)

    if not raw_msgs:
        print("  No new messages in the last window.")
        return

    raw_msgs.reverse()

    groups = []
    current = None

    def start_new_group(msg, has_text):
        return {
            "body": msg.message.strip() if has_text else "",
            "message_ids": [msg.id],
            "start_date": msg.date,
            "end_date": msg.date,
            "media_msgs": [msg] if (msg.photo or msg.document) else [],
            "anchor_time": msg.date,
            "grouped_id": msg.grouped_id
        }

    for msg in raw_msgs:
        has_text = bool(msg.message and msg.message.strip())
        has_media = bool(msg.photo or msg.document)
        msg_time = msg.date

        if not has_text and not has_media:
            continue

        if current is None:
            current = start_new_group(msg, has_text)
            continue

        should_group = False

        time_diff = (msg_time - current["end_date"]).total_seconds()

        if msg.grouped_id is not None and msg.grouped_id == current["grouped_id"]:
            should_group = True
        elif 0 <= time_diff <= MAX_TIME_DIFF_SECONDS:
            should_group = True

        if should_group:
            current["message_ids"].append(msg.id)
            current["end_date"] = msg_time

            if has_text:
                if current["body"]:
                    current["body"] += f"\n\n{msg.message.strip()}"
                else:
                    current["body"] = msg.message.strip()

            if has_media:
                current["media_msgs"].append(msg)

            if current["grouped_id"] is None and msg.grouped_id is not None:
                current["grouped_id"] = msg.grouped_id
        else:
            groups.append(current)
            current = start_new_group(msg, has_text)

    if current:
        groups.append(current)

    valid_groups = []
    for g in groups:
        if g['end_date'] < cutoff_time:
            continue
        if is_valid_news_group(g):
            valid_groups.append(g)

    if not valid_groups:
        print("  No valid news groups found.")
        return

    print(f"  Found {len(valid_groups)} valid groups to process.")

    final_output = []

    for g in valid_groups:
        post_id = generate_random_id(12)
        print(f"  Processing Group ID: {post_id} (Msg IDs: {g['message_ids']})")

        gallery_images = []
        media_to_download = g["media_msgs"][:MAX_IMAGES_PER_GROUP]

        for media_msg in media_to_download:
            entry = await download_media_entry(media_msg)
            local_path = entry['url']

            if local_path and os.path.exists(local_path):
                if UPLOAD_TO_SERVER:
                    try:
                        with open(local_path, "rb") as f:
                            f.filename = entry['name']
                            props = UploadProps(
                                file=f,
                                table_name="post",
                                ref_id=post_id,
                            )
                            result = upload_file_to_bunny(props)
                            entry['url'] = result.file_url

                        os.remove(local_path)

                    except Exception as e:
                        print(f"    [Error] Upload failed for {entry['name']}: {e}")
                        entry['error'] = str(e)
                        entry['status'] = "failed"
                else:
                    entry['url'] = os.path.abspath(local_path)
                    print(f"    [Test] Saved locally: {entry['name']}")

            gallery_images.append(entry)

        thumbnail_url = DEFAULT_THUMBNAIL

        if gallery_images and len(gallery_images) > 0:
            thumbnail_url = gallery_images[0]['url']

        raw_text = g["body"]
        blocks = []
        title = ""
        post_slug = ""

        if raw_text:
            paragraphs = [line.strip() for line in raw_text.split('\n') if line.strip()]

            if paragraphs:
                title = paragraphs[0]
                post_slug = generate_slug(title, post_id)

                for p in paragraphs:
                    blocks.append({
                        "id": generate_random_id(12),
                        "type": "paragraph",
                        "data": {"text": p}
                    })

        body_structure = {
            "time": int(time.time() * 1000),
            "blocks": blocks,
            "version": "2.31.0"
        }

        final_group = {
            "id": post_id,
            "title": title,
            "slug": post_slug,
            "body": body_structure,
            "imageUrl": thumbnail_url,
            "metadata": {
                "channel_id": channel.id,
                "telegram_message_ids": g["message_ids"],
                "start_date": g["start_date"].isoformat(),
                "end_date": g["end_date"].isoformat(),
                "media_count_total": len(g["media_msgs"]),
                "media_count_processed": len(gallery_images)
            },
            "galleryImages": gallery_images
        }
        final_output.append(final_group)

        if UPLOAD_TO_SERVER:
            if API_BASE_URL:
                try:
                    api_target_url = API_BASE_URL.replace("[id]", post_id)

                    payload = {
                        "title": title,
                        "body": body_structure,
                        "galleryImages": gallery_images,
                        "imageUrl": thumbnail_url,
                        "slug": post_slug,
                        "id": post_id
                    }

                    print(f"    Uploading to API: {api_target_url}")
                    response = requests.put(api_target_url, json=payload)

                    if response.status_code in [200, 201]:
                        print(f"    [SUCCESS] API Upload complete for {post_id}")
                    else:
                        print(f"    [FAILURE] API Status {response.status_code}: {response.text}")

                except Exception as e:
                    print(f"    [ERROR] API Request failed: {e}")
        else:
            print(f"    [Test] Skipping API Upload for {post_id}")

    if final_output:
        mode_label = "LIVE" if UPLOAD_TO_SERVER else "TEST"
        filename = f"batch_{mode_label}_{int(time.time())}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(final_output, f, ensure_ascii=False, indent=2)
        print(f"  Saved {mode_label} backup to {filename}")


async def run_forever():
    print("--- STARTING PERPETUAL SCRAPER ---")
    print(f"Checking every {CHECK_INTERVAL_SECONDS}s. Grouping window: {MAX_TIME_DIFF_SECONDS}s.")

    while True:
        try:
            await process_batch()
        except Exception as e:
            print(f"[Error] An error occurred in the loop: {e}")

        print(f"Sleeping for {CHECK_INTERVAL_SECONDS}s...")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == '__main__':
    client.start()
    with client:
        client.loop.run_until_complete(run_forever())