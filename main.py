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
from telethon import TelegramClient
from slug import generate_slug
from upload_to_bunny import upload_file_to_bunny, UploadProps

# --- INITIALIZATION ---
load_dotenv()

api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
session_name = os.getenv('SESSION_NAME')
API_BASE_URL = os.getenv('API_BASE_URL')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

UPLOAD_TO_SERVER = True
CHECK_INTERVAL_SECONDS = 600
LOOKBACK_MINUTES = 10
MAX_IMAGES_PER_GROUP = 12
MAX_TIME_DIFF_SECONDS = 120

CHANNELS_CONFIG = [
 {

"channel_username": "t.me/ECTAuthority",

"default_thumbnail": "eefd9c88-9d71-4fbe-8cd7-0d0f43dabd04.jpeg",

"source": "ECTA"

},
    {
        "channel_username": "t.me/motri_gov_et",
        "default_thumbnail": "155e1d47-4d84-487b-8b2e-7e70ebeb54ca.png",
        "source": "Motri"
    }
]

client = TelegramClient(session_name, api_id, api_hash)
AMHARIC_PATTERN = re.compile(r'[\u1200-\u137F]')
URL_PATTERN = re.compile(r'https?://\S+|www\.\S+')


# --- CORE AI LOGIC ---

def is_amharic(text):
    return bool(AMHARIC_PATTERN.search(text))


def call_gemini_ai(prompt, system_instruction, is_json=False):
    """
    General purpose Gemini caller for classification and title generation.
    Does NOT skip based on Amharic detection.
    """
    if not GEMINI_API_KEY:
        return None

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"

    payload = {
        "contents": [{
            "parts": [{"text": f"{system_instruction}\n\nInput:\n{prompt}"}]
        }],
        "generationConfig": {
            "response_mime_type": "application/json" if is_json else "text/plain"
        }
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        return result['candidates'][0]['content']['parts'][0]['text'].strip()
    except Exception as e:
        print(f"      [Gemini AI Error] {e}")
        return None


def is_export_news_worthy(full_text):
    """Checks if news is relevant to export trade."""
    system_instr = "You are a trade analyst. Respond with ONLY 'YES' or 'NO'."
    prompt = f"Is this news relevant to Ethiopia's export trade, logistics, or economy? Content: {full_text[:1500]}"

    result = call_gemini_ai(prompt, system_instr)
    return result and "YES" in result.upper()


def generate_ai_titles(full_text):
    """Generates Amharic and English titles in JSON format."""
    system_instr = "Generate a short title in Amharic and English. Return strictly JSON: {\"title\": \"...\", \"otherTitle\": \"...\"}"
    prompt = f"Content: {full_text[:2000]}"

    result = call_gemini_ai(prompt, system_instr, is_json=True)
    try:
        return json.loads(result) if result else {"title": "News Update", "otherTitle": "News Update"}
    except:
        return {"title": "News Update", "otherTitle": "News Update"}


def translate_batch_with_gemini(paragraphs):
    """Body translation logic (keeps the Amharic-only optimization)."""
    if not GEMINI_API_KEY or not any(is_amharic(p) for p in paragraphs):
        return [None] * len(paragraphs)

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    system_instruction = (
        "You are a translator. For each JSON string: If it contains Amharic, translate to English. "
        "If not, return null. Return a JSON array of same length."
    )

    payload = {
        "contents": [{"parts": [
            {"text": f"{system_instruction}\n\nInput JSON:\n{json.dumps(paragraphs, ensure_ascii=False)}"}]}],
        "generationConfig": {"response_mime_type": "application/json"}
    }

    try:
        response = requests.post(url, json=payload, timeout=60)
        return json.loads(response.json()['candidates'][0]['content']['parts'][0]['text'])
    except:
        return [None] * len(paragraphs)


# --- TELEGRAM & PROCESSING LOGIC ---

def generate_random_id(length=12):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


async def download_media(msg):
    os.makedirs("downloads", exist_ok=True)
    path = await msg.download_media(file=os.path.join("downloads", f"{msg.id}_{generate_random_id(4)}"))
    return {"url": path, "name": os.path.basename(path) if path else "", "status": "complete" if path else "failed"}


async def process_batch(config):
    target_channel = config['channel_username']
    default_thumb = config['default_thumbnail']
    source_name = config['source']

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Checking: {target_channel}")

    try:
        # Get the channel entity
        channel = await client.get_entity(target_channel)

        # Calculate cutoff time
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)

        # 1. IMMEDIATE SKIP IF NO RECENT MESSAGES
        msgs = []
        async for m in client.iter_messages(channel, limit=50):
            if m.date < cutoff:
                break
            msgs.append(m)

        if not msgs:
            print(f"    [SKIP] No new messages in the last {LOOKBACK_MINUTES} minutes.")
            return  # This prevents the script from getting stuck on inactive channels

        msgs.reverse()
    except Exception as e:
        print(f"  [Error accessing {target_channel}] {e}")
        return

    # --- Grouping and Filtering Logic ---
    groups, current = [], None
    for m in msgs:
        if not m.message and not (m.photo or m.document):
            continue

        # Logic to group related messages (captions + images)
        if not current or (m.date - current["end_date"]).total_seconds() > MAX_TIME_DIFF_SECONDS:
            if current: groups.append(current)
            current = {
                "body": m.message or "",
                "ids": [m.id],
                "end_date": m.date,
                "media": [m] if (m.photo or m.document) else [],
                "start_date": m.date
            }
        else:
            current["ids"].append(m.id)
            current["end_date"] = m.date
            if m.message: current["body"] += f"\n\n{m.message}"
            if (m.photo or m.document): current["media"].append(m)

    if current:
        groups.append(current)

    for g in groups:
        if len(g["body"].strip()) < 20:
            continue

        # 2. Worthiness Check
        if not is_export_news_worthy(g["body"]):
            print(f"    [SKIP] Not relevant: {g['ids']}")
            continue

        # 3. AI Title Generation
        post_id = generate_random_id(12)
        title_obj = generate_ai_titles(g["body"])
        print(f"    [MATCH] Title: {title_obj['title']}")

        # 4. Media Handling (with Fixed File Paths)
        gallery = []
        for m in g["media"][:MAX_IMAGES_PER_GROUP]:
            entry = await download_media(m)
            local_path = entry.get('url')  # Full path: e.g., 'downloads/123.jpg'

            if local_path and os.path.exists(local_path):
                if UPLOAD_TO_SERVER:
                    try:
                        with open(local_path, "rb") as f:
                            f.filename = entry['name']
                            upload_res = upload_file_to_bunny(UploadProps(file=f, table_name="post", ref_id=post_id))
                            entry['url'] = upload_res.file_url

                        # CLEANUP: Remove the local file using the correct path
                        os.remove(local_path)
                    except Exception as e:
                        print(f"    [Upload Error] {e}")
                gallery.append(entry)

        # 5. Body Translation
        paras = [p.strip() for p in g["body"].split('\n') if p.strip()]
        trans = translate_batch_with_gemini(paras)
        blocks = [
            {"id": generate_random_id(12), "type": "paragraph", "data": {"text": p, "englishText": trans[i] or ""}} for
            i, p in enumerate(paras)]

        payload = {
            "id": post_id,
            "title": title_obj,
            "slug": generate_slug(title_obj["title"], post_id),
            "source": source_name,
            "body": {"time": int(time.time() * 1000), "blocks": blocks, "version": "2.31.0"},
            "imageUrl": gallery[0]['url'] if gallery else default_thumb,
            "galleryImages": gallery
        }

        # 6. Upload to API
        if UPLOAD_TO_SERVER and API_BASE_URL:
            try:
                requests.put(API_BASE_URL.replace("[id]", post_id), json=payload, timeout=10)
                print(f"    [SUCCESS] Uploaded {post_id}")
            except Exception as e:
                print(f"    [API Error] {e}")


async def run_forever():
    while True:
        for config in CHANNELS_CONFIG:
            await process_batch(config)
            await asyncio.sleep(5)
        print(f"Cycle complete. Sleeping for {CHECK_INTERVAL_SECONDS}s...")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == '__main__':
    with client: client.loop.run_until_complete(run_forever())