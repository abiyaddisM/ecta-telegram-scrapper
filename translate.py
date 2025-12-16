import os
from dotenv import load_dotenv
import re
import json
import requests

load_dotenv()
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')  # Added API Key

AMHARIC_PATTERN = re.compile(r'[\u1200-\u137F]')  # Regex to detect Amharic chars

def is_amharic(text):
    """Checks if the text contains Ethiopic characters."""
    return bool(AMHARIC_PATTERN.search(text))


def translate_batch_with_gemini(paragraphs):
    """
    Sends a list of paragraphs to Gemini.
    Returns a list of translated strings (or null/None if original was not Amharic).
    """
    if not GEMINI_API_KEY:
        print("  [Warning] GEMINI_API_KEY not found. Skipping translation.")
        return [None] * len(paragraphs)

    # Optimization: If no Amharic in ANY paragraph, skip the API call entirely
    if not any(is_amharic(p) for p in paragraphs):
        return [None] * len(paragraphs)

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"

    # We send the data as a JSON string in the prompt to ensure structure
    prompt_data = json.dumps(paragraphs, ensure_ascii=False)

    system_instruction = (
        "You are a precise translator. You will receive a JSON array of strings. "
        "For each string: If it contains Amharic text, translate it to English. "
        "If it does NOT contain Amharic (e.g. it is already English), return null. "
        "Return strictly a JSON array of strings (or nulls) that matches the length and order of the input array."
    )

    payload = {
        "contents": [{
            "parts": [{"text": f"{system_instruction}\n\nInput JSON:\n{prompt_data}"}]
        }],
        "generationConfig": {
            "response_mime_type": "application/json"
        }
    }

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        result = response.json()

        # Parse the JSON text response from Gemini
        generated_text = result['candidates'][0]['content']['parts'][0]['text']
        translated_array = json.loads(generated_text)

        if len(translated_array) != len(paragraphs):
            print(f"  [Translation Error] Mismatch: Input {len(paragraphs)} vs Output {len(translated_array)}")
            return [None] * len(paragraphs)

        return translated_array

    except Exception as e:
        print(f"  [Translation Failed] {e}")
        return [None] * len(paragraphs)

