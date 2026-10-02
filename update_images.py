import os
import json
import re

JSON_FILE = "players.json"
IMAGES_DIR = "images"
PATH_PREFIX = "images/"
OVERWRITE_EXISTING = True

IGNORED_FILES = {"thumbs.db", ".ds_store", "desktop.ini"}

def normalize_name(name):
    return re.sub(r'[^a-z0-9]', '', name.lower())

def detect_and_fix_html_file(filepath, filename):
    """Checks if a .html/.htm file is a real image in disguise or an HTML page."""
    with open(filepath, "rb") as f:
        header = f.read(512)

    # Check magic bytes for real images saved with .html extension
    new_ext = None
    if header.startswith(b"\xff\xd8\xff"):
        new_ext = ".jpg"
    elif header.startswith(b"\x89PNG\r\n\x1a\n"):
        new_ext = ".png"
    elif header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        new_ext = ".webp"
    elif header.startswith(b"GIF87a") or header.startswith(b"GIF89a"):
        new_ext = ".gif"

    if new_ext:
        base, _ = os.path.splitext(filename)
        new_filename = base + new_ext
        new_filepath = os.path.join(IMAGES_DIR, new_filename)
        os.rename(filepath, new_filepath)
        print(f"[Fixed Extension] Renamed '{filename}' -> '{new_filename}'")
        return f"{PATH_PREFIX}{new_filename}"

    # If it is actual HTML text, try to extract the image link inside it
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        og_match = re.search(r'property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', content, re.I) or \
                   re.search(r'content=["\']([^"\']+)["\']\s+property=["\']og:image["\']', content, re.I)
        img_match = re.search(r'<img[^>]+src=["\'](https?://[^"\']+)["\']', content, re.I)

        if og_match:
            print(f"[HTML Webpage] Extracted og:image URL from '{filename}'")
            return og_match.group(1)
        elif img_match:
            print(f"[HTML Webpage] Extracted <img> URL from '{filename}'")
            return img_match.group(1)
        else:
            print(f"[WARNING] '{filename}' is an HTML file, not an image! Re-download this image.")
    except Exception:
        pass

    return f"{PATH_PREFIX}{filename}"

def update_player_images():
    if not os.path.exists(IMAGES_DIR):
        print(f"Error: Folder '{IMAGES_DIR}' not found.")
        return

    exact_map = {}
    norm_map = {}

    for filename in os.listdir(IMAGES_DIR):
        if filename.lower() in IGNORED_FILES or filename.startswith("."):
            continue
        filepath = os.path.join(IMAGES_DIR, filename)
        if os.path.isdir(filepath):
            continue

        name_part, _ = os.path.splitext(filename)
        exact_map[name_part.strip().lower()] = filename
        norm_map[normalize_name(name_part)] = filename

    with open(JSON_FILE, "r", encoding="utf-8") as f:
        players = json.load(f)

    matched_count = 0
    missing_players = []

    for player in players:
        player_name = player.get("name", "").strip()
        if not OVERWRITE_EXISTING and player.get("img"):
            continue

        matched_file = exact_map.get(player_name.lower()) or norm_map.get(normalize_name(player_name))

        if matched_file:
            _, ext = os.path.splitext(matched_file)
            full_path = os.path.join(IMAGES_DIR, matched_file)
            if ext.lower() in {".html", ".htm"}:
                player["img"] = detect_and_fix_html_file(full_path, matched_file)
            else:
                player["img"] = f"{PATH_PREFIX}{matched_file}"
            matched_count += 1
        else:
            missing_players.append(player_name)

    with open(JSON_FILE, "w", encoding="utf-8") as f:
        json.dump(players, f, indent=2, ensure_ascii=False)

    print(f"\nSuccessfully updated {matched_count}/{len(players)} players in '{JSON_FILE}'.")
    if missing_players:
        print(f"Missing images ({len(missing_players)}): {missing_players}")

if __name__ == "__main__":
    update_player_images()