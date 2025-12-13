import os
import uuid
import requests
from io import BytesIO
from typing import Iterable, Optional

from PIL import Image



BUNNY_UPLOAD_ENDPOINT = os.getenv("BUNNY_UPLOAD_ENDPOINT", )

IMAGE_EXTS = {"jpg", "jpeg", "png", "gif", "webp"}
MAX_FILE_SIZE = 10 * 1024 * 1024


class UploadProps:
    """
    file: a file-like object opened in 'rb' with .read()
          If it doesn't have .filename, pass original_name to upload_file_to_bunny()
    table_name: the table you store metadata in (not used here; stub for parity with JS)
    ref_id: foreign key or other reference (not used here; stub for parity with JS)
    """
    def __init__(self, file, table_name: str, ref_id: str, bucket = None, folder = None):
        self.file = file
        self.bucket = bucket
        self.table_name = table_name
        self.ref_id = ref_id
        self.folder = folder


class UploadResult:
    def __init__(self, file_url: str = "", blur_url: str = "", error: str = ""):
        self.file_url = file_url
        self.blur_url = blur_url
        self.error = error

    def to_dict(self) -> dict:
        return {"fileUrl": self.file_url, "blurUrl": self.blur_url, "error": self.error}



def _infer_extension(filename: str) -> str:
    return (filename.rsplit(".", 1)[-1] if "." in filename else "").lower()


def _save_compressed(img: Image.Image, extension: str) -> bytes:
    """
    Compress image while preserving the file format where sensible.
    JPEG/WEBP -> use quality; PNG -> optimize; GIF -> leave as is.
    """
    out = BytesIO()
    fmt_map = {"jpg": "JPEG", "jpeg": "JPEG", "png": "PNG", "webp": "WEBP", "gif": "GIF"}
    fmt = fmt_map.get(extension, "JPEG")

    save_kwargs = {}
    if fmt in ("JPEG", "WEBP"):
        save_kwargs["quality"] = 70
        save_kwargs["optimize"] = True
    elif fmt == "PNG":
        save_kwargs["optimize"] = True

    img.convert("RGB" if fmt in ("JPEG", "WEBP") else img.mode).save(out, format=fmt, **save_kwargs)
    return out.getvalue()


def create_blurred_version(file_bytes: bytes) -> bytes:
    """
    Create a very small blur/placeholder (~20px longest side) as JPEG (tiny).
    """
    try:
        img = Image.open(BytesIO(file_bytes))
    except Exception as e:
        raise ValueError(f"Failed to load image: {e}")

    max_side = 20
    scale = max_side / max(img.width, img.height)
    new_w = max(1, int(img.width * scale))
    new_h = max(1, int(img.height * scale))

    img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)

    out = BytesIO()
    img.convert("RGB").save(out, format="JPEG", quality=10, optimize=True)
    return out.getvalue()


def upload_to_bunny(file_bytes: bytes, path: str, original_name: Optional[str] = None) -> bool:
    """
    Posts to your Next.js route with BOTH:
      - multipart 'file' (bytes)
      - form field 'filename' (the remote Bunny path)
    """
    files = {
        "file": (original_name or "upload.bin", file_bytes, "application/octet-stream"),
    }
    data = {"filename": path}

    res = requests.post(BUNNY_UPLOAD_ENDPOINT, files=files, data=data, timeout=60)
    return res.ok



def upload_file_to_bunny(props: UploadProps, original_name: Optional[str] = None) -> UploadResult:
    """
    Mirrors your JS behavior:
      - Generates uuid
      - If image: compress + blur; upload two variants under image/max and image/blur
      - If non-image: upload to document/
      - Enforces 10MB limit for non-images (images are recompressed)
    Returns UploadResult(file_url="<uuid>.<ext>", blur_url="", error="")
    """
    file_bytes = props.file.read()

    file_name = (
        original_name
        or getattr(props.file, "filename", None)
        or getattr(props.file, "name", None)
        or "upload.bin"
    )

    extension = _infer_extension(file_name)
    uid = str(uuid.uuid4())

    is_image = extension in IMAGE_EXTS

    db_ok = True
    if not db_ok:
        return UploadResult(error="DB insert failed")

    if is_image:
        try:
            img = Image.open(BytesIO(file_bytes))
            compressed_bytes = _save_compressed(img, extension)
            blur_bytes = create_blurred_version(file_bytes)

            max_path = f"image/max/{uid}.{extension or 'jpg'}"
            blur_path = f"image/blur/{uid}.{extension or 'jpg'}"

            ok1 = upload_to_bunny(compressed_bytes, max_path, original_name=f"{uid}.{extension or 'jpg'}")
            ok2 = upload_to_bunny(blur_bytes, blur_path, original_name=f"{uid}.{extension or 'jpg'}")

            if not (ok1 and ok2):
                return UploadResult(error="Upload failed")

            return UploadResult(file_url=f"{uid}.{extension or 'jpg'}", blur_url="", error="")

        except Exception as e:
            return UploadResult(error=f"Image processing failed: {e}")

    if len(file_bytes) > MAX_FILE_SIZE:
        return UploadResult(error="File exceeds 10MB limit")

    doc_path = f"document/{uid}.{extension or 'bin'}"
    ok = upload_to_bunny(file_bytes, doc_path, original_name=f"{uid}.{extension or 'bin'}")
    if not ok:
        return UploadResult(error="Upload failed")

    return UploadResult(file_url=f"{uid}.{extension or 'bin'}", blur_url="", error="")



if __name__ == "__main__":
    test_path = os.path.join(os.path.dirname(__file__), "downloads", "myphoto.jpg")
    if os.path.exists(test_path):
        with open(test_path, "rb") as f:
            f.filename = os.path.basename(test_path)
            props = UploadProps(file=f, table_name="images", ref_id="123")
            res = upload_file_to_bunny(props)
            print(res.to_dict())
    else:
        print("Example skipped: downloads/myphoto.jpg not found.")
