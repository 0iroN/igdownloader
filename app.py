import os
import re
import glob
import uuid
import tempfile

from flask import (
    Flask,
    request,
    jsonify,
    send_file,
    render_template,
    after_this_request,
)
from yt_dlp import YoutubeDL

app = Flask(__name__)

# İndirilen dosyalar geçici klasöre yazılır, gönderildikten sonra silinir.
DOWNLOAD_DIR = os.path.join(tempfile.gettempdir(), "igdownloader")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# cookies.txt (Netscape formatı) varsa otomatik kullanılır. Giriş gerektiren
# videolar için gerekli. Sırayla şu konumlara bakılır (ilk bulunan kullanılır):
#   1) COOKIES_FILE ortam değişkeni
#   2) /data/cookies.txt   (Docker'da yeniden derlemeden eklemek için)
#   3) uygulama klasöründeki cookies.txt
_COOKIE_CANDIDATES = [
    os.environ.get("COOKIES_FILE", ""),
    "/data/cookies.txt",
    os.path.join(os.path.dirname(__file__), "cookies.txt"),
]

# Desteklenen platformlar
YOUTUBE_RE = re.compile(r"^https?://(www\.|m\.|music\.)?(youtube\.com|youtu\.be)/", re.I)
INSTAGRAM_RE = re.compile(r"^https?://(www\.)?instagram\.com/", re.I)

# Kullanıcıya sunulacak standart çözünürlük basamakları (yüksekten düşüğe)
STANDARD_HEIGHTS = [2160, 1440, 1080, 720, 480, 360, 240, 144]

# İzin verilen ses ayarları
AUDIO_CODECS = {"mp3", "m4a"}
AUDIO_BITRATES = {"320", "256", "192", "128"}


def find_cookies():
    for path in _COOKIE_CANDIDATES:
        if path and os.path.exists(path):
            return path
    return None


def base_opts():
    """Tüm yt-dlp çağrıları için ortak ayarlar."""
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
    }
    cookies = find_cookies()
    if cookies:
        opts["cookiefile"] = cookies
    return opts


def detect_platform(url):
    if YOUTUBE_RE.match(url):
        return "youtube"
    if INSTAGRAM_RE.match(url):
        return "instagram"
    return None


def first_entry(info):
    """Carousel/playlist dönerse ilk videoyu seç."""
    if info and info.get("entries"):
        entries = [e for e in info["entries"] if e]
        return entries[0] if entries else info
    return info


def offered_resolutions(info):
    """Videonun sunduğu en yüksek çözünürlüğe kadar standart basamaklar."""
    heights = [
        f["height"]
        for f in (info.get("formats") or [])
        if f.get("height") and f.get("vcodec") not in (None, "none")
    ]
    if not heights:
        return []
    top = max(heights)
    return [h for h in STANDARD_HEIGHTS if h <= top]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/info", methods=["POST"])
def api_info():
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    platform = detect_platform(url)
    if not platform:
        return jsonify({"error": "Geçerli bir YouTube veya Instagram bağlantısı girin."}), 400

    try:
        with YoutubeDL(base_opts()) as ydl:
            info = first_entry(ydl.extract_info(url, download=False))
    except Exception as e:
        return jsonify({"error": _friendly_error(e, platform)}), 502

    return jsonify(
        {
            "platform": platform,
            "title": info.get("title") or info.get("description") or "Video",
            "uploader": info.get("uploader") or info.get("channel") or "",
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "resolutions": offered_resolutions(info),
        }
    )


def build_opts(mode, quality, fmt, outtmpl):
    """İstenen mod/kalite/formata göre yt-dlp ayarlarını kur."""
    opts = base_opts()
    opts["outtmpl"] = outtmpl

    if mode == "audio":
        codec = fmt if fmt in AUDIO_CODECS else "mp3"
        bitrate = quality if quality in AUDIO_BITRATES else "192"
        opts["format"] = "bestaudio/best"
        opts["postprocessors"] = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": codec,
                "preferredquality": bitrate,
            }
        ]
    else:
        # video
        if quality and quality != "best" and quality.isdigit():
            h = int(quality)
            opts["format"] = (
                f"bestvideo[height<={h}][ext=mp4]+bestaudio[ext=m4a]/"
                f"bestvideo[height<={h}]+bestaudio/best[height<={h}]/best"
            )
        else:
            opts["format"] = (
                "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best"
            )
        opts["merge_output_format"] = "mp4"

    return opts


@app.route("/api/download")
def api_download():
    url = (request.args.get("url") or "").strip()
    platform = detect_platform(url)
    if not platform:
        return "Geçerli bir YouTube veya Instagram bağlantısı girin.", 400

    mode = request.args.get("mode", "video")
    mode = "audio" if mode == "audio" else "video"
    quality = request.args.get("quality", "best")
    fmt = request.args.get("format", "")

    token = uuid.uuid4().hex
    outtmpl = os.path.join(DOWNLOAD_DIR, f"{token}.%(ext)s")

    try:
        with YoutubeDL(build_opts(mode, quality, fmt, outtmpl)) as ydl:
            info = first_entry(ydl.extract_info(url, download=True))
    except Exception as e:
        _cleanup_token(token)
        return _friendly_error(e, platform), 502

    files = glob.glob(os.path.join(DOWNLOAD_DIR, f"{token}.*"))
    if not files:
        return "Dosya indirilemedi.", 502
    # Ses modunda dönüştürülmüş dosyayı (mp3/m4a), video modunda birleştirilmiş
    # dosyayı seç: en büyük dosya doğru olandır.
    filepath = max(files, key=os.path.getsize)

    raw_name = (info.get("title") or "download").strip()
    safe_name = re.sub(r'[\\/:*?"<>|]+', "_", raw_name)[:80] or "download"
    ext = os.path.splitext(filepath)[1] or ".bin"
    download_name = f"{safe_name}{ext}"

    @after_this_request
    def cleanup(response):
        _cleanup_token(token)
        return response

    return send_file(filepath, as_attachment=True, download_name=download_name)


def _cleanup_token(token):
    for f in glob.glob(os.path.join(DOWNLOAD_DIR, f"{token}.*")):
        try:
            os.remove(f)
        except OSError:
            pass


def _friendly_error(e, platform):
    msg = str(e).lower()
    if platform == "youtube":
        if "sign in" in msg or "bot" in msg or "cookies" in msg:
            return (
                "YouTube giriş/doğrulama istedi. cookies.txt ekleyip tekrar deneyin "
                "(README'ye bakın)."
            )
        if "private" in msg or "members-only" in msg:
            return "Bu video özel veya üyelere özel, indirilemez."
        if "unavailable" in msg or "removed" in msg:
            return "Video mevcut değil veya kaldırılmış."
    if platform == "instagram":
        if "login" in msg or "empty media" in msg or "rate-limit" in msg:
            return (
                "Bu Instagram videosu giriş gerektiriyor olabilir. cookies.txt ekleyip "
                "tekrar deneyin (README'ye bakın)."
            )
        if "private" in msg:
            return "Bu hesap/gönderi gizli, indirilemez."
    if "ffmpeg" in msg:
        return "Dönüştürme için ffmpeg gerekli (Docker imajında var, yerelde kurun)."
    return "İçerik alınamadı. Bağlantıyı kontrol edip tekrar deneyin."


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
