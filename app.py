import os
import re
import sys
import glob
import json
import time
import uuid
import shutil
import signal
import tempfile
import subprocess
import urllib.parse

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

# İstek gövdesi üst sınırı. MAX_PASTE_CHARS denetimi gövde belleğe alındıktan
# SONRA çalışıyor; bu satır olmadan 500 MB'lık bir gövde önce tamamen okunup
# JSON'a çözülüyor, 400 ancak ondan sonra dönüyordu. Werkzeug artık eşiği aşan
# gövdeyi okumadan keser (413). Sunucu HİÇBİR uçta dosya yüklemesi almıyor,
# yapıştırılan metnin sınırı 100.000 karakter — 2 MiB rahat üst sınırdır.
app.config["MAX_CONTENT_LENGTH"] = 2 * 1024 * 1024

# İndirilen dosyalar geçici klasöre yazılır, gönderildikten sonra silinir.
# Docker'da DOWNLOAD_DIR=/downloads (bind mount) verilir; verilmezse davranış
# eskisiyle birebir aynı kalır (geçici klasör).
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR") or os.path.join(
    tempfile.gettempdir(), "igdownloader"
)
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


# ---------------------------------------------------------------------------
# Oynatma listesi (playlist) sabitleri
# ---------------------------------------------------------------------------
# İş durumu BELLEKTE DEĞİL DİSKTE tutulur: gunicorn birden çok worker ile
# çalıştığı için bellekteki bir sözlük ikinci worker'a düşen isteklerde
# görünmez. Her iş = DOWNLOAD_DIR/pl_<job_id>/ klasörü.
JOB_PREFIX = "pl_"
JOB_RE = re.compile(r"^[0-9a-f]{32}$")


def _env_int(name, default):
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


GIB = 1024 ** 3

PLAYLIST_LIMIT = _env_int("PLAYLIST_LIMIT", 200)      # listelemede üst sınır
MAX_ITEMS_VIDEO = _env_int("MAX_ITEMS_VIDEO", 25)     # tek işte video sayısı
MAX_ITEMS_AUDIO = _env_int("MAX_ITEMS_AUDIO", 50)     # tek işte ses sayısı
MAX_JOB_BYTES = _env_int("MAX_JOB_BYTES", 8 * GIB)    # iş başına disk bütçesi
RESERVE_FREE = _env_int("RESERVE_FREE", 5 * GIB)      # bırakılacak boş alan
MAX_TOTAL_BYTES = _env_int("MAX_TOTAL_BYTES", 20 * GIB)  # tüm işlerin toplamı
MAX_DURATION = _env_int("MAX_DURATION", 3 * 3600)     # video modunda üst süre
MAX_DURATION_AUDIO = _env_int("MAX_DURATION_AUDIO", 6 * 3600)  # ses modunda üst süre

HEARTBEAT_STALE = 90     # status.json bu kadar sn güncellenmezse iş "stale"
LOCK_STALE = 120         # bayat kilit eşiği
JANITOR_INTERVAL = 60    # süpürücü en fazla bu sıklıkta çalışır
TTL_DONE = 30 * 60       # bitmiş işin dosyaları bu süre sonra silinir
TTL_STALE = 15 * 60      # bayat (ölü) işin klasörü bu süre sonra silinir
TTL_KEEP_FRESH = 10 * 60  # bu kadar yakında okunmuş iş ASLA silinmez
# Tarayıcı sekmesi arka plana atılınca zamanlayıcılar donar; 5 dk çok kısaydı ve
# sağlıklı işler kendini iptal ediyordu. Terk edilmiş işi asıl temizleyen janitor
# TTL'idir, bu yalnızca son çare.
NO_POLL_TIMEOUT = _env_int("NO_POLL_TIMEOUT", 3600)
ITEM_TIMEOUT = 20 * 60   # girdi başına duvar saati sınırı
ITEM_TIMEOUT_HARD = 180  # hook'lar cevap vermezse bu kadar sonra süreç öldürülür
CANCEL_GRACE = 10        # iptal işaretinden sonra işçiye tanınan süre
NO_PROGRESS_STALL = 15 * 60  # bu süre hiç ilerleme yoksa kalp atışı susar
POLL_MS = 2000           # arayüzün önerilen sorgu aralığı

# ---------------------------------------------------------------------------
# Çoklu bağlantı yapıştırma sabitleri
# ---------------------------------------------------------------------------
# Instagram'ın "kaydedilenler" koleksiyonu programatik olarak çekilemiyor
# (yt-dlp desteklemiyor, instaloader ilk istekte 429 yiyor). Bu yüzden
# kullanıcı bağlantıları tarayıcıdan elle kopyalayıp yapıştırır ve her
# bağlantı BUGÜN ÇALIŞAN tek-gönderi yolundan geçer.
MAX_PASTE_CHARS = 100_000          # ayrıştırılacak metnin üst sınırı
MAX_PASTE_LINKS = 300              # taranacak aday bağlantı sayısı
MAX_IG_PER_JOB = _env_int("MAX_IG_PER_JOB", 20)  # tek işte Instagram alt kotası
_ITEM_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,32}")

# Desteklenmeyen liste türleri (mix/radyo, kişisel listeler, kanal yüklemeleri)
_LIST_REJECTS = [
    (r"^(WL|LL)$", "Daha Sonra İzle / Beğenilenler listeleri desteklenmiyor."),
    (r"^(RD|UL)", "Bu bir otomatik karışım (mix) listesi, indirilemez."),
    (r"^UU", "Kanalın tüm yüklemeleri liste olarak indirilemez."),
]
_CHANNEL_PATH_RE = re.compile(r"^/(@|channel/|c/|user/)", re.I)

# Dosya adından temizlenecek karakterler: kontrol karakterleri, Windows'ta
# yasaklılar, sıfır genişlikli ve yön değiştiren (RTL override) karakterler.
_BAD_NAME_RE = re.compile(
    "[\\\\/:*?\"<>|\\x00-\\x1f\\u200b-\\u200f\\u202a-\\u202e\\ufeff]+"
)
_RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL"} | {
    f"COM{i}" for i in range(1, 10)
} | {f"LPT{i}" for i in range(1, 10)}


def find_cookies():
    for path in _COOKIE_CANDIDATES:
        if path and os.path.exists(path):
            return path
    return None


def base_opts(noplaylist=True):
    """Tüm yt-dlp çağrıları için ortak ayarlar.

    noplaylist varsayılan olarak True: mevcut tek video / Instagram carousel
    davranışı bit-bit korunur. Yalnızca playlist listeleme yolu False geçer.
    """
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": noplaylist,
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


# ---------------------------------------------------------------------------
# Playlist yardımcıları — URL çözümleme
# ---------------------------------------------------------------------------
def playlist_list_id(url):
    """URL'deki list= parametresini döndür (yoksa None). Sıfır ağ maliyeti."""
    try:
        parts = urllib.parse.urlparse(url)
    except ValueError:
        return None
    values = urllib.parse.parse_qs(parts.query or "").get("list") or []
    list_id = (values[0] if values else "").strip()
    if not list_id or not re.fullmatch(r"[A-Za-z0-9_-]{2,100}", list_id):
        return None
    return list_id


def playlist_reject_reason(url, list_id):
    """Desteklenmeyen liste türü ise Türkçe gerekçeyi, uygunsa None döndür."""
    try:
        path = urllib.parse.urlparse(url).path or ""
    except ValueError:
        path = ""
    if _CHANNEL_PATH_RE.match(path):
        return "Kanal bağlantıları liste olarak indirilemez."
    if not list_id:
        return "Bu bağlantı bir oynatma listesi değil."
    upper = list_id.upper()
    for pattern, reason in _LIST_REJECTS:
        if re.match(pattern, upper):
            return reason
    return None


def has_video_ref(url):
    """URL tek bir videoyu işaret ediyor mu? (v=, youtu.be/, /shorts/ vb.)

    Önemli: yt-dlp'nin `noplaylist` ayarı YALNIZCA URL'de bir video kimliği
    varken devreye girer. Çıplak `playlist?list=...` adresinde girmez ve tüm
    liste tam çözümlemeden geçer (dakikalar sürer). Bu yüzden ayırmamız şart.
    """
    try:
        parts = urllib.parse.urlparse(url)
    except ValueError:
        return False
    if (parts.netloc or "").lower().split(":")[0].endswith("youtu.be"):
        return bool((parts.path or "").strip("/"))
    if re.match(r"^/(shorts|embed|live|v)/", parts.path or "", re.I):
        return True
    return bool(urllib.parse.parse_qs(parts.query or "").get("v"))


def is_playlist_url(url):
    """Yalnızca YouTube + indirilebilir liste kimliği varsa True."""
    if detect_platform(url) != "youtube":
        return False
    list_id = playlist_list_id(url)
    return bool(list_id) and playlist_reject_reason(url, list_id) is None


def safe_filename_part(name, limit=150):
    """Dosya adı parçasını güvenli hale getir (UTF-8 BAYT cinsinden kırpar)."""
    text = _BAD_NAME_RE.sub("_", (name or "").strip())
    text = text.encode("utf-8")[:limit].decode("utf-8", "ignore")
    text = text.strip(" .")
    if not text:
        return "video"
    if text.upper() in _RESERVED_NAMES:
        return "_" + text
    return text


# ---------------------------------------------------------------------------
# Çoklu bağlantı yardımcıları — SAF metin işleme, HİÇBİRİ AĞA ÇIKMAZ
# ---------------------------------------------------------------------------
# Tek geçişli tarama: alternatif sıralaması önemli — şemalı adres önce gelir,
# böylece "https://www.youtube.com/..." metninde şemasız dal devreye girmez.
_LINK_SCAN_RE = re.compile(
    r"https?://[^\s<>\"'`\\|^\[\]{}]+"
    r"|(?:www\.)?(?:m\.|music\.)?(?:youtube\.com|youtu\.be|instagram\.com)"
    r"/[^\s<>\"'`\\|^\[\]{}]+",
    re.I,
)
# Sıfır genişlikli ve yön değiştiren karakterler URL'ye de bulaşıyor ve
# fullmatch doğrulamasını sessizce düşürüyor (bkz. _BAD_NAME_RE).
_INVISIBLE_RE = re.compile("[\\u200b-\\u200f\\u202a-\\u202e\\ufeff]+")
# Yapıştırılan blokta bağlantının kuyruğuna noktalama yapışır.
_TRAIL_TRIM = ".,;:!?)]}>\"'»”’…"

_YT_PATH_ID_RE = re.compile(r"^/(?:shorts|embed|live|v)/([^/?#]+)", re.I)
_YT_ID_RE = re.compile(r"[A-Za-z0-9_-]{11}")
# /p/<kod>/, /reel/<kod>/, /reels/<kod>/, /tv/<kod>/ ve /<kullanıcı>/<tür>/<kod>/
_IG_POST_RE = re.compile(
    r"^/(?:[^/]+/)?(p|reel|reels|tv)/([A-Za-z0-9_-]{5,32})(?:/|$)", re.I
)


def scan_links(text, cap=MAX_PASTE_LINKS):
    """Yapıştırılan ham metinden aday bağlantıları çıkar.

    Satır yapısına GÜVENİLMEZ: yapıştırılan blokta başlık, beğeni sayısı,
    tarih gibi metinler bağlantıyla aynı satırda olabilir.
    """
    found = []
    for match in _LINK_SCAN_RE.finditer(text or ""):
        raw = _INVISIBLE_RE.sub("", match.group(0))
        while raw and raw[-1] in _TRAIL_TRIM:
            raw = raw[:-1]
        if not raw:
            continue
        if not re.match(r"^https?://", raw, re.I):
            raw = "https://" + raw
        found.append(raw)
        if len(found) > cap:
            break
    return found[:cap]


def _normalize_host(url):
    """Mobil/eksik alan adını kanonik biçime çevir (m.instagram.com → www)."""
    try:
        parts = urllib.parse.urlsplit(url)
    except ValueError:
        return url
    host = (parts.netloc or "").lower().split(":")[0]
    if host in ("instagram.com", "m.instagram.com", "www.instagram.com"):
        return urllib.parse.urlunsplit(parts._replace(netloc="www.instagram.com"))
    return url


def youtube_video_id(url):
    """?v=, youtu.be/, /shorts/, /embed/, /live/, /v/ → 11 karakterlik kimlik."""
    try:
        parts = urllib.parse.urlsplit(url)
    except ValueError:
        return None
    host = (parts.netloc or "").lower().split(":")[0]
    path = parts.path or ""
    if host.endswith("youtu.be"):
        candidate = path.strip("/").split("/")[0]
        return candidate if _YT_ID_RE.fullmatch(candidate) else None
    if not host.endswith("youtube.com"):
        return None
    match = _YT_PATH_ID_RE.match(path)
    if match:
        candidate = match.group(1)
        return candidate if _YT_ID_RE.fullmatch(candidate) else None
    values = urllib.parse.parse_qs(parts.query or "").get("v") or []
    candidate = (values[0] if values else "").strip()
    return candidate if _YT_ID_RE.fullmatch(candidate) else None


def instagram_post_ref(url):
    """(tür, kod) döndür; tür 'p' | 'reel' | 'tv'. Tek gönderi değilse None."""
    try:
        path = urllib.parse.urlsplit(url).path or ""
    except ValueError:
        return None
    match = _IG_POST_RE.match(path)
    if not match:
        return None
    kind = match.group(1).lower()
    if kind == "reels":
        kind = "reel"
    return kind, match.group(2)


def canonical_link(raw):
    """(url, platform, item_id, reject_reason) döndür.

    TÜM sorgu parametreleri atılır (si=, t=, list=, pp=, feature=, utm_*,
    igsh=, img_index=): dedupe ancak kanonik biçimde güvenilir olur.
    """
    url = _normalize_host((raw or "").strip())
    platform = detect_platform(url)

    if platform == "youtube":
        video_id = youtube_video_id(url)
        if video_id:
            return ("https://www.youtube.com/watch?v=" + video_id, "youtube", video_id, None)
        try:
            path = urllib.parse.urlsplit(url).path or ""
        except ValueError:
            path = ""
        if _CHANNEL_PATH_RE.match(path):
            return (None, None, None, "Kanal bağlantısı — tek video bağlantısı yapıştırın.")
        if playlist_list_id(url):
            return (None, None, None, "Oynatma listesi — 📃 Oynatma listesi sekmesini kullanın.")
        return (None, None, None, "Tek video bağlantısı değil.")

    if platform == "instagram":
        try:
            segments = [s for s in (urllib.parse.urlsplit(url).path or "").split("/") if s]
        except ValueError:
            segments = []
        low = [s.lower() for s in segments]
        if "stories" in low or "highlights" in low or low[:1] == ["s"]:
            return (None, None, None, "Hikâye bağlantısı desteklenmiyor.")
        if "saved" in low:
            return (
                None,
                None,
                None,
                "Kaydedilenler koleksiyonu programatik olarak açılamıyor — "
                "gönderileri tek tek yapıştırın.",
            )
        if low[:1] == ["share"]:
            return (
                None,
                None,
                None,
                "Paylaşım bağlantısı — gönderiyi açıp adres çubuğundaki bağlantıyı kopyalayın.",
            )
        if (
            low[:1] in (["explore"], ["accounts"], ["directory"])
            or low[:2] == ["reels", "audio"]
            or "tagged" in low
        ):
            return (None, None, None, "Tek gönderi bağlantısı değil.")
        ref = instagram_post_ref(url)
        if ref:
            kind, code = ref
            return (
                f"https://www.instagram.com/{kind}/{code}/",
                "instagram",
                code,
                None,
            )
        if len(low) <= 1:
            return (None, None, None, "Profil bağlantısı — tek gönderi bağlantısı yapıştırın.")
        return (None, None, None, "Tek gönderi bağlantısı değil.")

    return (None, None, None, "Desteklenmeyen site.")


def _link_handle(url, platform, item_id):
    """Satırda gösterilecek okunabilir tutamak (başlık henüz bilinmiyor)."""
    if platform == "youtube":
        return "youtu.be/" + item_id
    return re.sub(r"^https?://(www\.)?", "", url).rstrip("/")[:300]


def normalize_links(raw_list):
    """(items, rejected, dup_count).

    Dedupe anahtarı (platform, item_id); İLK görülme sırası korunur.
    index SUNUCU tarafından 1..N yoğun olarak atanır — arayüzdeki patchRows
    data-idx eşleşmesi bunu şart koşuyor.
    """
    items, rejected = [], []
    seen = set()
    duplicates = 0
    for raw in raw_list:
        url, platform, item_id, reason = canonical_link(raw)
        if reason or not url or not item_id or not _ITEM_ID_RE.fullmatch(item_id):
            rejected.append(
                {
                    "raw": str(raw)[:200],
                    "reason": reason or "Desteklenmeyen site.",
                }
            )
            continue
        key = (platform, item_id)
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        items.append(
            {
                "index": len(items) + 1,
                "platform": platform,
                "url": url,
                "id": item_id,
                "title": _link_handle(url, platform, item_id),
            }
        )
    return items, rejected, duplicates


# ---------------------------------------------------------------------------
# Playlist yardımcıları — iş klasörü ve durum dosyası
# ---------------------------------------------------------------------------
def job_dir(job_id):
    return os.path.join(DOWNLOAD_DIR, JOB_PREFIX + job_id)


def job_files_dir(job_id):
    return os.path.join(job_dir(job_id), "files")


def job_status_path(job_id):
    return os.path.join(job_dir(job_id), "status.json")


def job_cancel_path(job_id):
    return os.path.join(job_dir(job_id), "cancel.flag")


def job_lastread_path(job_id):
    return os.path.join(job_dir(job_id), "lastread")


def job_pid_path(job_id):
    """İşçinin pid'i: status.json'a yazılmasını beklemeden, Popen'i çağıran
    taraf yazar. Böylece 'daha pid yazmamış' penceresinde de öldürülebilir."""
    return os.path.join(job_dir(job_id), "worker.pid")


def read_job_pid(job_id, status=None):
    """(pid, pgid) döndür. start_new_session=True olduğu için pgid == pid."""
    st = status if status is not None else (read_status(job_id) or {})
    pid, pgid = st.get("pid"), st.get("pgid")
    if pid and pgid:
        return pid, pgid
    try:
        with open(job_pid_path(job_id), "r", encoding="utf-8") as f:
            pid = int((f.read() or "").strip())
        return pid, pid
    except (OSError, ValueError):
        return None, None


def _proc_belongs_to_job(pid, job_id):
    """pid yeniden kullanılmış olabilir: cmdline'da job_id'yi doğrula."""
    if not pid:
        return False
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return job_id in f.read().decode("utf-8", "ignore")
    except OSError:
        return False


def write_status_atomic(job_id, status):
    """status.json'u atomik yaz (tmp + os.replace). TEK YAZAR: işçi süreç."""
    status["updated_at"] = time.time()
    path = job_status_path(job_id)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False)
    os.replace(tmp, path)


def read_status(job_id):
    """status.json'u oku. Yarım yazımda bir kez tekrar dener."""
    path = job_status_path(job_id)
    for attempt in (0, 1):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            if attempt == 0:
                time.sleep(0.15)
                continue
    return None


def _job_is_stale(job_id):
    """İşçi süreç öldü mü?

    Birincil ölçüt kalp atışı (status.json mtime); ancak makine uykuya girip
    uyandığında veya ağır yük altında mtime yanlışlıkla bayatlayabiliyor. Bu
    yüzden bayat görünen işte SÜRECİN VARLIĞI da doğrulanır (Linux /proc);
    /proc yoksa davranış eskisiyle aynı kalır.
    """
    try:
        age = time.time() - os.path.getmtime(job_status_path(job_id))
    except OSError:
        return True
    if age <= HEARTBEAT_STALE:
        return False
    pid, _pgid = read_job_pid(job_id)
    return not _proc_belongs_to_job(pid, job_id)


def _dir_size(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def total_jobs_bytes():
    """Tüm pl_* klasörlerinin toplam boyutu."""
    total = 0
    for entry in glob.glob(os.path.join(DOWNLOAD_DIR, JOB_PREFIX + "*")):
        if os.path.isdir(entry):
            total += _dir_size(entry)
    return total


def free_bytes():
    try:
        return shutil.disk_usage(DOWNLOAD_DIR).free
    except OSError:
        return 0


# ---------------------------------------------------------------------------
# Playlist yardımcıları — global kilit (aynı anda tek iş)
# ---------------------------------------------------------------------------
def _joblock_path():
    return os.path.join(DOWNLOAD_DIR, ".joblock")


def _joblock_stale():
    """Kilit dosyasının işaret ettiği iş ölmüşse True.

    'İçeriği okunamıyor' / 'klasör henüz yok' durumları YENİ alınmış bir kilitte
    de görülebilir (yaratma ile yazma arasındaki pencere). Bu yüzden o dallarda
    kilidi ancak LOCK_STALE geçtikten sonra bayat sayarız; aksi hâlde eşzamanlı
    iki /start birbirinin kilidini çalar.
    """
    path = _joblock_path()
    try:
        age = time.time() - os.path.getmtime(path)
    except OSError:
        return False  # kilit yok
    aged = age > LOCK_STALE
    try:
        with open(path, "r", encoding="utf-8") as f:
            info = json.load(f)
    except (OSError, ValueError):
        return aged
    job_id = info.get("job") or ""
    if not JOB_RE.match(job_id) or not os.path.isdir(job_dir(job_id)):
        return aged
    status = read_status(job_id)
    if not status:
        return aged
    if status.get("state") != "running":
        return True
    return _job_is_stale(job_id)


def acquire_joblock(job_id):
    """O_EXCL ile kilidi al. Bayat kilidi bir kez temizleyip tekrar dener.

    İçerik, dosya yaratılır yaratılmaz TEK yazma ile konur: yaratma ile yazma
    arasında 'boş dosya' penceresi kalmaz.
    """
    payload = json.dumps({"job": job_id, "pid": os.getpid(), "ts": time.time()})
    for attempt in (0, 1):
        try:
            fd = os.open(_joblock_path(), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            # Dosya tam bu arada silinmiş de olabilir: o zaman da tekrar dene.
            if attempt == 0 and (not os.path.exists(_joblock_path()) or _joblock_stale()):
                try:
                    os.remove(_joblock_path())
                except OSError:
                    pass
                continue
            return False
        except OSError:
            return False
        try:
            os.write(fd, payload.encode("utf-8"))
        except OSError:
            pass
        finally:
            os.close(fd)
        return True
    return False


def release_joblock(job_id):
    """Kilit bu işe aitse bırak."""
    try:
        with open(_joblock_path(), "r", encoding="utf-8") as f:
            info = json.load(f)
    except (OSError, ValueError):
        info = {}
    if not info or info.get("job") == job_id:
        try:
            os.remove(_joblock_path())
        except OSError:
            pass


def kill_job_process(job_id):
    """İşçi süreci (ve altındaki ffmpeg'i) süreç grubuyla birlikte durdur."""
    pid, pgid = read_job_pid(job_id)
    if not pid or not pgid:
        return False
    if not _proc_belongs_to_job(pid, job_id):
        return False
    try:
        os.killpg(pgid, signal.SIGTERM)
        return True
    except (OSError, AttributeError):
        return False


# ---------------------------------------------------------------------------
# Playlist yardımcıları — fırsatçı süpürücü (janitor)
# ---------------------------------------------------------------------------
def janitor():
    """Süresi dolmuş iş klasörlerini temizle. Ayrı thread YOK: istek başında
    en fazla JANITOR_INTERVAL sıklığında çalışır."""
    marker = os.path.join(DOWNLOAD_DIR, ".janitor")
    now = time.time()
    try:
        if now - os.path.getmtime(marker) < JANITOR_INTERVAL:
            return
    except OSError:
        pass
    try:
        with open(marker, "w", encoding="utf-8") as f:
            f.write(str(now))
    except OSError:
        return

    for path in glob.glob(os.path.join(DOWNLOAD_DIR, JOB_PREFIX + "*")):
        # Eşzamanlı ikinci bir süpürücü klasörü altımızdan silebilir: bu döngü
        # ASLA çağıran isteği düşürmemeli.
        try:
            if not os.path.isdir(path):
                continue
            job_id = os.path.basename(path)[len(JOB_PREFIX):]
            if not JOB_RE.match(job_id):
                continue
            try:
                last = os.path.getmtime(job_status_path(job_id))
            except OSError:
                try:
                    last = os.path.getmtime(path)
                except OSError:
                    continue
            try:
                last = max(last, os.path.getmtime(job_lastread_path(job_id)))
            except OSError:
                pass
            # Kullanıcı hâlâ dosya çekiyor olabilir: taze işe dokunma.
            if now - last < TTL_KEEP_FRESH:
                continue
            status = read_status(job_id) or {}
            state = status.get("state")
            if state == "running" and not _job_is_stale(job_id):
                continue
            ttl = TTL_STALE if state == "running" else TTL_DONE
            if now - last >= ttl:
                shutil.rmtree(path, ignore_errors=True)
        except OSError:
            continue

    # Fotoğraf medya önbelleği (ph_<kod>.json): ömrü dolmuş olanlar.
    for path in glob.glob(os.path.join(DOWNLOAD_DIR, PHOTO_PREFIX + "*.json")):
        try:
            if now - os.path.getmtime(path) >= PHOTO_CACHE_TTL:
                os.remove(path)
        except OSError:
            continue

    # Sahipsiz kalmış bayat kilit
    try:
        if os.path.exists(_joblock_path()) and _joblock_stale():
            os.remove(_joblock_path())
    except OSError:
        pass


@app.errorhandler(413)
def too_large(_e):
    """Gövde sınırı aşıldı. Arayüz her hatada d.error bekliyor: HTML sayfa
    yerine aynı sözleşmeyi döndürüyoruz."""
    return jsonify({"error": "Gönderilen veri çok büyük."}), 413


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

    janitor()

    # Çıplak liste sayfası (`playlist?list=...`, v= YOK): burada tam extract_info
    # yapmak 60 videoluk bir listede dakikalar sürer ve tek bir silinmiş video
    # tüm isteği patlatır. Ağır çağrıyı hiç yapmadan arayüzü liste görünümüne
    # yönlendiriyoruz; asıl listeleme /api/playlist/list'te extract_flat ile.
    if platform == "youtube" and not has_video_ref(url):
        list_id = playlist_list_id(url)
        if list_id and playlist_reject_reason(url, list_id) is None:
            return jsonify(
                {
                    "platform": "youtube",
                    "title": "Oynatma Listesi",
                    "uploader": "",
                    "thumbnail": None,
                    "duration": None,
                    "resolutions": [],
                    "type": "playlist",
                    "playlist_hint": {"list_id": list_id, "count_unknown": True},
                }
            )

    # Instagram: önce medya ucu (tek istek). Yalnız fotoğraflı gönderide yt-dlp
    # hiç çağrılmaz — "No video formats found!" vereceği ölçüldü. Uç cevap
    # vermezse (cookies yok vb.) media None olur ve bugünkü yol aynen işler.
    media = instagram_media_for_url(url) if platform == "instagram" else None
    if media and media["photos"] and not media["videos"]:
        return jsonify(_photo_only_info(media))

    probe = base_opts()
    if platform == "youtube" and playlist_list_id(url) and not has_video_ref(url):
        # Desteklenmeyen liste türü (mix/WL) ama yine de çıplak liste sayfası:
        # yalnızca ilk girdiyi çıkar, bozuk girdiler isteği düşürmesin.
        probe.update({"playlist_items": "1", "ignoreerrors": True})

    try:
        with YoutubeDL(probe) as ydl:
            info = first_entry(ydl.extract_info(url, download=False)) or {}
    except Exception as e:
        if media and media["photos"]:
            return jsonify(_photo_only_info(media))
        return jsonify({"error": _friendly_error(e, platform)}), 502

    if not info.get("title") and not info.get("id"):
        return jsonify({"error": "İçerik alınamadı. Bağlantıyı kontrol edip tekrar deneyin."}), 502

    # Bağlantıda indirilebilir bir liste kimliği varsa arayüze SADECE ipucu
    # veririz; playlist dalı kullanıcı isteyene kadar tetiklenmez (opt-in).
    hint = None
    if platform == "youtube":
        list_id = playlist_list_id(url)
        if list_id and playlist_reject_reason(url, list_id) is None:
            hint = {"list_id": list_id, "count_unknown": True}

    return jsonify(
        {
            "platform": platform,
            "title": info.get("title") or info.get("description") or "Video",
            "uploader": info.get("uploader") or info.get("channel") or "",
            "thumbnail": info.get("thumbnail"),
            "duration": info.get("duration"),
            "resolutions": offered_resolutions(info),
            "type": "video",
            "playlist_hint": hint,
            # Karışık carousel (video + fotoğraf): fotoğraf sekmesi de açılır.
            "photos": _photo_info_json(media) if media and media["photos"] else None,
        }
    )


def _photo_only_info(media):
    count = len(media["photos"])
    return {
        "platform": "instagram",
        "title": media["caption"] or f"{count} fotoğraf",
        "uploader": media["owner"],
        # Instagram CDN başka origin'den görsel isteğini engelliyor: önizleme
        # kendi ucumuzdan (kapak önbelleğiyle aynı ders).
        "thumbnail": f"api/photo?code={media['code']}&i=0&inline=1",
        "duration": None,
        "resolutions": [],
        "type": "photos",
        "playlist_hint": None,
        "photos": _photo_info_json(media),
    }


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


# YouTube bazı videoların medya adreslerini varsayılan (web) oynatıcı
# istemcisine 403 Forbidden ile kapatıyor; aynı video "android" istemcisiyle
# sorunsuz iniyor (17.08.2026'da ölçüldü: web 403, android 25 MiB indirdi).
# Bu yüzden SADECE 403 alındığında bir kez android istemcisiyle tekrar denenir.
# Başarılı yol hiç değişmez, yani bugün çalışan hiçbir indirme etkilenmez.
def is_forbidden_error(e):
    msg = str(e).lower()
    return "403" in msg and "forbidden" in msg


def with_android_client(opts):
    """Aynı ayarların android oynatıcı istemcisini kullanan kopyası."""
    alt = dict(opts)
    extractor = dict(alt.get("extractor_args") or {})
    youtube = dict(extractor.get("youtube") or {})
    youtube["player_client"] = ["android"]
    extractor["youtube"] = youtube
    alt["extractor_args"] = extractor
    return alt


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

    opts = build_opts(mode, quality, fmt, outtmpl)
    try:
        with YoutubeDL(opts) as ydl:
            info = first_entry(ydl.extract_info(url, download=True))
    except Exception as e:
        # 403 ise: yarım parçaları at, android istemcisiyle bir kez daha dene.
        if platform != "youtube" or not is_forbidden_error(e):
            _cleanup_token(token)
            return _friendly_error(e, platform), 502
        _cleanup_token(token)
        try:
            with YoutubeDL(with_android_client(opts)) as ydl:
                info = first_entry(ydl.extract_info(url, download=True))
        except Exception as e2:
            _cleanup_token(token)
            return _friendly_error(e2, platform), 502

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


# ---------------------------------------------------------------------------
# Instagram fotoğrafları
# ---------------------------------------------------------------------------
# yt-dlp yalnız video çıkarır: fotoğraf gönderisinde "No video formats found!"
# verir (22.10.2026'da ölçüldü, 3 fotoğraflı carousel). Fotoğrafları
# Instagram'ın kendi medya ucundan (yt-dlp'nin de girişliyken kullandığı uç)
# cookies.txt ile alırız. İstek yt-dlp'nin ağ katmanından geçer: çerez
# kavanozu, vekil, zaman aşımı aynıdır.
#
# CDN adresi istemciden ASLA alınmaz (kapak önbelleğindeki SSRF dersi): sunucu
# medya ucunu kendisi okur ve kısa süre DOWNLOAD_DIR/ph_<kod>.json'a yazar.
# Önizleme ve indirme tıklamaları Instagram API'sine YENİ istek atmaz, yalnız
# CDN'den görsel çeker — gönderi başına tek API isteği (hesap riski).
IG_APP_ID = "936619743392459"
IG_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"
)
PHOTO_PREFIX = "ph_"
PHOTO_CACHE_TTL = 20 * 60          # medya ucu yanıtının önbellek ömrü
MAX_PHOTO_BYTES = 30 * 1024 * 1024  # tek görsel üst sınırı
MAX_PHOTOS = 20                     # Instagram carousel üst sınırı 20
_IG_CODE_RE = re.compile(r"^[A-Za-z0-9_-]{5,64}$")
_PHOTO_EXT = {"image/jpeg": "jpg", "image/webp": "webp", "image/png": "png", "image/heic": "heic"}


def _photo_cache_path(code):
    return os.path.join(DOWNLOAD_DIR, f"{PHOTO_PREFIX}{code}.json")


def instagram_media(code, force=False):
    """Gönderinin fotoğrafları: {code, owner, caption, photos:[{url,w,h}], videos}.

    Ağ/giriş hatasında istisna fırlatır; çağıran karar verir.
    """
    path = _photo_cache_path(code)
    if not force:
        try:
            if time.time() - os.path.getmtime(path) < PHOTO_CACHE_TTL:
                with open(path, encoding="utf-8") as f:
                    return json.load(f)
        except (OSError, ValueError):
            pass

    # Modül seviyesinde DEĞİL: yt-dlp bu özel adı değiştirirse yalnız fotoğraf
    # yolu düşer, plworker'ın `import app` satırı düşmez.
    from yt_dlp.networking import Request
    from yt_dlp.extractor.instagram import _id_to_pk

    with YoutubeDL(base_opts()) as ydl:
        csrf = next((c.value for c in ydl.cookiejar if c.name == "csrftoken"), "")
        req = Request(
            f"https://i.instagram.com/api/v1/media/{_id_to_pk(code)}/info/",
            headers={"X-IG-App-ID": IG_APP_ID, "X-CSRFToken": csrf, "User-Agent": IG_UA},
        )
        with ydl.urlopen(req) as res:
            data = json.loads(res.read(8 * 1024 * 1024))

    item = (data.get("items") or [None])[0]
    if not item:
        raise ValueError("empty media response")

    photos, videos = [], 0
    for part in (item.get("carousel_media") or [item])[:MAX_PHOTOS]:
        if part.get("video_versions"):
            videos += 1
            continue
        cands = [
            c for c in (part.get("image_versions2") or {}).get("candidates") or []
            if cover_host_ok(c.get("url"))
        ]
        if not cands:
            continue
        best = max(cands, key=lambda c: (c.get("width") or 0) * (c.get("height") or 0))
        photos.append({"url": best["url"], "w": best.get("width"), "h": best.get("height")})

    out = {
        "code": code,
        "owner": str((item.get("user") or {}).get("username") or "")[:60],
        "caption": str((item.get("caption") or {}).get("text") or "")[:300],
        "photos": photos,
        "videos": videos,
    }
    tmp = f"{path}.{uuid.uuid4().hex}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(out, f)
        os.replace(tmp, path)
    except OSError:
        try:
            os.remove(tmp)
        except OSError:
            pass
    return out


def instagram_media_for_url(url):
    """Gönderi bağlantısından instagram_media; tek gönderi değilse ya da
    medya ucu cevap vermezse None (o zaman bugünkü yt-dlp yolu aynen çalışır)."""
    ref = instagram_post_ref(url)
    if not ref or not _IG_CODE_RE.match(ref[1]):
        return None
    try:
        return instagram_media(ref[1])
    except Exception:  # noqa: BLE001 - fotoğraf yolu video yolunu düşürmemeli
        return None


def _fetch_photo(url):
    """CDN'den (baytlar, uzantı). Yönlendirme hedefi de beyaz listeden geçer."""
    from yt_dlp.networking import Request

    if not cover_host_ok(url):
        raise ValueError("host not allowed")
    with YoutubeDL(base_opts()) as ydl:
        with ydl.urlopen(Request(url, headers={"User-Agent": IG_UA})) as res:
            if not cover_host_ok(getattr(res, "url", url)):
                raise ValueError("redirect host not allowed")
            ctype = (res.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            body = res.read(MAX_PHOTO_BYTES + 1)
    ext = _PHOTO_EXT.get(ctype)
    if not ext or len(body) > MAX_PHOTO_BYTES:
        raise ValueError("not an image or too large")
    return body, ext


def photo_bytes(media, index):
    """index'inci fotoğraf. İmzalı CDN adresinin süresi dolmuşsa medya ucunu
    BİR KEZ tazeleyip yeniden dener. Döndürülen: (baytlar, uzantı, media)."""
    try:
        body, ext = _fetch_photo(media["photos"][index]["url"])
        return body, ext, media
    except Exception:  # noqa: BLE001
        media = instagram_media(media["code"], force=True)
        body, ext = _fetch_photo(media["photos"][index]["url"])
        return body, ext, media


def photo_name(media, index, ext):
    owner = safe_filename_part(media.get("owner") or "instagram", 60)
    if len(media["photos"]) == 1:
        return f"{owner}_{media['code']}.{ext}"
    return f"{owner}_{media['code']}_{index + 1:02d}.{ext}"


def photos_zip(media):
    """Tüm fotoğraflar tek ZIP (STORED: jpg zaten sıkışık). (baytlar, ad)."""
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        for i in range(len(media["photos"])):
            body, ext, media = photo_bytes(media, i)
            zf.writestr(photo_name(media, i, ext), body)
    owner = safe_filename_part(media.get("owner") or "instagram", 60)
    return buf.getvalue(), f"{owner}_{media['code']}.zip"


def save_instagram_photos(url, dest_dir, prefix):
    """İşçi için: fotoğrafları <prefix>.<ext> (tek) ya da <prefix>.zip olarak
    yaz. Yazım .part üzerinden: _pick_file yarım dosyayı asla seçmez."""
    ref = instagram_post_ref(url)
    if not ref or not _IG_CODE_RE.match(ref[1]):
        raise ValueError("not an Instagram post")
    media = instagram_media(ref[1])
    if not media["photos"]:
        raise ValueError("no photos in post")
    if len(media["photos"]) == 1:
        body, ext, media = photo_bytes(media, 0)
    else:
        body, _name = photos_zip(media)
        ext = "zip"
    final = os.path.join(dest_dir, f"{prefix}.{ext}")
    with open(final + ".part", "wb") as f:
        f.write(body)
    os.replace(final + ".part", final)
    return media


def _photo_info_json(media):
    """/api/info yanıtının fotoğraf parçası."""
    return {
        "code": media["code"],
        "count": len(media["photos"]),
        "sizes": [[p.get("w"), p.get("h")] for p in media["photos"]],
    }


@app.route("/api/photo")
def api_photo():
    """?code=<kısa kod>&i=<sıra|all>[&inline=1]. Yol/adres istemciden gelmez."""
    code = (request.args.get("code") or "").strip()
    if not _IG_CODE_RE.match(code):
        return "Geçersiz gönderi kodu.", 400
    which = (request.args.get("i") or "all").strip()
    inline = request.args.get("inline") == "1"
    try:
        media = instagram_media(code)
    except Exception as e:  # noqa: BLE001
        return _friendly_error(e, "instagram"), 502
    count = len(media["photos"])
    if not count:
        return "Bu gönderide fotoğraf yok.", 404

    index = None
    if which != "all" or count == 1:
        try:
            index = 0 if which == "all" else int(which)
        except ValueError:
            index = -1
        if not 0 <= index < count:
            return "Geçersiz sıra numarası.", 400

    try:
        if index is None:
            body, name = photos_zip(media)
            mimetype = "application/zip"
        else:
            body, ext, media = photo_bytes(media, index)
            name = photo_name(media, index, ext)
            mimetype = "image/" + ("jpeg" if ext == "jpg" else ext)
    except Exception as e:  # noqa: BLE001
        return _friendly_error(e, "instagram"), 502

    import io

    resp = send_file(io.BytesIO(body), mimetype=mimetype, as_attachment=not inline, download_name=name)
    if inline:
        resp.headers["Cache-Control"] = "private, max-age=600"
    return resp


# ===========================================================================
# OYNATMA LİSTESİ UÇLARI
# Tek video akışı (/api/info, /api/download) buradan HİÇ etkilenmez.
# ===========================================================================
@app.route("/api/playlist/list", methods=["POST"])
def api_playlist_list():
    """Listeyi hızlıca çıkar (extract_flat). Yalnız kullanıcı 'Oynatma listesi'
    dediğinde çağrılır; tek video akışına maliyet eklemez."""
    janitor()
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()

    if detect_platform(url) != "youtube":
        return jsonify({"error": "Oynatma listeleri yalnızca YouTube için desteklenir."}), 400
    list_id = playlist_list_id(url)
    reason = playlist_reject_reason(url, list_id)
    if reason:
        return jsonify({"error": reason}), 400

    opts = base_opts(noplaylist=False)
    opts.update(
        {
            "extract_flat": "in_playlist",
            "skip_download": True,
            "playlistend": PLAYLIST_LIMIT,
            "ignoreerrors": True,
        }
    )
    canonical = "https://www.youtube.com/playlist?list=" + list_id

    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(canonical, download=False) or {}
    except Exception as e:
        return jsonify({"error": _friendly_error(e, "youtube", context="playlist")}), 502

    raw_entries = [e for e in (info.get("entries") or []) if e]
    if not raw_entries:
        return jsonify({"error": "Oynatma listesi boş veya erişilemiyor."}), 502

    entries = []
    for i, e in enumerate(raw_entries, start=1):
        title = (e.get("title") or "").strip()
        video_id = (e.get("id") or "").strip()
        unavailable = (
            not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id)
            or title.lower() in ("[deleted video]", "[private video]", "[unavailable video]")
            or not title
        )
        entries.append(
            {
                "index": i,
                "id": video_id,
                "title": title or "(erişilemeyen video)",
                "duration": int(e["duration"]) if e.get("duration") else None,
                "uploader": e.get("uploader") or e.get("channel") or "",
                "unavailable": unavailable,
            }
        )

    return jsonify(
        {
            "type": "playlist",
            "platform": "youtube",
            "playlist_id": list_id,
            "title": info.get("title") or "Oynatma Listesi",
            "uploader": info.get("uploader") or info.get("channel") or "",
            "count": len(entries),
            "truncated": len(entries) >= PLAYLIST_LIMIT,
            # extract_flat'te format bilgisi gelmez: tam merdiveni sunuyoruz,
            # o kaliteyi vermeyen videolar build_opts'taki /best zinciriyle iner.
            "resolutions": STANDARD_HEIGHTS,
            "max_select": {"video": MAX_ITEMS_VIDEO, "audio": MAX_ITEMS_AUDIO},
            "entries": entries,
        }
    )


def _create_and_spawn_job(seed_status):
    """İş klasörünü kur, status.json'u yaz, kilidi al ve kopmuş işçiyi başlat.

    SIRALAMA LOAD-BEARING'dir, aynen korunmuştur:
      1. klasör + status.json  (kilitten ÖNCE)
      2. acquire_joblock
      3. Popen(start_new_session=True)
      4. worker.pid yazımı
    Tek satır sıra değişikliği aynı anda iki işçi çalışmasına ya da hayalet
    işçiye yol açar. seed_status'un anahtar SIRASI çağıran tarafta kurulur;
    burada yalnız job/started_at/updated_at YERİNDE doldurulur.
    """
    job_id = uuid.uuid4().hex
    status = dict(seed_status)
    # Klasör ve status.json kilitten ÖNCE oluşturulur: rakip bir /start isteği
    # _joblock_stale()'de "klasör yok" görüp kilidi çalamasın.
    try:
        os.makedirs(job_files_dir(job_id), exist_ok=True)
        now = time.time()
        status["job"] = job_id
        status["started_at"] = now
        status["updated_at"] = now
        write_status_atomic(job_id, status)
    except OSError:
        shutil.rmtree(job_dir(job_id), ignore_errors=True)
        return jsonify({"error": "İş klasörü oluşturulamadı."}), 500

    if not acquire_joblock(job_id):
        shutil.rmtree(job_dir(job_id), ignore_errors=True)
        return (
            jsonify({"error": "Şu anda başka bir indirme işi sürüyor, bitmesini bekleyin."}),
            409,
        )

    try:
        worker = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plworker.py")
        proc = subprocess.Popen(
            [sys.executable, worker, job_id],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        # pid'i işçinin status.json'a yazmasını BEKLEMEYİZ: soğuk başlangıçta
        # (python + yt_dlp import) 1-3 sn sürer ve o pencerede iptal/silme
        # isteği süreci bulamayıp hayalet işçi bırakırdı.
        try:
            with open(job_pid_path(job_id), "w", encoding="utf-8") as f:
                f.write(str(proc.pid))
        except OSError:
            pass
    except Exception:
        release_joblock(job_id)
        shutil.rmtree(job_dir(job_id), ignore_errors=True)
        return jsonify({"error": "İndirme süreci başlatılamadı."}), 500

    return jsonify(
        {"job": job_id, "count": len(status.get("items") or []), "poll_ms": POLL_MS}
    )


@app.route("/api/playlist/start", methods=["POST"])
def api_playlist_start():
    """İşi HAZIRLAR ve kopmuş bir işçi süreç başlatır; indirme yapmaz."""
    janitor()
    data = request.get_json(silent=True) or {}
    url = (data.get("url") or "").strip()
    mode = "audio" if data.get("mode") == "audio" else "video"
    quality = str(data.get("quality") or "best")
    fmt = str(data.get("format") or "")
    items = data.get("items") or []

    if not is_playlist_url(url):
        return jsonify({"error": "Bu bağlantı bir oynatma listesi değil."}), 400
    if not isinstance(items, list) or not items:
        return jsonify({"error": "Hiç video seçilmedi."}), 400

    limit = MAX_ITEMS_AUDIO if mode == "audio" else MAX_ITEMS_VIDEO
    if len(items) > limit:
        return (
            jsonify(
                {
                    "error": f"En fazla {limit} video seçebilirsiniz (şu an {len(items)}). "
                    "Aralıkla bölerek indirin."
                }
            ),
            400,
        )

    clean_items = []
    for raw in items:
        if not isinstance(raw, dict):
            return jsonify({"error": "Seçim listesi bozuk."}), 400
        video_id = str(raw.get("id") or "")
        if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
            return jsonify({"error": "Seçimde geçersiz video kimliği var."}), 400
        try:
            index = int(raw.get("index"))
        except (TypeError, ValueError):
            return jsonify({"error": "Seçim listesi bozuk."}), 400
        clean_items.append(
            {
                "index": index,
                "id": video_id,
                "title": str(raw.get("title") or "video")[:300],
                "duration": int(raw["duration"]) if raw.get("duration") else None,
                "state": "queued",
                "size": None,
                "height": None,
                "file": None,
                "err": None,
            }
        )

    if free_bytes() < RESERVE_FREE:
        return jsonify({"error": "Sunucuda yeterli disk alanı yok."}), 507
    if total_jobs_bytes() > MAX_TOTAL_BYTES:
        return jsonify({"error": "Sunucudaki geçici dosyalar dolu, biraz sonra deneyin."}), 507

    # Anahtar SIRASI bugünkü status.json ile birebir aynı: job/started_at/
    # updated_at yer tutucudur, _create_and_spawn_job YERİNDE doldurur.
    return _create_and_spawn_job(
        {
            "job": None,
            "state": "running",
            "url": url,
            "mode": mode,
            "quality": quality,
            "format": fmt,
            "total": len(clean_items),
            "done": 0,
            "failed": 0,
            "bytes": 0,
            "started_at": 0,
            "updated_at": 0,
            "current": None,
            "pid": None,
            "pgid": None,
            "items": clean_items,
        }
    )


# ===========================================================================
# ÇOKLU BAĞLANTI YAPIŞTIRMA UÇLARI
# İş makinesi (status.json, .joblock, janitor, iptal, disk bütçesi, ilerleme,
# dosya sunma) playlist yoluyla PAYLAŞILIR; değişen tek şey iş kaleminin nasıl
# doğduğudur. /api/playlist/* uçlarının tek satırı bile değişmez.
# ===========================================================================
@app.route("/api/links/parse", methods=["POST"])
def api_links_parse():
    """Yapıştırılan metni bağlantılara çevirir. AĞA HİÇ ÇIKMAZ, indirme yapmaz.

    Önizleme şart: tek bozuk satır yüzünden tüm isteği reddetmek yerine
    kullanıcıya kaçının geçtiğini ve kaçının neden elendiğini gösteririz.
    """
    janitor()
    data = request.get_json(silent=True) or {}
    text = data.get("text")
    if not isinstance(text, str):
        text = ""
    mode = "audio" if data.get("mode") == "audio" else "video"

    if not text.strip():
        return jsonify({"error": "Önce bağlantıları yapıştırın."}), 400
    if len(text) > MAX_PASTE_CHARS:
        return (
            jsonify(
                {
                    "error": "Yapıştırılan metin çok uzun "
                    f"(en fazla {MAX_PASTE_CHARS} karakter)."
                }
            ),
            400,
        )

    raw_links = scan_links(text)
    if not raw_links:
        return jsonify({"error": "Metinde hiç bağlantı bulunamadı.", "rejected": []}), 400

    items, rejected, duplicates = normalize_links(raw_links)
    if not items:
        return (
            jsonify(
                {
                    "error": "Geçerli bir YouTube/Instagram gönderi bağlantısı bulunamadı.",
                    "rejected": rejected,
                    "scanned": len(raw_links),
                }
            ),
            400,
        )

    ig_count = sum(1 for i in items if i["platform"] == "instagram")
    limit = MAX_ITEMS_AUDIO if mode == "audio" else MAX_ITEMS_VIDEO
    return jsonify(
        {
            "kind": "links",
            "count": len(items),
            "limit": limit,
            "ig_count": ig_count,
            "duplicates": duplicates,
            "scanned": len(raw_links),
            "truncated": len(raw_links) >= MAX_PASTE_LINKS,
            # Önizlemede probe YAPILMAZ (25 bağlantı = 25 istek = tam olarak
            # kaçındığımız 429): format bilgisi yok, tam merdiveni sunuyoruz.
            "resolutions": STANDARD_HEIGHTS,
            "max_select": {"video": MAX_ITEMS_VIDEO, "audio": MAX_ITEMS_AUDIO},
            "max_ig": MAX_IG_PER_JOB,
            "items": items,
            "rejected": rejected,
        }
    )


@app.route("/api/links/start", methods=["POST"])
def api_links_start():
    """Çoklu bağlantı işini HAZIRLAR ve kopmuş işçiyi başlatır; indirme yapmaz."""
    janitor()
    data = request.get_json(silent=True) or {}
    links = data.get("links") or []
    mode = "audio" if data.get("mode") == "audio" else "video"
    quality = str(data.get("quality") or "best")
    fmt = str(data.get("format") or "")

    if not isinstance(links, list) or not links:
        return jsonify({"error": "Hiç bağlantı seçilmedi."}), 400
    if len(links) > MAX_PASTE_LINKS:
        return jsonify({"error": "Tek seferde çok fazla bağlantı gönderildi."}), 400
    if any(not isinstance(x, str) for x in links):
        return jsonify({"error": "Bağlantı listesi bozuk."}), 400

    # İstemciye GÜVENİLMEZ: gelen liste /parse ile AYNI fonksiyondan tekrar
    # geçer. Tek bir ret ya da dedupe farkı = kurcalanmış/bayat liste.
    items, rejected, duplicates = normalize_links(links)
    if rejected or duplicates or len(items) != len(links):
        return (
            jsonify({"error": "Bağlantı listesi geçersiz, 'Bağlantıları çöz'ü tekrar çalıştırın."}),
            400,
        )

    limit = MAX_ITEMS_AUDIO if mode == "audio" else MAX_ITEMS_VIDEO
    if len(items) > limit:
        return (
            jsonify(
                {
                    "error": f"En fazla {limit} bağlantı indirebilirsiniz "
                    f"(şu an {len(items)}). Kalanları ikinci turda indirin."
                }
            ),
            400,
        )

    ig_count = sum(1 for i in items if i["platform"] == "instagram")
    if ig_count > MAX_IG_PER_JOB:
        return (
            jsonify(
                {
                    "error": f"Tek işte en fazla {MAX_IG_PER_JOB} Instagram bağlantısı "
                    f"indirilebilir (şu an {ig_count}). Kalanları ikinci turda indirin."
                }
            ),
            400,
        )

    if free_bytes() < RESERVE_FREE:
        return jsonify({"error": "Sunucuda yeterli disk alanı yok."}), 507
    if total_jobs_bytes() > MAX_TOTAL_BYTES:
        return jsonify({"error": "Sunucudaki geçici dosyalar dolu, biraz sonra deneyin."}), 507

    clean_items = []
    for item in items:
        clean_items.append(
            {
                "index": item["index"],
                # Dosya adı öneki bu alandan kurulur (plworker prefix). Beyaz
                # liste [A-Za-z0-9_-]{1,32} olduğu için glob metakarakteri,
                # nokta ve Windows yasaklısı yapısal olarak giremez.
                "id": item["id"],
                "title": item["title"],
                # Probe yapmadık: süreyi UYDURMUYORUZ. Koruma match_filter +
                # max_filesize + _TooBig zincirine kalır.
                "duration": None,
                "state": "queued",
                "size": None,
                "height": None,
                "file": None,
                "err": None,
                "platform": item["platform"],
                "url": item["url"],
                "title_pending": True,
            }
        )

    return _create_and_spawn_job(
        {
            "job": None,
            "state": "running",
            "kind": "links",
            # Çoklu bağlantı işinin tek kaynak URL'si yoktur. plworker bu alanı
            # hiç okumaz; arayüz de boş stringte no-op davranır.
            "url": "",
            "mode": mode,
            "quality": quality,
            "format": fmt,
            "total": len(clean_items),
            "done": 0,
            "failed": 0,
            "bytes": 0,
            "started_at": 0,
            "updated_at": 0,
            "current": None,
            "pid": None,
            "pgid": None,
            "items": clean_items,
        }
    )


def _valid_job(job_id):
    return bool(job_id) and bool(JOB_RE.match(job_id)) and os.path.isdir(job_dir(job_id))


@app.route("/api/playlist/status")
def api_playlist_status():
    job_id = (request.args.get("job") or "").strip()
    if not job_id or not JOB_RE.match(job_id):
        return jsonify({"error": "Geçersiz iş kimliği."}), 400
    if not os.path.isdir(job_dir(job_id)):
        return jsonify({"error": "İş bulunamadı veya süresi doldu."}), 404

    status = read_status(job_id)
    if not status:
        return jsonify({"error": "İş bulunamadı veya süresi doldu."}), 404

    # "Kullanıcı hâlâ burada" sinyali: hem TTL hem işçinin kendini durdurması
    # bu dosyanın tazeliğine bakar. status.json'a YAZMIYORUZ (tek yazar kuralı).
    try:
        with open(job_lastread_path(job_id), "a", encoding="utf-8"):
            pass
        os.utime(job_lastread_path(job_id), None)
    except OSError:
        pass

    # Süpürücü BURADAN da tetiklenir: en sık çağrılan uç budur. Kimse /api/info
    # çağırmasa bile TTL'i dolmuş dosyalar temizlensin (JANITOR_INTERVAL sıklığı
    # zaten sınırlıyor). lastread tazelendikten SONRA çağrılır ki bu iş silinmesin.
    janitor()

    # Canlılık: kalp atışı bayatladıysa yanıtta 'stale' raporla (dosyaya değil).
    if status.get("state") == "running" and _job_is_stale(job_id):
        status["state"] = "stale"

    response = jsonify(status)
    response.headers["Cache-Control"] = "no-store"
    return response


@app.route("/api/playlist/file")
def api_playlist_file():
    job_id = (request.args.get("job") or "").strip()
    if not _valid_job(job_id):
        return jsonify({"error": "İş bulunamadı veya süresi doldu."}), 404
    try:
        index = int(request.args.get("i", ""))
    except ValueError:
        return jsonify({"error": "Geçersiz sıra numarası."}), 400

    status = read_status(job_id) or {}
    item = next((it for it in status.get("items") or [] if it.get("index") == index), None)
    if not item or not item.get("file"):
        return jsonify({"error": "Dosya yok."}), 404
    if item.get("state") != "ready":
        return jsonify({"error": "Bu video henüz hazır değil."}), 409

    # Yol istemci metninden DEĞİL, status.json kaydından kurulur.
    path = os.path.join(job_files_dir(job_id), os.path.basename(item["file"]))
    if not os.path.isfile(path):
        return jsonify({"error": "Dosya yok."}), 404
    try:
        if item.get("size") and os.path.getsize(path) != item["size"]:
            return jsonify({"error": "Dosya eksik indirilmiş, tekrar deneyin."}), 409
    except OSError:
        return jsonify({"error": "Dosya yok."}), 404

    ext = os.path.splitext(path)[1] or ".bin"
    download_name = f"{index:02d} - {safe_filename_part(item.get('title'))}{ext}"
    return send_file(path, as_attachment=True, download_name=download_name, conditional=True)


@app.route("/api/playlist/cancel", methods=["POST"])
def api_playlist_cancel():
    data = request.get_json(silent=True) or {}
    job_id = str(data.get("job") or "").strip()
    if not _valid_job(job_id):
        return jsonify({"error": "İş bulunamadı."}), 404

    # İşaret zaten varsa mtime'ını TAZELEMEYİZ: yaşı, işçiye tanınan sürenin
    # ölçüsüdür (ffmpeg birleştirmesi sırasında hook'lar çağrılmaz ve işçi
    # bayrağı görmez; o durumda süreci biz durdururuz).
    flag = job_cancel_path(job_id)
    try:
        first_asked = os.path.getmtime(flag)
    except OSError:
        first_asked = None
        try:
            with open(flag, "w", encoding="utf-8") as f:
                f.write(str(time.time()))
        except OSError:
            return jsonify({"error": "İptal işareti yazılamadı."}), 500

    status = read_status(job_id) or {}
    if status.get("state") == "running":
        overdue = first_asked is not None and (time.time() - first_asked) > CANCEL_GRACE
        if overdue or _job_is_stale(job_id):
            kill_job_process(job_id)
            release_joblock(job_id)

    return jsonify({"ok": True, "state": "cancelling"})


@app.route("/api/playlist/discard", methods=["POST"])
def api_playlist_discard():
    """Kullanıcı 'Sunucudan sil' dedi: iş klasörünü tamamen kaldır."""
    data = request.get_json(silent=True) or {}
    job_id = str(data.get("job") or "").strip()
    if not _valid_job(job_id):
        return jsonify({"error": "İş bulunamadı."}), 404

    status = read_status(job_id) or {}
    if status.get("state") == "running":
        try:
            with open(job_cancel_path(job_id), "w", encoding="utf-8") as f:
                f.write(str(time.time()))
        except OSError:
            pass
        kill_job_process(job_id)
        time.sleep(1.0)

    release_joblock(job_id)
    # .part / .ytdl / ayrı akış parçaları dahil her şey gider.
    shutil.rmtree(job_dir(job_id), ignore_errors=True)
    return jsonify({"ok": True})


def _friendly_error(e, platform, context=None):
    msg = str(e).lower()
    if context == "playlist":
        if "unviewable" in msg or "does not exist" in msg or "not exist" in msg:
            return "Oynatma listesi gizli veya erişilemiyor."
        if "sign in" in msg or "login" in msg or "cookies" in msg:
            return (
                "Bu liste giriş gerektiriyor. cookies.txt ekleyip tekrar deneyin "
                "(README'ye bakın)."
            )
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


# ===========================================================================
# KİTAPLIK
# ---------------------------------------------------------------------------
# Instagram'ın "kaydedilenler" koleksiyonu programatik olarak alınamıyor
# (yt-dlp'de çıkarıcı yok, instaloader ilk istekte 429 yiyor). Bu yüzden
# hasadı TARAYICI BETİĞİ yapar, buraya JSON olarak aktarılır ve kalıcı bir
# SQLite kataloğunda tutulur.
#
# BU BÖLÜM SAF EKLEMEDİR. Yukarıdaki hiçbir fonksiyona dokunulmaz:
#   * indirme motoru YENİDEN YAZILMAZ — kitaplıktan seçilenler istemci
#     tarafında mevcut çoklu-bağlantı görünümüne aktarılır ve bugünkü
#     /api/links/start akışından geçer (403 android yedeği, disk bütçesi,
#     iptal, süpürücü, geri bağlanma hepsi bedavaya devralınır).
#   * indirme kilidi (DOWNLOAD_DIR/.joblock) hiç kullanılmaz; AI turunun
#     kendi kilidi vardır (/data/.ailock), böylece indirme ile kategorilendirme
#     birbirini bloklamaz.
#
# MODÜL SEVİYESİNDE HİÇBİR G/Ç YAPILMAZ: plworker.py `import app as web`
# yapıyor, yani bu dosyanın gövdesi HER indirme işçisinin soğuk başlangıcında
# da koşar. Buradaki tek bir istisna doğrulanmış beş akışın hepsini düşürür.
# os.makedirs / sqlite3.connect / şema kurulumu TEMBEL'dir (ensure_schema).
# ===========================================================================
import math
import sqlite3
import hashlib
import unicodedata

# /data compose'da bind mount'tur ve KALICIDIR. Veritabanı DOWNLOAD_DIR
# altına ASLA konmaz: janitor() oradaki pl_* klasörlerini TTL ile süpürür ve
# free_bytes()/total_jobs_bytes() ölçümlerini kirletirdi.
DATA_DIR = os.environ.get("DATA_DIR") or (
    "/data"
    if os.path.isdir("/data")
    else os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
)
LIBRARY_DB = os.path.join(DATA_DIR, "library.db")
AI_LOCK = os.path.join(DATA_DIR, ".ailock")
AI_CANCEL = os.path.join(DATA_DIR, ".aicancel")

AI_LOCK_STALE = 600        # bu kadar sn eski kilit bayat sayılır
AI_HEARTBEAT_STALE = 600   # ai_run bu kadar güncellenmezse "durmus"
AI_MODEL = "claude-opus-5"
AI_BATCH = 40              # tek çağrıda sınıflandırılan gönderi sayısı

LIB_IMPORT_MAX = 200       # tek içe aktarım parçasındaki kayıt sayısı
LIB_PAGE_MAX = 100         # /api/library/items üst sınırı
LIB_PAGE_DEFAULT = 60
LIB_IDS_MAX = 500          # kategori atama / silme toplu üst sınırı
LIB_CAPTION_MAX = 2000
LIB_COVER_MAX = 2048
LIB_URL_MAX = 400

# ---------------------------------------------------------------------------
# KAPAK ÖNBELLEĞİ
# ---------------------------------------------------------------------------
# ÖLÇÜM: kaydedilmiş cover_url adresleri CANLI (sunucudan çekilince HTTP 200 +
# image/jpeg). Sorun "süre doldu" DEĞİL; Instagram CDN'i başka origin'den gelen
# görsel isteğini engelliyor (hotlink koruması). Çözüm: dosyayı SUNUCUYA bir kez
# indirip KENDİ ucumuzdan sunmak.
#
# Dosyalar /data altındadır — /downloads DEĞİL: janitor() oradaki pl_* iş
# klasörlerini TTL ile süpürüyor ve free_bytes()/total_jobs_bytes() disk
# muhasebesi de oraya bakıyor. Kapaklar indirme bütçesini kirletmemeli.
COVER_DIR = os.path.join(DATA_DIR, "covers")
# tmp AYNI dosya sisteminde olmak ZORUNDA: os.replace ancak o zaman atomiktir.
COVER_TMP = os.path.join(COVER_DIR, "tmp")
COVER_LOCK = os.path.join(DATA_DIR, ".coverlock")      # .ailock'tan TAMAMEN AYRI
COVER_CANCEL = os.path.join(DATA_DIR, ".covercancel")

COVER_LOCK_STALE = 600
COVER_HEARTBEAT_STALE = 600
COVER_BATCH = 25              # tek transaction'da yazılan satır sayısı
COVER_MAX_BYTES = 3 * 1024 * 1024          # tek kapak tavanı
COVER_RUN_MAX_BYTES = 512 * 1024 * 1024    # önbellek toplam tavanı
COVER_DELAY = 0.4             # istekler arası bekleme (üstüne jitter)
COVER_TIMEOUT = 15
COVER_TMP_TTL = 3600          # yarım kalan .part dosyası bu kadar sonra çöp

# HOST BEYAZ LİSTESİ. cover_url kullanıcının içe aktardığı JSON'dan geliyor;
# import yalnız "https:// ile başlıyor mu, 2048'i aşıyor mu" bakıyordu. Bu liste
# olmadan uydurma bir adres sunucuyu keyfi bir hedefe istek atmaya zorlayabilirdi
# (SSRF). Ayrıca "Instagram API'sine istek atma" kuralını niyet olmaktan çıkarıp
# yapısal garantiye çevirir: yalnız statik CDN dosyalarına gidilir.
COVER_HOSTS = tuple(
    h.strip().lower()
    for h in (
        os.environ.get("COVER_HOSTS") or ".cdninstagram.com,.fbcdn.net"
    ).split(",")
    if h.strip()
)

# Content-Type → uzantı. Listede olmayan tür indirilmez (satır 'gone' olur).
COVER_EXT = {"image/jpeg": "jpg", "image/jpg": "jpg", "image/webp": "webp", "image/png": "png"}
COVER_MIME = {"jpg": "image/jpeg", "webp": "image/webp", "png": "image/png"}

# Sunum ucunun TEK kapısı. Dosya adı içerik adreslidir (sha256(cover_url)[:32]);
# istemciden gelen metinden yol KURULMAZ.
_COVER_NAME_RE = re.compile(r"[0-9a-f]{32}\.(?:jpg|webp|png)")
# /items'ın kategori filtresi için biçim kapısı: listede olmayan ("bilinmeyen")
# kodlar da filtrelenebilsin ama serbest metin WHERE'e girmesin.
_CAT_KOD_RE = re.compile(r"[a-z0-9_]{1,32}")

COVER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
COVER_ACCEPT = "image/avif,image/webp,image/jpeg,*/*"

# AI yeniden sınıflandırma kapsamının hedefi: "kutu bulunamadı" (diger) ve
# "sinyal yok" (belirsiz) kayıtları.
YENIDEN_KATEGORILER = ("diger", "belirsiz")
AI_ITEM_USD = 0.00097         # ölçülmüş birim maliyet (1932 kayıt = 1,87 USD)

# Sabit taksonomi. TABLO YOK, sürüm YOK: tek kişilik ev aracı.
#   diger    = konu belli ama listeye girmiyor
#   belirsiz = elde sinyal yok (Instagram'ın otomatik alt metinleri)
KATEGORILER = [
    ("yemek", "Yemek & Tarif"),
    ("muzik", "Müzik"),
    ("mizah", "Mizah"),
    ("spor", "Spor & Antrenman"),
    ("teknoloji", "Teknoloji"),
    ("seyahat", "Seyahat"),
    ("egitim", "Eğitim & Nasıl yapılır"),
    ("moda", "Moda & Güzellik"),
    ("hayvan", "Hayvanlar"),
    ("sanat", "Sanat & Tasarım"),
    ("oyun", "Oyun"),
    ("haber", "Haber & Gündem"),
    ("otomobil", "Otomobil"),
    # 19.08.2026: kategorisiz 1195 kaydin OLCUMUNDEN eklendi (/api/library/scan).
    # Kelime sikligi: film 235, dizi 56, imdb 46, movie 41, sinema 26 | anime 93.
    ("film", "Film & Dizi"),
    ("anime", "Anime & Manga"),
    ("diger", "Diğer"),
    ("belirsiz", "Belirsiz"),
]
KATEGORI_KODLARI = [k for k, _ in KATEGORILER]
KATEGORI_ADLARI = dict(KATEGORILER)

# Modele verilecek tek satırlık tanımlar (sistem promptu buradan kurulur).
KATEGORI_TANIMLARI = {
    "yemek": "yemek tarifi, mutfak, restoran, içecek",
    "muzik": "şarkı, enstrüman, konser, prodüksiyon",
    "mizah": "komedi, skeç, şaka, eğlence amaçlı içerik",
    "spor": "antrenman, fitness, maç, sporcu",
    "teknoloji": "yazılım, donanım, yapay zekâ, elektronik",
    "seyahat": "gezi, şehir, doğa, tatil, rota",
    "egitim": "öğretici anlatım, nasıl yapılır, ders, bilgi",
    "moda": "giyim, stil, makyaj, bakım",
    "hayvan": "evcil hayvan, vahşi yaşam, hayvan videosu",
    "sanat": "resim, tasarım, mimari, el işi, fotoğrafçılık",
    "oyun": "video oyunu, oyun içi görüntü, oyun haberi",
    "haber": "güncel olay, siyaset, gündem, ekonomi haberi",
    "otomobil": "araba, motosiklet, modifiye, sürüş",
    "film": "film, dizi, sinema, film önerisi, oyuncu, yönetmen, konu anlatımı, IMDb",
    "anime": "anime, manga, anime edit, japon animasyonu, anime karakteri",
    "diger": "konu belli ama yukarıdaki hiçbir başlığa girmiyor",
    "belirsiz": "metinde gerçek bir konu ipucu yok",
}

# SQLite LOWER() yalnız ASCII katlar: "İSTANBUL" ile "istanbul" eşleşmez.
# Katlama bu yüzden Python'da yapılmak ZORUNDA ve sonucu search_text'e yazılır.
_TR_FOLD = str.maketrans("İIıŞşĞğÜüÖöÇç", "iiissgguuoocc")

_LIB_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS item (
  id            INTEGER PRIMARY KEY,
  platform      TEXT NOT NULL CHECK (platform IN ('youtube','instagram')),
  item_id       TEXT NOT NULL
                CHECK (length(item_id) BETWEEN 1 AND 32
                       AND item_id NOT GLOB '*[^A-Za-z0-9_-]*'),
  url           TEXT NOT NULL CHECK (length(url) <= 400),
  kind          TEXT NOT NULL DEFAULT '',
  title         TEXT NOT NULL DEFAULT '',
  caption       TEXT NOT NULL DEFAULT '' CHECK (length(caption) <= 2000),
  uploader      TEXT NOT NULL DEFAULT '',
  duration      INTEGER,
  unavailable   INTEGER NOT NULL DEFAULT 0,
  cover_url     TEXT CHECK (cover_url IS NULL OR length(cover_url) <= 2048),
  cover_at      INTEGER,
  search_text   TEXT NOT NULL DEFAULT '',
  category      TEXT,
  category_src  TEXT NOT NULL DEFAULT ''
                CHECK (category_src IN ('','ai','kural','kullanici')),
  ai_conf       TEXT CHECK (ai_conf IS NULL OR ai_conf IN ('yuksek','orta','dusuk')),
  ai_state      TEXT NOT NULL DEFAULT 'pending'
                CHECK (ai_state IN ('pending','done','failed','refused','skipped')),
  ai_tries      INTEGER NOT NULL DEFAULT 0,
  ai_at         INTEGER,
  dl_state      TEXT NOT NULL DEFAULT 'none'
                CHECK (dl_state IN ('none','ready','failed')),
  dl_job        TEXT,
  dl_at         INTEGER,
  dl_size       INTEGER,
  dl_err        TEXT,
  dl_count      INTEGER NOT NULL DEFAULT 0,
  first_seen_at INTEGER NOT NULL,
  last_seen_at  INTEGER NOT NULL,
  -- KAPAK ÖNBELLEĞİ. Bu beş sütunda CHECK KISITI YOK: mevcut 1932 kayıtlık
  -- tabloya ALTER TABLE ADD COLUMN ile eklenen sürümle BİREBİR aynı olsun.
  --   cover_file  = COVER_DIR'e göreli yol ("ab/abcd….jpg")
  --   cover_state = '' | 'ok' | 'failed' | 'gone'
  cover_file    TEXT,
  cover_state   TEXT NOT NULL DEFAULT '',
  cover_tries   INTEGER NOT NULL DEFAULT 0,
  cover_bytes   INTEGER,
  cover_dl_at   INTEGER,
  UNIQUE (platform, item_id)
);

CREATE INDEX IF NOT EXISTS idx_item_recent   ON item(first_seen_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_item_platform ON item(platform, first_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_item_category ON item(category, first_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_item_dl       ON item(dl_state);
CREATE INDEX IF NOT EXISTS idx_item_ai_pend  ON item(id) WHERE ai_state = 'pending';

CREATE TABLE IF NOT EXISTS collection (
  id         INTEGER PRIMARY KEY,
  source     TEXT NOT NULL CHECK (source IN ('ig_saved','yt_playlist','manual')),
  ext_id     TEXT NOT NULL DEFAULT '',
  name       TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE (source, ext_id)
);

CREATE TABLE IF NOT EXISTS collection_item (
  collection_id INTEGER NOT NULL REFERENCES collection(id) ON DELETE CASCADE,
  item_id       INTEGER NOT NULL REFERENCES item(id)       ON DELETE CASCADE,
  pos           INTEGER NOT NULL DEFAULT 0,
  added_at      INTEGER NOT NULL,
  PRIMARY KEY (collection_id, item_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_ci_item ON collection_item(item_id);

CREATE TABLE IF NOT EXISTS ai_run (
  id         INTEGER PRIMARY KEY,
  state      TEXT NOT NULL DEFAULT 'running'
             CHECK (state IN ('running','done','failed','cancelled','paused')),
  model      TEXT NOT NULL,
  total      INTEGER NOT NULL DEFAULT 0,
  done       INTEGER NOT NULL DEFAULT 0,
  failed     INTEGER NOT NULL DEFAULT 0,
  batches    INTEGER NOT NULL DEFAULT 0,
  note       TEXT,
  pid        INTEGER,
  started_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  -- Turun kapsamı. İşçi seçim sorgusunu BUNA göre dallandırır. CHECK YOK:
  -- ALTER ile eklenen sürümle birebir aynı kalmalı.
  scope      TEXT NOT NULL DEFAULT 'kategorisiz'
);

-- Kapak turu AYRI tabloda tutulur. ai_run'a yazsaydı /api/library/ai/status
-- (son ai_run satırını okuyor) AI çubuğunda kapak turunu gösterirdi; ayrıca
-- _ailock_stale() bayatlığı ai_run.updated_at'ten okuduğu için paylaşılan bir
-- kilit hangi turun kalbini dinleyeceğini bilemezdi.
CREATE TABLE IF NOT EXISTS cover_run (
  id         INTEGER PRIMARY KEY,
  state      TEXT NOT NULL DEFAULT 'running',
  total      INTEGER NOT NULL DEFAULT 0,
  done       INTEGER NOT NULL DEFAULT 0,
  failed     INTEGER NOT NULL DEFAULT 0,
  gone       INTEGER NOT NULL DEFAULT 0,
  bytes      INTEGER NOT NULL DEFAULT 0,
  note       TEXT,
  pid        INTEGER,
  started_at INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL DEFAULT 0
);
"""

# UPSERT (ON CONFLICT DO UPDATE) SQLite 3.35+ ister. python:3.12-slim 3.40+
# taşıyor ama taban imaj değişirse sessizce kırılmasın: düşük sürümde iki
# adımlı SELECT + INSERT/UPDATE yoluna düşülür.
_LIB_HAS_UPSERT = sqlite3.sqlite_version_info >= (3, 35, 0)
_LIB_SCHEMA_READY = False


def fold_tr(s):
    """Türkçe duyarlı arama katlaması (İ/I/ı → i, aksanlar sıyrılır)."""
    s = (s or "").translate(_TR_FOLD).lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).strip()


def _lib_like_escape(s):
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def lib_db():
    """Her istekte YENİ bağlantı. Thread'ler arasında ASLA paylaşılmaz.

    gunicorn 2 worker + kopmuş AI işçisi aynı dosyaya bakıyor: WAL ve
    busy_timeout olmadan rastgele isteklerde 'database is locked' patlar.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    con = sqlite3.connect(LIBRARY_DB, timeout=5.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000")
    # BAĞLANTI BAŞINA açılmak ZORUNDA: yoksa ON DELETE CASCADE çalışmaz.
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def ensure_schema():
    """TEMBEL şema kurulumu — yalnız /api/library/* uçlarının ilk satırında.

    Kitaplık hiç açılmazsa /data/library.db dosyası OLUŞMAZ.
    """
    global _LIB_SCHEMA_READY
    if _LIB_SCHEMA_READY:
        return
    con = lib_db()
    try:
        try:
            row = con.execute("PRAGMA journal_mode=WAL").fetchone()
            mode = (row[0] if row else "") or ""
            if mode.lower() != "wal":
                # Ağ dosya sisteminde (NFS/SMB) WAL alınamaz. Kitaplık yine de
                # açık kalmalı: DELETE modunda çalışmaya devam ediyoruz.
                con.execute("PRAGMA journal_mode=DELETE")
        except sqlite3.Error:
            pass
        con.executescript(_LIB_SCHEMA_SQL)
        _lib_migrate(con)
        con.execute("PRAGMA user_version=1")
        con.commit()
    finally:
        con.close()
    _LIB_SCHEMA_READY = True


def _lib_migrate(con):
    """Mevcut tabloya yeni sütunları ekler. İDEMPOTAN ve YARIŞA DAYANIKLI.

    _LIB_SCHEMA_SQL yalnız CREATE TABLE IF NOT EXISTS kullanıyor: tablo zaten
    varsa (canlıda 1932 kayıt var) oradaki yeni sütun tanımı HİÇBİR ŞEY YAPMAZ.
    Sütun ancak ALTER TABLE ile gelir.

    ALTER TABLE ADD COLUMN yalnız şema başlığını yazar; satırlara DOKUNMAZ,
    tabloyu YENİDEN YAZMAZ. Tablo yeniden oluşturma (CREATE+INSERT SELECT+DROP)
    KESİNLİKLE YASAK — veri kaybının tek gerçek yolu odur.

    EŞZAMANLILIK: _LIB_SCHEMA_READY SÜREÇ YEREL bir bayrak; soğuk başlangıçta
    2 gunicorn worker'ı ve kopmuş işçiler PRAGMA table_info'yu aynı anda okuyup
    üçü de ALTER gönderebilir. table_info kontrolü yaygın durumu, except yarışı
    kapatır — tek başına ikisi de yetmez. Yalnız OperationalError yutulur.
    """
    var = {r[1] for r in con.execute("PRAGMA table_info(item)")}
    for ad, tanim in (
        ("cover_file", "TEXT"),
        ("cover_state", "TEXT NOT NULL DEFAULT ''"),
        ("cover_tries", "INTEGER NOT NULL DEFAULT 0"),
        ("cover_bytes", "INTEGER"),
        ("cover_dl_at", "INTEGER"),
    ):
        if ad in var:
            continue
        try:
            con.execute("ALTER TABLE item ADD COLUMN %s %s" % (ad, tanim))
        except sqlite3.OperationalError:
            pass  # yarış: başka bir süreç aynı anda ekledi

    arun = {r[1] for r in con.execute("PRAGMA table_info(ai_run)")}
    if "scope" not in arun:
        try:
            con.execute(
                "ALTER TABLE ai_run ADD COLUMN scope TEXT NOT NULL "
                "DEFAULT 'kategorisiz'"
            )
        except sqlite3.OperationalError:
            pass

    # İNDEKS ALTER'LARDAN SONRA. _LIB_SCHEMA_SQL'e yazılsaydı executescript
    # ALTER'lardan ÖNCE koşacağı için eski tabloda cover_state henüz yokken
    # OperationalError fırlatır, ensure_schema patlar ve TÜM /api/library/*
    # uçları düşerdi — bu değişikliğin bilinen tek tuğlalaştırma yolu.
    try:
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_item_cover ON item(cover_state) "
            "WHERE cover_url IS NOT NULL"
        )
    except sqlite3.OperationalError:
        pass


def ai_enabled():
    """Anahtar yoksa AI özelliği ZARİFÇE KAPALI olmalı, servis çökmemeli."""
    return bool((os.environ.get("ANTHROPIC_API_KEY") or "").strip())


def _ailock_stale():
    """Kilidin işaret ettiği AI turu ölmüşse True.

    SALT mtime'a BAKMAK YANLIŞTI: kilit dosyası bir kez yazılıyor, işçi ise
    onu değil ai_run.updated_at'i tazeliyor. 951 kayıtlık SAĞLIKLI bir tur
    10-18 dakika sürebildiği için (AI_MAX_MINUTES=30) mtime 600 sn'yi aşıyor
    ve ikinci bir işçi doğuruluyordu → çift fatura. Bu yüzden bayatlık
    _joblock_stale() deseniyle turun KALP ATIŞINDAN okunur.

    Damgalanmamış (henüz run_id yazılmamış) ya da okunamayan kilitte yalnız
    yaşa bakılır: yaratma ile damgalama arasındaki pencerede kilit çalınmasın.
    """
    try:
        age = time.time() - os.path.getmtime(AI_LOCK)
    except OSError:
        return False  # kilit yok
    aged = age > AI_LOCK_STALE
    try:
        with open(AI_LOCK, "r", encoding="utf-8") as f:
            info = json.load(f)
    except (OSError, ValueError):
        return aged
    run_id = info.get("run") if isinstance(info, dict) else None
    if not run_id:
        return aged
    try:
        con = lib_db()
        try:
            row = con.execute(
                "SELECT state, updated_at FROM ai_run WHERE id=?", (run_id,)
            ).fetchone()
        finally:
            con.close()
    except sqlite3.Error:
        return aged
    if not row:
        return aged
    if row["state"] != "running":
        return True
    return (time.time() - (row["updated_at"] or 0)) > AI_HEARTBEAT_STALE


def lib_acquire_ailock(note=""):
    """/data/.ailock — indirme kilidinden (DOWNLOAD_DIR/.joblock) TAMAMEN AYRI."""
    os.makedirs(DATA_DIR, exist_ok=True)
    for attempt in (0, 1):
        try:
            fd = os.open(AI_LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {"ts": time.time(), "note": str(note), "pid": os.getpid(), "run": None}
                    )
                )
            return True
        except FileExistsError:
            if attempt:
                return False
            # Dosya tam bu arada silinmiş de olabilir: o zaman da tekrar dene.
            if not os.path.exists(AI_LOCK) or _ailock_stale():
                try:
                    os.unlink(AI_LOCK)
                except OSError:
                    pass
                continue
            return False
        except OSError:
            return False
    return False


def lib_stamp_ailock(run_id):
    """Kilide tur numarasını damgala — SAHİPLİK buradan doğar.

    Damga olmadan lib_release_ailock() bir başkasının kilidini silebilirdi
    (release_joblock()'un çözdüğü sorunun aynısı).
    """
    try:
        with open(AI_LOCK, "w", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.time(), "pid": os.getpid(), "run": int(run_id)}))
        return True
    except (OSError, TypeError, ValueError):
        return False


def lib_release_ailock(run_id=None):
    """Kilit bu tura aitse bırak (release_joblock deseninin aynısı).

    run_id verilmezse koşulsuz bırakılır — YALNIZCA kilidi yeni almış, henüz
    damgalamamış çağıran böyle kullanmalıdır.
    """
    if run_id is not None:
        try:
            with open(AI_LOCK, "r", encoding="utf-8") as f:
                info = json.load(f)
        except (OSError, ValueError):
            info = {}
        if isinstance(info, dict):
            sahip = info.get("run")
            if sahip is not None:
                try:
                    if int(sahip) != int(run_id):
                        return  # BAŞKASININ kilidi: dokunma
                except (TypeError, ValueError):
                    pass
    try:
        os.unlink(AI_LOCK)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# KAPAK ÖNBELLEĞİ — yol yardımcıları ve KENDİ kilidi
# ---------------------------------------------------------------------------
# Kilit üçlüsü AI üçlüsünün KOPYASIDIR, parametreleştirilmedi: canlı AI akışının
# kilit mantığına dokunmamak (regresyon riski) ~40 satır tekrardan daha değerli.
# İki kilit AYRI olduğu için kapak turu ile AI turu AYNI ANDA çalışabilir.


def cover_key(cover_url):
    """İÇERİK ADRESLİ dosya anahtarı: sha256(cover_url)[:32].

    Adın adresten türemesi önbellek geçersizlemesini bedavaya çözer: yeniden
    içe aktarımda cover_url değişirse anahtar da değişir, yani eski dosya asla
    yeni kapak diye sunulamaz. Kayıt id'si dosya adına GİRMEZ.
    """
    return hashlib.sha256((cover_url or "").encode("utf-8")).hexdigest()[:32]


def cover_rel(key, ext):
    """COVER_DIR'e göreli yol — 256 alt klasöre yayılır."""
    return "%s/%s.%s" % (key[:2], key, ext)


def cover_host_ok(url):
    """https + host beyaz listesi. Yönlendirme hedefi de buradan geçer."""
    try:
        parca = urllib.parse.urlsplit(url or "")
    except ValueError:
        return False
    if parca.scheme != "https":
        return False
    host = (parca.hostname or "").lower()
    if not host:
        return False
    # NOKTA SINIRI ZORUNLU. 'host.endswith(h)' ham hâliyle COVER_HOSTS'a
    # noktasız yazılmış bir girdide ("fbcdn.net" — Ölç çıktısı host adlarını
    # noktasız bastığı için doğal yazım budur) 'evil-fbcdn.net' gibi SALDIRGANIN
    # KAYDEDEBİLECEĞİ alan adlarını da geçirirdi. Girdiyi normalize ediyoruz:
    # '.fbcdn.net' ve 'fbcdn.net' artık aynı ve güvenli davranır.
    for h in COVER_HOSTS:
        kok = h.strip().lstrip(".").lower()
        if kok and (host == kok or host.endswith("." + kok)):
            return True
    return False


def _coverlock_stale():
    """Kilidin işaret ettiği kapak turu ölmüşse True (_ailock_stale kopyası).

    EK KAPI (yalnız burada, .ailock'a DOKUNULMAZ): kilit container doğuşundan
    ESKİYSE koşulsuz bayattır. Süreç ölürken kilit dosyası diskte kalır ve
    yeniden başlatma sonrası hiçbir kalp atışı gelmeyeceği için tur sonsuza dek
    "sürüyor" görünürdü. /proc/1 Linux'a özgü; başka yerde sessizce düşer.
    """
    try:
        age = time.time() - os.path.getmtime(COVER_LOCK)
    except OSError:
        return False  # kilit yok
    aged = age > COVER_LOCK_STALE
    try:
        with open(COVER_LOCK, "r", encoding="utf-8") as f:
            info = json.load(f)
    except (OSError, ValueError):
        return aged
    if not isinstance(info, dict):
        return aged
    try:
        if float(info.get("ts") or 0) < os.path.getmtime("/proc/1"):
            return True  # kilit bu container doğmadan önce yazılmış
    except (OSError, TypeError, ValueError):
        pass
    run_id = info.get("run")
    if not run_id:
        return aged
    try:
        con = lib_db()
        try:
            row = con.execute(
                "SELECT state, updated_at, pid FROM cover_run WHERE id=?", (run_id,)
            ).fetchone()
        finally:
            con.close()
    except sqlite3.Error:
        return aged
    if not row:
        return aged
    if row["state"] != "running":
        return True
    if (time.time() - (row["updated_at"] or 0)) <= COVER_HEARTBEAT_STALE:
        return False
    # CANLILIK DOĞRULAMASI (indirme tarafındaki _proc_belongs_to_job muadili):
    # kalp atışı geç kalmış olabilir ama süreç YAŞIYORSA kilidi ÇALMAYIZ. Aksi
    # hâlde ikinci bir --cover işçisi doğar, CDN'e giden istek hızı tam hız
    # sınırının yendiği anda ikiye katlanır ve yeni işçinin _cover_prune()'u
    # eskisinin henüz commit etmediği dosyaları öksüz sanıp siler.
    try:
        pid = int(row["pid"] or 0)
    except (KeyError, IndexError, TypeError, ValueError):
        pid = 0
    # cmdline'da run_id aranır: pid yeniden kullanılmış olabilir.
    if pid and _proc_belongs_to_job(pid, str(run_id)):
        return False
    return True


def lib_acquire_covlock(note=""):
    os.makedirs(DATA_DIR, exist_ok=True)
    for attempt in (0, 1):
        try:
            fd = os.open(COVER_LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(
                    json.dumps(
                        {"ts": time.time(), "note": str(note), "pid": os.getpid(), "run": None}
                    )
                )
            return True
        except FileExistsError:
            if attempt:
                return False
            if not os.path.exists(COVER_LOCK) or _coverlock_stale():
                try:
                    os.unlink(COVER_LOCK)
                except OSError:
                    pass
                continue
            return False
        except OSError:
            return False
    return False


def lib_stamp_covlock(run_id):
    try:
        with open(COVER_LOCK, "w", encoding="utf-8") as f:
            f.write(json.dumps({"ts": time.time(), "pid": os.getpid(), "run": int(run_id)}))
        return True
    except (OSError, TypeError, ValueError):
        return False


def lib_release_covlock(run_id=None):
    """Kilit bu tura aitse bırak. TEK bırakma noktası plworker main()'in
    finally'sidir; run_cover kendi finally'sinde BIRAKMAZ (yarış önlemi)."""
    if run_id is not None:
        try:
            with open(COVER_LOCK, "r", encoding="utf-8") as f:
                info = json.load(f)
        except (OSError, ValueError):
            info = {}
        if isinstance(info, dict):
            sahip = info.get("run")
            if sahip is not None:
                try:
                    if int(sahip) != int(run_id):
                        return  # BAŞKASININ kilidi: dokunma
                except (TypeError, ValueError):
                    pass
    try:
        os.unlink(COVER_LOCK)
    except OSError:
        pass


def _lib_collection(con, source, ext_id, name):
    now = int(time.time())
    row = con.execute(
        "SELECT id FROM collection WHERE source=? AND ext_id=?", (source, ext_id)
    ).fetchone()
    if row:
        con.execute(
            "UPDATE collection SET name=?, updated_at=? WHERE id=?",
            (name, now, row["id"]),
        )
        return row["id"]
    cur = con.execute(
        "INSERT INTO collection (source, ext_id, name, created_at, updated_at) "
        "VALUES (?,?,?,?,?)",
        (source, ext_id, name, now, now),
    )
    return cur.lastrowid


_LIB_UPSERT_SQL = """
INSERT INTO item (platform,item_id,url,kind,title,caption,uploader,duration,
                  cover_url,cover_at,search_text,unavailable,
                  first_seen_at,last_seen_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(platform,item_id) DO UPDATE SET
  url         = excluded.url,
  kind        = CASE WHEN excluded.kind     <> '' THEN excluded.kind     ELSE item.kind     END,
  title       = CASE WHEN excluded.title    <> '' THEN excluded.title    ELSE item.title    END,
  caption     = CASE WHEN excluded.caption  <> '' THEN excluded.caption  ELSE item.caption  END,
  uploader    = CASE WHEN excluded.uploader <> '' THEN excluded.uploader ELSE item.uploader END,
  duration    = COALESCE(excluded.duration, item.duration),
  cover_url   = COALESCE(excluded.cover_url, item.cover_url),
  cover_at    = CASE WHEN excluded.cover_url IS NOT NULL THEN excluded.cover_at ELSE item.cover_at END,
  unavailable = excluded.unavailable,
  search_text = excluded.search_text,
  last_seen_at= excluded.last_seen_at,
  -- ÖNBELLEK GEÇERSİZLEME: kapak ADRESİ değiştiyse indirme durumu sıfırlanır,
  -- böylece yeni adres bir sonraki kapak turunda indirilir. cover_file SİLİNMEZ
  -- (eski görsel yenisi inene kadar ekranda kalsın); yeni dosya inince işçi
  -- eskisini kendisi siler.
  cover_state = CASE WHEN excluded.cover_url IS NOT NULL
                      AND excluded.cover_url <> item.cover_url
                     THEN '' ELSE item.cover_state END,
  cover_tries = CASE WHEN excluded.cover_url IS NOT NULL
                      AND excluded.cover_url <> item.cover_url
                     THEN 0  ELSE item.cover_tries END
"""


def _lib_upsert_item(con, row, mevcut):
    """Tek kalemi ekler/günceller, (item_pk, 'eklendi'|'guncellendi') döner.

    ASLA EZİLMEYENLER: category, category_src, ai_*, dl_*, first_seen_at.
    Yani yeniden aktarım AI emeğini ve indirme geçmişini YOK ETMEZ; yalnız
    süresi dolan kapağı ve metinleri tazeler.
    """
    now = int(time.time())
    # search_text BİRLEŞMİŞ değerlerden hesaplanmalı: boş gelen alan eskisini
    # ezmediği için yeni metni eskisiyle harmanlıyoruz.
    title = row["title"] or (mevcut["title"] if mevcut else "")
    caption = row["caption"] or (mevcut["caption"] if mevcut else "")
    uploader = row["uploader"] or (mevcut["uploader"] if mevcut else "")
    search_text = fold_tr(" ".join([title, caption, uploader, row["item_id"]]))

    params = (
        row["platform"], row["item_id"], row["url"], row["kind"],
        row["title"], row["caption"], row["uploader"], row["duration"],
        row["cover_url"], row["cover_at"], search_text, 1 if row["unavailable"] else 0,
        now, now,
    )
    if _LIB_HAS_UPSERT:
        con.execute(_LIB_UPSERT_SQL, params)
    elif mevcut:
        # UPSERT'siz yedek yol da önbelleği geçersizlemek ZORUNDA: yoksa eski
        # SQLite'ta kapak adresi değişse bile önbellek sessizce bayat kalırdı.
        kapak_degisti = bool(row["cover_url"]) and row["cover_url"] != mevcut["cover_url"]
        con.execute(
            "UPDATE item SET url=?, kind=?, title=?, caption=?, uploader=?, "
            "duration=?, cover_url=?, cover_at=?, unavailable=?, search_text=?, "
            "cover_state=?, cover_tries=?, last_seen_at=? WHERE id=?",
            (
                row["url"],
                row["kind"] or mevcut["kind"],
                title, caption, uploader,
                row["duration"] if row["duration"] is not None else mevcut["duration"],
                row["cover_url"] or mevcut["cover_url"],
                row["cover_at"] if row["cover_url"] else mevcut["cover_at"],
                1 if row["unavailable"] else 0,
                search_text,
                "" if kapak_degisti else (mevcut["cover_state"] or ""),
                0 if kapak_degisti else (mevcut["cover_tries"] or 0),
                now, mevcut["id"],
            ),
        )
    else:
        con.execute(
            "INSERT INTO item (platform,item_id,url,kind,title,caption,uploader,"
            "duration,cover_url,cover_at,search_text,unavailable,"
            "first_seen_at,last_seen_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            params,
        )
    pk = con.execute(
        "SELECT id FROM item WHERE platform=? AND item_id=?",
        (row["platform"], row["item_id"]),
    ).fetchone()
    return (pk["id"] if pk else None), ("guncellendi" if mevcut else "eklendi")


def _lib_row_out(row, now=None):
    """DB satırını arayüz sözlüğüne çevirir.

    YouTube'da kapak HER ZAMAN null döner: istemci
    i.ytimg.com/vi/<kod>/mqdefault.jpg adresini kendisi kurar — deterministik,
    imzasız ve SÜRESİ DOLMAZ.

    Instagram kapağı ARTIK HAM CDN ADRESİ DÖNMEZ. Adres canlı olsa bile
    tarayıcı onu başka origin'den çekemiyor (hotlink koruması) ve kart rozete
    düşüyordu. Önbelleğe alınmış dosya varsa KENDİ ucumuz verilir; yoksa None
    döner ve istemci bugünkü degrade rozetine iner (kısmi önbellek regresyon
    değil, düz iyileşmedir).
    """
    now = now or int(time.time())
    yas = None
    if row["cover_at"]:
        yas = max(0, int((now - row["cover_at"]) // 86400))
    kapak = None
    if row["platform"] == "instagram" and row["cover_state"] == "ok" and row["cover_file"]:
        ad = os.path.basename(row["cover_file"])
        if _COVER_NAME_RE.fullmatch(ad):
            kapak = "/api/library/cover/" + ad
    return {
        "id": row["id"],
        "plat": row["platform"],
        "kod": row["item_id"],
        "url": row["url"],
        "tur": row["kind"],
        "baslik": row["title"],
        "aciklama": row["caption"],
        "kanal": row["uploader"],
        "kapak": kapak,
        "kapak_yas_gun": yas,
        "kategori": row["category"],
        "kategori_kaynak": row["category_src"],
        "guven": row["ai_conf"],
        "dl": row["dl_state"],
        # Kart altındaki "✕ hata" rozetinin ipucu buradan beslenir; alan
        # sunulmazsa arayüz her kalemde jenerik metin gösterirdi.
        "hata": row["dl_err"] if row["dl_state"] == "failed" else None,
        "sure": row["duration"],
    }


def _lib_kapak_ozeti(con, now):
    """/api/library/state'in kapak bölümü — tek SELECT + tur durumu."""
    r = con.execute(
        "SELECT "
        " SUM(CASE WHEN platform='instagram' AND cover_url IS NOT NULL THEN 1 ELSE 0 END) t,"
        " SUM(CASE WHEN cover_state='ok' AND cover_file IS NOT NULL THEN 1 ELSE 0 END) ok,"
        " SUM(CASE WHEN platform='instagram' AND cover_url IS NOT NULL"
        "      AND cover_state NOT IN ('ok','gone') AND cover_tries < 3 THEN 1 ELSE 0 END) eksik,"
        " SUM(CASE WHEN cover_state='gone' THEN 1 ELSE 0 END) olu,"
        " SUM(CASE WHEN platform='instagram' AND cover_url IS NOT NULL"
        "      AND cover_state <> 'ok' AND (cover_state='failed' OR cover_tries >= 3)"
        "      THEN 1 ELSE 0 END) hatali,"
        " COALESCE(SUM(cover_bytes),0) b "
        "FROM item"
    ).fetchone()
    run = con.execute(
        "SELECT state, updated_at FROM cover_run ORDER BY id DESC LIMIT 1"
    ).fetchone()
    calisiyor = bool(
        run
        and run["state"] == "running"
        and (now - (run["updated_at"] or 0)) <= COVER_HEARTBEAT_STALE
    )
    return {
        "toplam_url": r["t"] or 0,
        "onbellek": r["ok"] or 0,
        "eksik": r["eksik"] or 0,
        "olu": r["olu"] or 0,
        "hatali": r["hatali"] or 0,
        "boyut_mb": round((r["b"] or 0) / (1024.0 * 1024.0), 1),
        "calisiyor": calisiyor,
    }


@app.route("/api/library/cover/<ad>")
def api_library_cover(ad):
    """Önbelleğe alınmış kapağı sunar. VERİTABANI AÇILMAZ.

    Izgara sayfa başına 60 kart basıyor ve DOM tavanı 600: id tabanlı bir uç,
    tek kaydırma oturumunda yüzlerce SQLite bağlantısını (üstelik kapak ve AI
    işçileri yazarken) açardı. Dosya adı içerik adresli olduğu için burada
    yetkilendirme değil YALNIZCA adresleme var.

    YOL GEÇİŞİ YAPISAL OLARAK İMKÂNSIZ: ad regex'ten geçer (32 hex + bilinen
    uzantı, '/' zaten Flask'ın <ad> dönüştürücüsüne girmez), yol istemci
    metninden değil COVER_DIR'den yeniden kurulur ve son olarak realpath
    kapsama kontrolünden geçer.
    """
    if not _COVER_NAME_RE.fullmatch(ad or ""):
        return jsonify({"error": "Bulunamadı."}), 404
    tam = os.path.join(COVER_DIR, ad[:2], ad)
    try:
        kok = os.path.realpath(COVER_DIR)
        if not os.path.realpath(tam).startswith(kok + os.sep):
            return jsonify({"error": "Bulunamadı."}), 404
        if not os.path.isfile(tam):
            return jsonify({"error": "Bulunamadı."}), 404
    except OSError:
        return jsonify({"error": "Bulunamadı."}), 404

    uzanti = ad.rsplit(".", 1)[-1]
    # conditional=True → ETag/Last-Modified/304/Range bedavaya gelir.
    yanit = send_file(tam, mimetype=COVER_MIME.get(uzanti, "image/jpeg"), conditional=True)
    # İçerik adresli: aynı ad her zaman aynı bayt demektir, 'immutable' dürüsttür.
    yanit.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    return yanit


@app.route("/api/library/covers/start", methods=["POST"])
def api_library_covers_start():
    """Kapak indirme turunu başlatır (kopmuş işçi: plworker.py --cover).

    MALİYET 0 USD. Instagram'ın API'sine İSTEK ATILMAZ: yalnız veritabanında
    ZATEN kayıtlı olan cover_url'e (statik CDN dosyası) gidilir.
    """
    if request.get_json(silent=True) is None:
        return jsonify({"error": "Geçersiz istek."}), 400
    ensure_schema()
    data = request.get_json(silent=True) or {}
    istek = data.get("kapsam")
    kapsam = istek if istek in ("eksik", "basarisiz", "hepsi") else "eksik"
    limit = data.get("limit")
    try:
        limit = int(limit) if limit else None
    except (TypeError, ValueError):
        limit = None

    # KİLİT EN BAŞTA: kapsam UPDATE'i ancak kilit bizdeyken koşar.
    if not lib_acquire_covlock("cover"):
        return jsonify({"error": "Kapak indirme zaten sürüyor."}), 409

    run_id = None
    con = lib_db()
    try:
        if kapsam == "basarisiz":
            con.execute(
                "UPDATE item SET cover_state='', cover_tries=0 "
                "WHERE platform='instagram' AND cover_url IS NOT NULL "
                "AND cover_state <> 'ok' "
                "AND (cover_state='failed' OR cover_tries >= 3)"
            )
            con.commit()
        elif kapsam == "hepsi":
            # ONARIM YOLU: 'gone' dâhil her satır seçime döner; 'ok' satırlar
            # ise yalnızca DOSYASI GERÇEKTEN YOKSA sıfırlanır. Diskte duran
            # 1932 dosyayı yeniden indirmek içerik adresli isimlendirmede
            # birebir aynı baytları getirirdi — bedeli var, faydası yok.
            con.execute(
                "UPDATE item SET cover_state='', cover_tries=0 "
                "WHERE platform='instagram' AND cover_url IS NOT NULL "
                "AND cover_state <> 'ok'"
            )
            eksik_ids = []
            for r in con.execute(
                "SELECT id, cover_file FROM item WHERE cover_state='ok'"
            ):
                yol = r["cover_file"]
                if not yol or not os.path.isfile(os.path.join(COVER_DIR, yol)):
                    eksik_ids.append(r["id"])
            for i in range(0, len(eksik_ids), 500):
                dilim = eksik_ids[i:i + 500]
                con.execute(
                    "UPDATE item SET cover_state='', cover_tries=0, cover_file=NULL "
                    "WHERE id IN (%s)" % ",".join("?" * len(dilim)),
                    dilim,
                )
            con.commit()

        toplam = con.execute(
            "SELECT COUNT(*) c FROM item WHERE platform='instagram' "
            "AND cover_url IS NOT NULL AND cover_state NOT IN ('ok','gone') "
            "AND cover_tries < 3"
        ).fetchone()["c"]
        if limit and limit > 0:
            toplam = min(toplam, limit)
        if not toplam:
            lib_release_covlock()
            return jsonify({"ok": False, "sebep": "İndirilecek kapak yok."})

        try:
            os.unlink(COVER_CANCEL)
        except OSError:
            pass

        now = int(time.time())
        cur = con.execute(
            "INSERT INTO cover_run (state, total, done, failed, gone, bytes, "
            "started_at, updated_at) VALUES ('running',?,0,0,0,0,?,?)",
            (toplam, now, now),
        )
        run_id = cur.lastrowid
        con.commit()
    except sqlite3.Error:
        lib_release_covlock()
        return jsonify({"error": "Tur kaydı oluşturulamadı."}), 500
    finally:
        con.close()

    lib_stamp_covlock(run_id)

    try:
        worker = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plworker.py")
        proc = subprocess.Popen(
            [sys.executable, worker, "--cover", str(run_id)],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception:
        lib_release_covlock(run_id)
        try:
            con3 = lib_db()
            try:
                con3.execute(
                    "UPDATE cover_run SET state='failed', note=?, updated_at=? WHERE id=?",
                    ("Süreç başlatılamadı.", int(time.time()), run_id),
                )
                con3.commit()
            finally:
                con3.close()
        except sqlite3.Error:
            pass
        return jsonify({"error": "Kapak indirme süreci başlatılamadı."}), 500

    # AYRI TRY: işçi ARTIK YAŞIYOR, pid yazımı patlasa da kilit BIRAKILMAZ.
    try:
        con2 = lib_db()
        try:
            con2.execute("UPDATE cover_run SET pid=? WHERE id=?", (proc.pid, run_id))
            con2.commit()
        finally:
            con2.close()
    except sqlite3.Error:
        pass

    return jsonify({"ok": True, "run": run_id, "toplam": toplam})


@app.route("/api/library/covers/status")
def api_library_covers_status():
    try:
        ensure_schema()
        con = lib_db()
    except (sqlite3.Error, OSError):
        return jsonify({"ok": False, "sebep": "Kitaplık veritabanı açılamadı."}), 200
    try:
        row = con.execute("SELECT * FROM cover_run ORDER BY id DESC LIMIT 1").fetchone()
    finally:
        con.close()
    if not row:
        return jsonify({"ok": True, "var": False, "durum": "yok"})

    now = int(time.time())
    durum = row["state"]
    # "durmus" DB'ye YAZILMAZ; AI tarafındaki desenin aynısı, anlık üretilir.
    if durum == "running" and (now - (row["updated_at"] or 0)) > COVER_HEARTBEAT_STALE:
        # Kalp atışı geç kalmış olabilir; süreç YAŞIYORSA "durmuş" DEMEYİZ.
        # Yanlış "yanıt vermiyor" mesajı kullanıcıyı düğmeye bastırıp ikinci
        # bir işçi doğurmaya iterdi (_coverlock_stale ile aynı gerekçe).
        try:
            pid = int(row["pid"] or 0)
        except (KeyError, IndexError, TypeError, ValueError):
            pid = 0
        if not (pid and _proc_belongs_to_job(pid, str(row["id"]))):
            durum = "durmus"
    toplam = row["total"] or 0
    biten = (row["done"] or 0) + (row["failed"] or 0) + (row["gone"] or 0)
    return jsonify(
        {
            "ok": True,
            "var": True,
            "run": row["id"],
            "durum": durum,
            "toplam": toplam,
            "biten": row["done"] or 0,
            "hatali": row["failed"] or 0,
            "olu": row["gone"] or 0,
            "boyut_mb": round((row["bytes"] or 0) / (1024.0 * 1024.0), 1),
            "pct": int(min(100, (biten * 100) // toplam)) if toplam else 0,
            "not": row["note"],
        }
    )


@app.route("/api/library/covers/cancel", methods=["POST"])
def api_library_covers_cancel():
    """Yalnız işaret dosyasını yazar; SÜREÇ ÖLDÜRÜLMEZ (işçi grup başında bakar)."""
    if request.get_json(silent=True) is None:
        return jsonify({"error": "Geçersiz istek."}), 400
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(COVER_CANCEL, "w", encoding="utf-8") as f:
            f.write(str(time.time()))
    except OSError:
        return jsonify({"error": "İptal işareti yazılamadı."}), 500
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# ÖLÇÜM UCU — taksonomi kararının girdisi. AI ÇAĞRISI YOK, MALİYET 0 USD.
# ---------------------------------------------------------------------------
# Instagram'ın otomatik alt metin kalıpları sinyal DEĞİL. Bu iki yardımcı
# plworker'daki _AI_STRIP_HEAD/_ai_clean ile AYNI İŞİ yapar ve BİLEREK
# kopyalanmıştır: canlı AI yoluna dokunmadan aynı ölçüyü üretmek gerekiyordu.
# plworker.py:755-784 değişirse burası da elle güncellenmeli.
_SCAN_STRIP_HEAD = [
    re.compile(r"^\s*Photo by .*? on .*?\.\s*", re.I),
    re.compile(r"^\s*Video by .*? on .*?\.\s*", re.I),
    re.compile(r"^\s*Reel by .*? on .*?\.\s*", re.I),
    re.compile(r"^\s*Photo shared by .*? on .*?\.\s*", re.I),
]
_SCAN_HASHTAG_RE = re.compile(r"#([\wÀ-ɏ]{2,30})", re.U)
_SCAN_WORD_RE = re.compile(r"[a-z0-9çğıöşü]{3,20}")
# Sıklık listesini kirleten taşıyıcı kelimeler.
_SCAN_STOP = set(
    "the and for you your with this that from have has are was were not but all "
    "his her out who its our can will one two how why what when who com www http "
    "https jpg reel video photo instagram bir bu ve ile ama daha cok çok gibi "
    "icin için sen ben biz siz onlar var yok ne mi mu da de ki ise diye kadar "
    "sonra once önce her hic hiç ozaman şey sey olan olarak oldu olur".split()
)


def _scan_clean(text):
    t = (text or "").strip()
    for rx in _SCAN_STRIP_HEAD:
        t = rx.sub("", t, count=1)
    low = t.lower()
    i = low.find("may be an image of")
    if i < 0:
        i = low.find("may be a video of")
    if i >= 0:
        j = low.find("text that says", i)
        t = (t[:i] + ((" " + t[j:]) if j >= 0 else "")).strip()
    return re.sub(r"\s+", " ", t).strip()


@app.route("/api/library/scan")
def api_library_scan():
    """'diger' + 'belirsiz' kayıtların ÖLÇÜMÜ. Salt okunur, hiçbir satır değişmez.

    Taksonomiyi tahminden değil BU SAYILARDAN türetiyoruz: hangi kutu gerçekten
    yok, kaç satır metinsiz olduğu için zaten çözülemez, para harcanacaksa kaç
    satıra harcanacak. Kapak beyaz listesi de buradan doğrulanır.
    """
    try:
        ensure_schema()
        con = lib_db()
    except (sqlite3.Error, OSError):
        return jsonify({"ok": False, "sebep": "Kitaplık veritabanı açılamadı."}), 200
    try:
        rows = con.execute(
            "SELECT platform, title, caption, uploader, category, category_src "
            "FROM item WHERE category IN ('diger','belirsiz')"
        ).fetchall()
        hostlar = {}
        beklenmeyen = 0
        for r in con.execute(
            "SELECT cover_url FROM item WHERE cover_url IS NOT NULL"
        ):
            try:
                host = (urllib.parse.urlsplit(r["cover_url"]).hostname or "?").lower()
            except ValueError:
                host = "?"
            hostlar[host] = hostlar.get(host, 0) + 1
            if not cover_host_ok(r["cover_url"]):
                beklenmeyen += 1
    except sqlite3.Error:
        con.close()
        return jsonify({"error": "Kitaplık okunamadı."}), 500
    finally:
        try:
            con.close()
        except sqlite3.Error:
            pass

    etiketler, kelimeler = {}, {}
    kaynak = {"diger": {}, "belirsiz": {}}
    metinsiz = 0
    api_gidecek = 0
    for r in rows:
        kat = r["category"]
        src = r["category_src"] or ""
        kaynak.setdefault(kat, {})
        kaynak[kat][src] = kaynak[kat].get(src, 0) + 1

        if r["platform"] == "youtube":
            metin = re.sub(
                r"\s+", " ", " — ".join(p for p in [r["title"] or "", r["uploader"] or ""] if p)
            ).strip()
        else:
            metin = _scan_clean(r["caption"] or r["title"] or "")
        # AI_MIN_TEXT eşiği: bu satırlar API'ye HİÇ gitmez, ücretsiz 'belirsiz'
        # olur. Yani taksonomi büyüse bile ÇÖZÜLEMEZLER.
        if len(metin) < 12:
            metinsiz += 1
            continue
        api_gidecek += 1

        for h in _SCAN_HASHTAG_RE.findall(metin):
            k = fold_tr(h)
            if k:
                etiketler[k] = etiketler.get(k, 0) + 1
        for w in _SCAN_WORD_RE.findall(fold_tr(metin)):
            if w in _SCAN_STOP or w.isdigit():
                continue
            kelimeler[w] = kelimeler.get(w, 0) + 1

    def _ilk(d, n):
        return [
            {"ad": k, "adet": v}
            for k, v in sorted(d.items(), key=lambda x: (-x[1], x[0]))[:n]
        ]

    return jsonify(
        {
            "ok": True,
            "kapsam": len(rows),
            "hashtagler": _ilk(etiketler, 150),
            "kelimeler": _ilk(kelimeler, 100),
            "kaynak": kaynak,
            "metinsiz": metinsiz,
            "api_gidecek": api_gidecek,
            "tahmini_usd": round(api_gidecek * AI_ITEM_USD, 2),
            "kapak_hostlari": [
                {"host": k, "adet": v}
                for k, v in sorted(hostlar.items(), key=lambda x: (-x[1], x[0]))[:30]
            ],
            "beklenmeyen_host": beklenmeyen,
            "esik": 30,
        }
    )


@app.route("/api/library/state")
def api_library_state():
    """Kitaplığın özeti. Kitaplık boşsa da, AI kapalıysa da 200 döner."""
    try:
        ensure_schema()
        con = lib_db()
    except (sqlite3.Error, OSError):
        return jsonify({"ok": False, "sebep": "Kitaplık veritabanı açılamadı."}), 200
    try:
        now = int(time.time())
        toplam = con.execute("SELECT COUNT(*) c FROM item").fetchone()["c"]
        bekleyen = con.execute(
            "SELECT COUNT(*) c FROM item WHERE ai_state='pending' AND category IS NULL "
            "AND ai_tries < 3"
        ).fetchone()["c"]
        # ÇIKMAZ SAYACI: bu satırlar seçim sorgusuna ARTIK GİRMİYOR (ai_state
        # 'failed' ya da ai_tries kapıya dayandı). Sayı görünmezse geçersiz bir
        # API anahtarı 951 kaydı sessizce öldürür ve arayüz "kayıt yok" der.
        basarisiz = con.execute(
            "SELECT COUNT(*) c FROM item WHERE category IS NULL "
            "AND category_src <> 'kullanici' "
            "AND (ai_state='failed' OR (ai_state='pending' AND ai_tries >= 3))"
        ).fetchone()["c"]

        sayim = {
            r["category"]: r["c"]
            for r in con.execute(
                "SELECT category, COUNT(*) c FROM item GROUP BY category"
            )
        }
        kategoriler = [
            {"kod": kod, "ad": ad, "adet": sayim.get(kod, 0), "atanabilir": True}
            for kod, ad in KATEGORILER
        ]
        # GÜVENLİK AĞI: DB'de olup listede olmayan kod (ör. taksonomiden bir
        # kalem düşürülmüşse) SESSİZCE GİZLENİRDİ — /items filtresi
        # `cat in KATEGORI_KODLARI` dediği için o satırlar hiçbir filtreyle
        # bulunamaz, çipler toplamı "Tümü"yü tutmaz. Görünür kılıyoruz.
        for kod, adet in sayim.items():
            if kod is None or kod in KATEGORI_ADLARI or not adet:
                continue
            # atanabilir=False: bu kod /api/library/category'nin beyaz listesinde
            # DEĞİL. Elle atama menüsüne düşerse seçilebilir görünür ama
            # "Kategori ata" 400 döner — çip olarak kalsın, menüye girmesin.
            kategoriler.append({
                "kod": kod, "ad": "bilinmeyen: " + str(kod),
                "adet": adet, "atanabilir": False,
            })
        kategoriler.append(
            {"kod": "_yok", "ad": "Kategorisiz", "adet": sayim.get(None, 0),
             "atanabilir": False}
        )

        # 'diger' + 'belirsiz' — yeniden sınıflandırma kapsamının büyüklüğü.
        ai_yeniden = con.execute(
            "SELECT COUNT(*) c FROM item WHERE category IN ('diger','belirsiz') "
            "AND category_src <> 'kullanici'"
        ).fetchone()["c"]

        kapak = _lib_kapak_ozeti(con, now)

        koleksiyonlar = [
            {"id": r["id"], "ad": r["name"], "adet": r["c"]}
            for r in con.execute(
                "SELECT c.id, c.name, COUNT(ci.item_id) c FROM collection c "
                "LEFT JOIN collection_item ci ON ci.collection_id = c.id "
                "GROUP BY c.id ORDER BY c.name"
            )
        ]
        platform = {
            r["platform"]: r["c"]
            for r in con.execute("SELECT platform, COUNT(*) c FROM item GROUP BY platform")
        }
        dl = {
            r["dl_state"]: r["c"]
            for r in con.execute("SELECT dl_state, COUNT(*) c FROM item GROUP BY dl_state")
        }

        kapak_row = con.execute("SELECT MAX(cover_at) m FROM item").fetchone()
        kapak_yas = None
        if kapak_row and kapak_row["m"]:
            kapak_yas = max(0, int((now - kapak_row["m"]) // 86400))

        run = con.execute(
            "SELECT state, updated_at FROM ai_run ORDER BY id DESC LIMIT 1"
        ).fetchone()
        calisiyor = bool(
            run
            and run["state"] == "running"
            and (now - (run["updated_at"] or 0)) <= AI_HEARTBEAT_STALE
        )

        return jsonify(
            {
                "ok": True,
                "toplam": toplam,
                "ai_acik": ai_enabled(),
                "ai_bekleyen": bekleyen,
                "ai_basarisiz": basarisiz,
                # ESKİ ALANLAR KORUNUR: dağıtımdan sonra açık kalan eski bir
                # sekme yeni API ile konuşabilmeli.
                "kapak_yas_gun": kapak_yas,
                "kapak_eski": bool(kapak_yas is not None and kapak_yas > 3),
                "kapak": kapak,
                "ai_yeniden": ai_yeniden,
                "kategoriler": kategoriler,
                "koleksiyonlar": koleksiyonlar,
                "platform": {
                    "instagram": platform.get("instagram", 0),
                    "youtube": platform.get("youtube", 0),
                },
                "dl": {
                    "none": dl.get("none", 0),
                    "ready": dl.get("ready", 0),
                    "failed": dl.get("failed", 0),
                },
                "ai": {"calisiyor": calisiyor},
            }
        )
    finally:
        con.close()


@app.route("/api/library/import", methods=["POST"])
def api_library_import():
    """Tarayıcı betiğinin ürettiği JSON'u parça parça alır (dosya yüklemesi YOK).

    TEK DOĞRULAMA KAYNAĞI canonical_link(): betiğin gönderdiği "kod"/"tur"
    alanlarına GÜVENİLMEZ, yok sayılır.
    """
    ensure_schema()
    data = request.get_json(silent=True) or {}
    kaynak = str(data.get("kaynak") or "ig_saved")
    if kaynak not in ("ig_saved", "yt_playlist", "manual"):
        return jsonify({"error": "Bilinmeyen kaynak."}), 400
    ad = (str(data.get("koleksiyon") or "").strip() or "Kaydedilenler")[:120]
    ext_id = str(data.get("ext_id") or "")[:120]
    try:
        parca = max(0, int(data.get("parca") or 0))
    except (TypeError, ValueError):
        parca = 0
    kayitlar = data.get("kayitlar")

    if not isinstance(kayitlar, list) or not kayitlar:
        return jsonify({"error": "Aktarılacak kayıt yok."}), 400
    if len(kayitlar) > LIB_IMPORT_MAX:
        return (
            jsonify(
                {"error": f"Tek parçada en fazla {LIB_IMPORT_MAX} kayıt gönderilebilir."}
            ),
            400,
        )

    okunan = len(kayitlar)
    eklenen = guncellenen = yinelenen = 0
    reddedilen = []
    gorulen = set()

    con = lib_db()
    try:
        # BEGIN IMMEDIATE ŞART: düz (deferred) BEGIN önce _lib_collection'ın
        # SELECT'iyle okuma anlık görüntüsü alır; araya AI işçisinin commit'i
        # girerse yazma yükseltmesi SQLITE_BUSY_SNAPSHOT ile döner ve bu hatada
        # busy_timeout HİÇ ÇAĞRILMAZ → 200 kayıtlık parça anında 500 olurdu.
        con.execute("BEGIN IMMEDIATE")
        koleksiyon_id = _lib_collection(con, kaynak, ext_id, ad)

        for sira, kayit in enumerate(kayitlar):
            if not isinstance(kayit, dict):
                reddedilen.append({"raw": str(kayit)[:200], "reason": "Kayıt biçimi bozuk."})
                continue
            ham = kayit.get("url")
            if not isinstance(ham, str) or not ham.strip():
                reddedilen.append({"raw": str(ham)[:200], "reason": "Bağlantı yok."})
                continue

            url, platform, item_id, reason = canonical_link(ham)
            if reason or not url or not item_id or not _ITEM_ID_RE.fullmatch(item_id):
                reddedilen.append(
                    {"raw": ham[:200], "reason": reason or "Desteklenmeyen site."}
                )
                continue
            if len(url) > LIB_URL_MAX:
                reddedilen.append({"raw": ham[:200], "reason": "Bağlantı çok uzun."})
                continue

            anahtar = (platform, item_id)
            if anahtar in gorulen:
                yinelenen += 1
                continue
            gorulen.add(anahtar)

            if platform == "instagram":
                ref = instagram_post_ref(url)
                tur = ref[0] if ref else ""
            else:
                tur = "video"

            kapak = kayit.get("kapak")
            if (
                not isinstance(kapak, str)
                or not kapak.startswith("https://")
                or len(kapak) > LIB_COVER_MAX
            ):
                kapak = None
            # YouTube kapağı deterministik türetilir; imzalı adres saklamayız.
            if platform == "youtube":
                kapak = None

            aciklama = kayit.get("aciklama")
            aciklama = aciklama[:LIB_CAPTION_MAX] if isinstance(aciklama, str) else ""
            baslik = kayit.get("baslik")
            baslik = baslik[:300] if isinstance(baslik, str) else ""
            kanal = kayit.get("kanal")
            kanal = kanal[:200] if isinstance(kanal, str) else ""
            try:
                sure = int(kayit.get("sure")) if kayit.get("sure") else None
            except (TypeError, ValueError):
                sure = None
            if sure is not None and (sure < 0 or sure > 100 * 3600):
                sure = None

            satir = {
                "platform": platform,
                "item_id": item_id,
                "url": url,
                "kind": tur,
                "title": baslik,
                "caption": aciklama,
                "uploader": kanal,
                "duration": sure,
                "cover_url": kapak,
                "cover_at": int(time.time()) if kapak else None,
                "unavailable": bool(kayit.get("yok")),
            }

            # TEK BOZUK KALEM 200'LÜK PARÇAYI DÜŞÜRMEZ.
            try:
                mevcut = con.execute(
                    "SELECT * FROM item WHERE platform=? AND item_id=?",
                    (platform, item_id),
                ).fetchone()
                pk, sonuc = _lib_upsert_item(con, satir, mevcut)
                if sonuc == "eklendi":
                    eklenen += 1
                else:
                    guncellenen += 1
                if pk:
                    con.execute(
                        "INSERT INTO collection_item (collection_id,item_id,pos,added_at) "
                        "VALUES (?,?,?,?) "
                        "ON CONFLICT(collection_id,item_id) DO UPDATE SET pos = excluded.pos"
                        if _LIB_HAS_UPSERT
                        else "INSERT OR REPLACE INTO collection_item "
                        "(collection_id,item_id,pos,added_at) VALUES (?,?,?,?)",
                        (
                            koleksiyon_id,
                            pk,
                            parca * LIB_IMPORT_MAX + sira,
                            int(time.time()),
                        ),
                    )
            except sqlite3.IntegrityError:
                reddedilen.append({"raw": ham[:200], "reason": "Kayıt veritabanına yazılamadı."})

        con.commit()
        toplam = con.execute("SELECT COUNT(*) c FROM item").fetchone()["c"]
    except sqlite3.Error:
        con.rollback()
        con.close()
        return jsonify({"error": "Kitaplığa yazılamadı."}), 500
    finally:
        try:
            con.close()
        except sqlite3.Error:
            pass

    return jsonify(
        {
            "ok": True,
            "parca": parca,
            "okunan": okunan,
            "eklenen": eklenen,
            "guncellenen": guncellenen,
            "yinelenen": yinelenen,
            "reddedilen": reddedilen[:50],
            "koleksiyon_id": koleksiyon_id,
            "toplam": toplam,
        }
    )


@app.route("/api/library/items")
def api_library_items():
    ensure_schema()
    q = (request.args.get("q") or "").strip()
    plat = (request.args.get("plat") or "all").strip()
    cat = (request.args.get("cat") or "").strip()
    dl = (request.args.get("dl") or "all").strip()
    col = (request.args.get("col") or "").strip()
    sort = (request.args.get("sort") or "yeni").strip()
    try:
        offset = max(0, int(request.args.get("offset") or 0))
    except ValueError:
        offset = 0
    try:
        limit = int(request.args.get("limit") or LIB_PAGE_DEFAULT)
    except ValueError:
        limit = LIB_PAGE_DEFAULT
    limit = max(1, min(LIB_PAGE_MAX, limit))

    where = []
    args = []
    if plat in ("instagram", "youtube"):
        where.append("i.platform = ?")
        args.append(plat)
    if cat == "_yok":
        where.append("i.category IS NULL")
    elif cat and _CAT_KOD_RE.fullmatch(cat):
        # LİSTE DIŞI KODLAR DA FİLTRELENİR. Eskiden koşul `cat in
        # KATEGORI_KODLARI` idi: /state'in "bilinmeyen: X" çipi (taksonomiden
        # düşmüş bir kod) tıklanınca WHERE'e HİÇBİR koşul eklenmiyor ve sorgu
        # FİLTRESİZ koşuyordu — çip 'aktif' görünüp tüm kitaplığı listeliyordu.
        # Sorgu parametreli olduğu için enjeksiyon riski yok; regex yalnızca
        # kod biçimini (kısa, ASCII) sınırlar.
        where.append("i.category = ?")
        args.append(cat)
    if dl in ("none", "ready", "failed"):
        where.append("i.dl_state = ?")
        args.append(dl)
    if q:
        where.append("i.search_text LIKE ? ESCAPE '\\'")
        args.append("%" + _lib_like_escape(fold_tr(q)) + "%")

    join = ""
    if col:
        try:
            join = " JOIN collection_item ci ON ci.item_id = i.id AND ci.collection_id = ?"
            args.insert(0, int(col))
        except ValueError:
            join = ""

    sql_where = (" WHERE " + " AND ".join(where)) if where else ""
    if sort == "eski":
        order = " ORDER BY i.first_seen_at ASC, i.id ASC"
    elif sort == "kategori":
        order = " ORDER BY (i.category IS NULL), i.category, i.first_seen_at DESC, i.id DESC"
    else:
        order = " ORDER BY i.first_seen_at DESC, i.id DESC"

    con = lib_db()
    try:
        toplam = con.execute(
            "SELECT COUNT(*) c FROM item i" + join + sql_where, args
        ).fetchone()["c"]
        rows = con.execute(
            "SELECT i.* FROM item i" + join + sql_where + order + " LIMIT ? OFFSET ?",
            args + [limit, offset],
        ).fetchall()
    except sqlite3.Error:
        con.close()
        return jsonify({"error": "Kitaplık okunamadı."}), 500
    finally:
        try:
            con.close()
        except sqlite3.Error:
            pass

    now = int(time.time())
    return jsonify(
        {
            "ok": True,
            "toplam": toplam,
            "offset": offset,
            "limit": limit,
            "kayitlar": [_lib_row_out(r, now) for r in rows],
        }
    )


@app.route("/api/library/mark", methods=["POST"])
def api_library_mark():
    """İndirme defteri. finishJob(st) TEK SATIRLA çağırır.

    'ready' = GEÇMİŞTE BİR KEZ İNDİRİLDİ demektir; "dosya sunucuda duruyor"
    DEMEK DEĞİLDİR (janitor TTL_DONE sonrası iş klasörünü siler).
    Kitaplıkta bulunmayan (platform,kod) çifti SESSİZCE atlanır — böylece
    oynatma listesi ve yapıştırma işleri de bedavaya işaretlenir.
    """
    ensure_schema()
    data = request.get_json(silent=True) or {}
    job = str(data.get("job") or "")[:32]
    sonuclar = data.get("sonuclar")
    if not isinstance(sonuclar, list):
        return jsonify({"error": "Sonuç listesi bozuk."}), 400

    now = int(time.time())
    guncellenen = 0
    con = lib_db()
    try:
        con.execute("BEGIN")
        for s in sonuclar[:LIB_IDS_MAX]:
            if not isinstance(s, dict):
                continue
            plat = s.get("plat") or "youtube"
            kod = s.get("kod")
            durum = s.get("durum")
            if plat not in ("youtube", "instagram") or not isinstance(kod, str):
                continue
            if durum not in ("ready", "failed"):
                continue
            boyut = s.get("boyut")
            try:
                boyut = int(boyut) if boyut else None
            except (TypeError, ValueError):
                boyut = None
            hata = s.get("hata")
            hata = hata[:300] if isinstance(hata, str) else None
            cur = con.execute(
                "UPDATE item SET dl_state=?, dl_job=?, dl_at=?, dl_size=?, dl_err=?, "
                "dl_count = dl_count + ? WHERE platform=? AND item_id=?",
                (
                    durum, job, now, boyut, hata,
                    1 if durum == "ready" else 0,
                    plat, kod,
                ),
            )
            guncellenen += cur.rowcount or 0
        con.commit()
    except sqlite3.Error:
        con.rollback()
        con.close()
        return jsonify({"error": "Defter yazılamadı."}), 500
    finally:
        try:
            con.close()
        except sqlite3.Error:
            pass
    return jsonify({"ok": True, "guncellenen": guncellenen})


def _lib_id_list(data):
    ids = data.get("ids")
    if not isinstance(ids, list) or not ids:
        return None
    out = []
    for x in ids[:LIB_IDS_MAX]:
        try:
            out.append(int(x))
        except (TypeError, ValueError):
            return None
    return out


@app.route("/api/library/category", methods=["POST"])
def api_library_category():
    """Elle kategori atama.

    category_src='kullanici' olan satır SONRAKİ HİÇBİR AI TURUNDA seçilmez
    (seçim sorgusu ai_state='pending' bakıyor, burada 'skipped' yapılıyor).
    Yapısal bağışıklık: elle iş asla ezilmez.
    """
    ensure_schema()
    data = request.get_json(silent=True) or {}
    ids = _lib_id_list(data)
    kategori = str(data.get("kategori") or "")
    if ids is None:
        return jsonify({"error": "Geçersiz kayıt listesi."}), 400
    if kategori not in KATEGORI_KODLARI:
        return jsonify({"error": "Bilinmeyen kategori."}), 400

    yer = ",".join("?" * len(ids))
    con = lib_db()
    try:
        cur = con.execute(
            f"UPDATE item SET category=?, category_src='kullanici', ai_state='skipped', "
            f"ai_at=? WHERE id IN ({yer})",
            [kategori, int(time.time())] + ids,
        )
        con.commit()
        return jsonify({"ok": True, "guncellenen": cur.rowcount or 0})
    except sqlite3.Error:
        con.rollback()
        return jsonify({"error": "Kategori yazılamadı."}), 500
    finally:
        con.close()


@app.route("/api/library/delete", methods=["POST"])
def api_library_delete():
    ensure_schema()
    data = request.get_json(silent=True) or {}
    ids = _lib_id_list(data)
    if ids is None:
        return jsonify({"error": "Geçersiz kayıt listesi."}), 400
    yer = ",".join("?" * len(ids))
    con = lib_db()
    try:
        # collection_item satırları ON DELETE CASCADE ile gider
        # (PRAGMA foreign_keys=ON bağlantı başına açık).
        cur = con.execute(f"DELETE FROM item WHERE id IN ({yer})", ids)
        con.commit()
        return jsonify({"ok": True, "silinen": cur.rowcount or 0})
    except sqlite3.Error:
        con.rollback()
        return jsonify({"error": "Silinemedi."}), 500
    finally:
        con.close()


@app.route("/api/library/ai/start", methods=["POST"])
def api_library_ai_start():
    """AI kategorilendirme turunu başlatır (kopmuş işçi: plworker.py --ai).

    ANAHTAR YOKKEN 500 DEĞİL 200 + {"ok": false} döner.
    İNDİRME KİLİDİNE (.joblock) HİÇ DOKUNULMAZ.
    """
    ensure_schema()
    if not ai_enabled():
        return jsonify(
            {"ok": False, "sebep": "AI kapalı (ANTHROPIC_API_KEY tanımlı değil)."}
        )

    data = request.get_json(silent=True) or {}
    istek = data.get("kapsam")
    if istek == "hepsi":
        kapsam = "hepsi"
    elif istek == "basarisiz":
        kapsam = "basarisiz"
    elif istek == "yeniden":
        kapsam = "yeniden"
    else:
        kapsam = "kategorisiz"
    limit = data.get("limit")
    try:
        limit = int(limit) if limit else None
    except (TypeError, ValueError):
        limit = None

    # KİLİT EN BAŞTA ALINIR. Eskiden kapsam='hepsi' TÜM kategorileri kilit
    # denemesinden ÖNCE siliyordu: 409 dönen bir istek bile kitaplığı
    # boşaltıyor ve o emek geri gelmiyordu.
    if not lib_acquire_ailock("ai"):
        return jsonify({"error": "Kategorilendirme zaten sürüyor."}), 409

    run_id = None
    con = lib_db()
    try:
        if kapsam == "hepsi":
            # Elle atanmış kategoriler (category_src='kullanici') KORUNUR.
            con.execute(
                "UPDATE item SET ai_state='pending', ai_tries=0, category=NULL "
                "WHERE category_src <> 'kullanici'"
            )
            con.commit()
        elif kapsam == "basarisiz":
            # ÇIKMAZDAN TEK ÇIKIŞ: 'failed' ve ai_tries kapısına dayanmış
            # kalemleri seçime geri koyar. Kategorisi olan hiçbir satıra
            # dokunmaz (elle atananlar da category_src ile ayrıca korunur).
            con.execute(
                "UPDATE item SET ai_state='pending', ai_tries=0 "
                "WHERE category IS NULL AND category_src <> 'kullanici' "
                "AND (ai_state='failed' OR (ai_state='pending' AND ai_tries >= 3))"
            )
            con.commit()
        elif kapsam == "yeniden":
            # YIKICI DEĞİL: category'ye HİÇ DOKUNULMAZ. 'hepsi' kapsamı
            # category=NULL yazdığı için tek bir başarılı çağrı olmadan da
            # 1195 kaydın kategorisini siler ve geri alma YOKTUR. Burada ölü
            # anahtar / iptal / çökme hâlinde kaybedilen tek şey ai_state ve
            # ai_tries'tır: bilgi taşımayan, kendiliğinden düzelen alanlar.
            con.execute(
                "UPDATE item SET ai_state='pending', ai_tries=0 "
                "WHERE category IN ('diger','belirsiz') AND category_src <> 'kullanici'"
            )
            con.commit()

        if kapsam == "yeniden":
            toplam = con.execute(
                "SELECT COUNT(*) c FROM item WHERE ai_state='pending' AND ai_tries < 3 "
                "AND category IN ('diger','belirsiz') AND category_src <> 'kullanici'"
            ).fetchone()["c"]
        else:
            toplam = con.execute(
                "SELECT COUNT(*) c FROM item WHERE ai_state='pending' AND category IS NULL "
                "AND ai_tries < 3"
            ).fetchone()["c"]
        if limit and limit > 0:
            toplam = min(toplam, limit)
        if not toplam:
            lib_release_ailock()
            return jsonify({"ok": False, "sebep": "Kategorilendirilecek kayıt yok."})

        try:
            os.unlink(AI_CANCEL)
        except OSError:
            pass

        now = int(time.time())
        cur = con.execute(
            "INSERT INTO ai_run (state, model, total, done, failed, batches, "
            "started_at, updated_at, scope) VALUES ('running',?,?,0,0,0,?,?,?)",
            (AI_MODEL, toplam, now, now, kapsam),
        )
        run_id = cur.lastrowid
        con.commit()
    except sqlite3.Error:
        # Kilit KESİNLİKLE bizim (yukarıda aldık): başkasının turunu düşürmüyoruz.
        lib_release_ailock()
        return jsonify({"error": "Tur kaydı oluşturulamadı."}), 500
    finally:
        con.close()

    # Sahiplik damgası: bundan sonra kilidi YALNIZ bu tur bırakabilir.
    lib_stamp_ailock(run_id)

    try:
        worker = os.path.join(os.path.dirname(os.path.abspath(__file__)), "plworker.py")
        proc = subprocess.Popen(
            [sys.executable, worker, "--ai", str(run_id)],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception:
        lib_release_ailock(run_id)
        try:
            con3 = lib_db()
            try:
                con3.execute(
                    "UPDATE ai_run SET state='failed', note=?, updated_at=? WHERE id=?",
                    ("Süreç başlatılamadı.", int(time.time()), run_id),
                )
                con3.commit()
            finally:
                con3.close()
        except sqlite3.Error:
            pass
        return jsonify({"error": "Kategorilendirme süreci başlatılamadı."}), 500

    # AYRI TRY: işçi ARTIK YAŞIYOR. pid yazımı patlarsa kilidi BIRAKMAYIZ —
    # bırakırsak sağlıklı bir turun üstüne ikinci işçi doğardı. pid zaten
    # işçinin kendi ilk yazımıyla da düşer.
    try:
        con2 = lib_db()
        try:
            con2.execute("UPDATE ai_run SET pid=? WHERE id=?", (proc.pid, run_id))
            con2.commit()
        finally:
            con2.close()
    except sqlite3.Error:
        pass

    return jsonify(
        {
            "ok": True,
            "run": run_id,
            "toplam": toplam,
            "parti": int(math.ceil(toplam / float(AI_BATCH))),
        }
    )


@app.route("/api/library/ai/status")
def api_library_ai_status():
    ensure_schema()
    con = lib_db()
    try:
        row = con.execute(
            "SELECT * FROM ai_run ORDER BY id DESC LIMIT 1"
        ).fetchone()
    finally:
        con.close()
    if not row:
        return jsonify({"ok": True, "var": False})

    now = int(time.time())
    durum = row["state"]
    # "durmus" DOSYAYA YAZILMAZ: playlist'teki 'stale' deseninin aynısı,
    # yanıtta ANLIK üretilir.
    if durum == "running" and (now - (row["updated_at"] or 0)) > AI_HEARTBEAT_STALE:
        durum = "durmus"

    toplam = row["total"] or 0
    biten = (row["done"] or 0) + (row["failed"] or 0)

    return jsonify(
        {
            "ok": True,
            "var": True,
            "run": row["id"],
            "durum": durum,
            "toplam": toplam,
            "biten": row["done"] or 0,
            "hatali": row["failed"] or 0,
            "parti": row["batches"] or 0,
            "parti_toplam": int(math.ceil(toplam / float(AI_BATCH))) if toplam else 0,
            "pct": int(min(100, (biten * 100) // toplam)) if toplam else 0,
            "not": row["note"],
        }
    )


@app.route("/api/library/ai/cancel", methods=["POST"])
def api_library_ai_cancel():
    """İşçi PARTİLER ARASINDA bu dosyaya bakar. SÜREÇ ÖLDÜRÜLMEZ: uçuştaki
    API çağrısının bitmesi beklenir (yarım transaction bırakmamak için)."""
    # JSON GÖVDESİ ZORUNLU: diğer tüm POST uçlarıyla aynı sözleşme. Gövdeye
    # hiç bakmayan uç, siteler arası düz bir <form> POST'uyla tetiklenebiliyordu
    # (application/json ön-uçuş gerektirir, urlencoded gerektirmez).
    if request.get_json(silent=True) is None:
        return jsonify({"error": "Geçersiz istek."}), 400
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(AI_CANCEL, "w", encoding="utf-8") as f:
            f.write(str(time.time()))
    except OSError:
        return jsonify({"error": "İptal işareti yazılamadı."}), 500
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
