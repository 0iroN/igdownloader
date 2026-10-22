"""Oynatma listesi işçisi.

Web katmanından KOPUK çalışır: /api/playlist/start bu dosyayı
`subprocess.Popen([sys.executable, "plworker.py", <job_id>], start_new_session=True)`
ile başlatır ve hemen yanıt döner. Böylece indirme hiçbir HTTP isteğinin
içinde yapılmaz (gunicorn timeout'u devre dışı kalır) ve takılan bir ffmpeg
süreç grubuyla birlikte tek hamlede durdurulabilir.

Durum yalnızca DİSKTE tutulur: pl_<job>/status.json. Bu dosyanın TEK YAZARI
bu süreçtir; web katmanı yalnızca okur (iptal için ayrı cancel.flag dosyası
kullanır).
"""

import os
import re
import sys
import glob
import time
import random
import signal
import threading

from yt_dlp import YoutubeDL

import app as web  # ortak sabitler, yol yardımcıları ve build_opts


class _Cancelled(Exception):
    """Kullanıcı iptal etti ya da sekme kapandı."""


class _ItemTimeout(Exception):
    """Tek girdi için duvar saati sınırı aşıldı."""


class _TooBig(Exception):
    """Dosya, kalan disk bütçesini aşıyor."""


def _budget_left(status):
    """Bu girdiye harcanabilecek en fazla bayt (iş kotası ∧ serbest alan)."""
    job_left = web.MAX_JOB_BYTES - int(status.get("bytes") or 0)
    disk_left = web.free_bytes() - web.RESERVE_FREE
    return max(0, min(job_left, disk_left))


# SIGTERM ile durdurulduğumuzda mevcut girdiyi bırakıp temiz çıkalım.
_STOP = {"flag": False}


def _on_sigterm(_signum, _frame):
    _STOP["flag"] = True


# Ağ kaynaklı (tekrar denemeye değer) ve kesin (denemeye değmez) hatalar
_RETRYABLE = ("timed out", "timeout", "connection", "fragment", "temporary", "http error 5")
_DETERMINISTIC = (
    "private video",
    "members-only",
    "members only",
    "age",
    "removed",
    "unavailable",
    "not available",
    "copyright",
    "geo",
)
# Devre kesiciyi tetikleyen imzalar. YouTube tarafı ("sign in", "bot", ...)
# bugünküyle birebir aynı; Instagram'ın GERÇEK engel metinleri eklendi —
# "Requested content is not available, rate-limit reached or login required"
# eskiden bu listenin hiçbir imzasına uymuyordu ve kesici hiç tetiklenmiyordu.
_BOTLIKE = (
    "sign in",
    "bot",
    "cookies",
    "confirm you",
    "rate-limit",
    "rate limit",
    "too many requests",
    "429",
    "login required",
    "login_required",
    "empty media response",
    "checkpoint",
    "challenge_required",
)
# Instagram fotoğraf gönderisi ses modunda: alınacak ses akışı yok. Bu bir
# arıza değil, içerik türü — 'failed' sayacına yazmıyoruz.
# NOT: çıplak "postprocessing" imzası BİLEREK yok. Fotoğraf gönderisinde ffmpeg
# mesajı zaten "Postprocessing: ... does not contain any stream" biçiminde geldiği
# için tespit bozulmaz; çıplak imza ise gerçek bir ffmpeg arızasını "fotoğraf
# gönderisi" diye etiketleyip kalemi tekrar denenemez hâle getiriyordu.
_NO_MEDIA = (
    "does not contain any stream",
    "no audio",
    "no video formats found",
    "there is no video in this post",
)


def _is_retryable(err):
    msg = str(err).lower()
    if any(k in msg for k in _DETERMINISTIC):
        return False
    return any(k in msg for k in _RETRYABLE)


def _is_botlike(err):
    msg = str(err).lower()
    return any(k in msg for k in _BOTLIKE)


def _is_media_missing(err):
    msg = str(err).lower()
    return any(k in msg for k in _NO_MEDIA)


def _bot_note(platform):
    """Üst üste bot benzeri hata alan platform için Türkçe gerekçe."""
    if platform == "instagram":
        return (
            "Instagram üst üste giriş/doğrulama istedi. cookies.txt dosyasını "
            "yenileyip tekrar deneyin."
        )
    return (
        "YouTube üst üste doğrulama istedi. cookies.txt dosyasını "
        "yenileyip tekrar deneyin."
    )


def _cancel_requested(job_id, status):
    """İptal bayrağı, SIGTERM ya da 'kimse izlemiyor' durumu.

    'Kimse izlemiyor' eşiği (NO_POLL_TIMEOUT) bilerek uzun tutulur: tarayıcı
    sekmesi arka plana atılınca setTimeout zinciri donuyor ve kısa bir eşik
    sağlıklı işleri kendi kendine iptal ettiriyordu.
    """
    if _STOP["flag"] or os.path.exists(web.job_cancel_path(job_id)):
        return True
    started = status.get("started_at") or time.time()
    try:
        last = max(os.path.getmtime(web.job_lastread_path(job_id)), started)
    except OSError:
        last = started
    return (time.time() - last) > web.NO_POLL_TIMEOUT


def _pick_file(files_dir, prefix):
    """Girdiye ait bitmiş dosyayı seç; ara parçaları ele.

    Döndürülen: (secilen_yol, silinecek_artiklar)
    """
    candidates = []
    for path in glob.glob(os.path.join(files_dir, prefix + ".*")):
        name = os.path.basename(path)
        if name.endswith((".part", ".ytdl", ".temp")):
            continue
        # 007-ID.f137.mp4 gibi birleştirilmemiş tek akış artıkları
        if re.search(r"\.f\d+\.", name):
            continue
        candidates.append(path)
    if not candidates:
        return None, []
    best = max(candidates, key=os.path.getsize)
    return best, [p for p in candidates if p != best]


def _sweep_leftovers(files_dir, prefix, keep):
    """Girdiye ait yarım/ara dosyaları sil (disk muhasebesi doğru kalsın)."""
    for path in glob.glob(os.path.join(files_dir, prefix + ".*")):
        if keep and os.path.abspath(path) == os.path.abspath(keep):
            continue
        try:
            os.remove(path)
        except OSError:
            pass


def _heartbeat(job_id, live, stop_event):
    """status.json'un mtime'ını taze tut: uzun ffmpeg birleştirmesi sırasında
    iş yanlışlıkla 'stale' görünmesin.

    AMA koşulsuz değil: son NO_PROGRESS_STALL süresinde hiç ilerleme olmadıysa
    susarız. Aksi hâlde takılmış bir ffmpeg'i sonsuza kadar "sapasağlam" gösterip
    janitor'ın ve devralma yollarının önünü kapatıyordu.
    """
    path = web.job_status_path(job_id)
    while not stop_event.wait(20):
        if (time.monotonic() - live.get("progress_at", 0)) > web.NO_PROGRESS_STALL:
            continue
        try:
            os.utime(path, None)
        except OSError:
            return


def _hard_stop(job_id, state, note):
    """Hook'lara güvenmeden süreç GRUBUNU (ffmpeg dahil) durdur.

    yt-dlp hook'ları yalnızca indirme/son işlem adımlarında çağrılır; ffmpeg
    birleştirmesi/dönüştürmesi sırasında iptal ve süre aşımı hiç görülmez.
    Bu yüzden bağımsız bir izleyici thread'den çağrılır.
    """
    try:
        status = web.read_status(job_id)
        if status:
            status["state"] = state
            status["note"] = note
            status["current"] = None
            for item in status.get("items") or []:
                if item.get("state") == "downloading":
                    item["state"] = "queued"
            web.write_status_atomic(job_id, status)
    except Exception:  # noqa: BLE001 - durdurma yolu asla patlamamalı
        pass
    try:
        web.release_joblock(job_id)
    except Exception:  # noqa: BLE001
        pass
    # Süreç grubunu ancak GRUP LİDERİ isek öldürürüz (start_new_session=True
    # bunu garanti eder). Aksi hâlde bizi başlatan kabuğu da öldürürdük.
    try:
        if os.getpgid(0) == os.getpid():
            os.killpg(os.getpgid(0), signal.SIGKILL)
    except (OSError, AttributeError):
        pass
    os._exit(1)


def _watchdog(job_id, live, stop_event):
    """İptal / süre aşımı / 'iş klasörü silinmiş' durumlarını hook'lardan
    BAĞIMSIZ izler."""
    while not stop_event.wait(5):
        if not os.path.isdir(web.job_files_dir(job_id)):
            # Kullanıcı 'Sunucudan sil' dedi: hayalet işçi klasörü yeniden
            # yaratmadan çıksın.
            _hard_stop(job_id, "cancelled", "İş silindi.")
            return

        if _STOP["flag"] or os.path.exists(web.job_cancel_path(job_id)):
            if live.get("cancel_seen") is None:
                live["cancel_seen"] = time.monotonic()
            elif (time.monotonic() - live["cancel_seen"]) > web.CANCEL_GRACE:
                _hard_stop(job_id, "cancelled", "İptal edildi (süreç durduruldu).")
                return
        else:
            live["cancel_seen"] = None

        started = live.get("item_started")
        if started is not None:
            limit = web.ITEM_TIMEOUT + web.ITEM_TIMEOUT_HARD
            if (time.monotonic() - started) > limit:
                _hard_stop(job_id, "partial", "Bir video takıldı, iş durduruldu.")
                return


def run(job_id):
    status = web.read_status(job_id)
    if not status:
        return 1

    files_dir = web.job_files_dir(job_id)
    os.makedirs(files_dir, exist_ok=True)

    status["pid"] = os.getpid()
    try:
        status["pgid"] = os.getpgid(0)
    except (OSError, AttributeError):
        status["pgid"] = None
    status["state"] = "running"
    status["note"] = None

    last_write = {"t": 0.0}
    # İzleyici thread'lerle paylaşılan canlı durum (monotonik saat: makine
    # uykuya girip uyanınca duvar saati sıçraması bizi yanıltmasın).
    live = {"progress_at": time.monotonic(), "item_started": None, "cancel_seen": None}

    def save(force=True):
        live["progress_at"] = time.monotonic()
        if not force and (time.time() - last_write["t"]) < 1.2:
            return
        last_write["t"] = time.time()
        try:
            web.write_status_atomic(job_id, status)
        except OSError:
            pass

    save()

    stop_event = threading.Event()
    beat = threading.Thread(
        target=_heartbeat, args=(job_id, live, stop_event), daemon=True
    )
    beat.start()
    guard = threading.Thread(
        target=_watchdog, args=(job_id, live, stop_event), daemon=True
    )
    guard.start()

    mode = status.get("mode") or "video"
    quality = status.get("quality") or "best"
    fmt = status.get("format") or ""

    cancelled = False
    # Devre kesici PLATFORM BAZLIDIR: karışık bir listede baştaki 3 Instagram
    # hatası arkadaki YouTube bağlantılarını öldürmesin. Saf YouTube listesinde
    # kalan uygun kalem kalmadığı için davranış bugünküyle birebir aynıdır.
    bot_streak = {}
    blocked = {}
    all_items = status["items"]
    # Bir önceki kalem GERÇEKTEN ağa çıktı mı? Kalemler arası bekleme buna
    # bakar: eskiden bekleme döngü gövdesinin sonundaydı ve hata/atlama
    # yollarının hepsi `continue` ile onu atlıyordu (20 başarısız Instagram
    # bağlantısı = beklemesiz 20 ardışık istek).
    net_hit = False

    try:
        for item in all_items:
            # Kullanıcı 'Sunucudan sil' demiş olabilir: klasör yoksa yt-dlp
            # outtmpl'i yeniden yaratıp hayalet iş üretmesin.
            if not os.path.isdir(files_dir):
                cancelled = True
                break
            if _cancel_requested(job_id, status):
                cancelled = True
                break

            # Disk güvenliği: her girdiden ÖNCE kontrol edilir.
            if web.free_bytes() < web.RESERVE_FREE:
                status["note"] = "Sunucuda disk alanı kalmadı, iş durduruldu."
                break
            if status.get("bytes", 0) >= web.MAX_JOB_BYTES:
                status["note"] = "Bu iş için ayrılan disk bütçesi doldu."
                break

            index, video_id = item["index"], item["id"]
            # Alan yoksa "youtube": oynatma listesi işlerinin ürettiği
            # status.json'da bu alan HİÇ bulunmaz ve davranış değişmez.
            platform = item.get("platform") or "youtube"
            duration = item.get("duration")

            # Bu platform devre dışı bırakıldıysa hiç denemeden atla.
            if platform in blocked:
                item["state"] = "skipped"
                item["err"] = blocked[platform]
                save()
                continue

            # Bilinen uzun içeriği hiç denemeden atla (10 saatlik yayın = 30+ GB)
            dur_cap = web.MAX_DURATION if mode == "video" else web.MAX_DURATION_AUDIO
            if duration and duration > dur_cap:
                item["state"] = "skipped"
                item["err"] = f"{dur_cap // 3600} saatten uzun, atlandı."
                save()
                continue

            # Nazik olalım: 429 riskini düşürmek için ağa çıkan kalemler arasında
            # bekle. Bekleme AĞA ÇIKILACAK kalemin platformuna göredir ve yalnız
            # önceki kalem ağa çıktıysa uygulanır — bu yüzden ağ isteği yapmayan
            # atlama dalları (engellenmiş platform, süre kapağı) gecikmez, ama
            # HATA yolları da artık seyreltmeden kaçamaz.
            if net_hit:
                time.sleep(random.uniform(5.0, 9.0) if platform == "instagram"
                           else random.uniform(1.0, 3.0))
                # Bekleme sırasında iptal gelmiş olabilir: boşuna istek atmayalım.
                if _cancel_requested(job_id, status):
                    cancelled = True
                    break
            net_hit = True

            item["state"] = "downloading"
            status["current"] = {
                "index": index,
                "title": item.get("title") or "",
                "pct": 0.0,
                "stage": "download",
            }
            save()

            item_started = time.monotonic()
            live["item_started"] = item_started
            filtered = {"reason": None}
            seen_height = {"h": None}
            # İndirme SÜRERKEN geçerli bütçe: iş kotasından kalan ile diskte
            # bırakılması gereken pay arasındaki küçük olan. Tek bir 50 GB'lik
            # dosya kök diski doldurmasın.
            room_left = _budget_left(status)

            def progress_hook(d, _item=item, _started=item_started):
                if _cancel_requested(job_id, status):
                    raise _Cancelled()
                if (time.monotonic() - _started) > web.ITEM_TIMEOUT:
                    raise _ItemTimeout()
                if d.get("status") == "downloading":
                    total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                    got = d.get("downloaded_bytes") or 0
                    # Bütçe/serbest alan denetimi indirme SIRASINDA da yapılır:
                    # yalnız girdi başında bakmak, tek bir dev dosyayı kaçırıyordu.
                    if got and (got > room_left or (got % (64 * 1024 * 1024)) < 262144):
                        if got > room_left or web.free_bytes() < web.RESERVE_FREE:
                            raise _TooBig()
                    pct = round(100.0 * got / total, 1) if total else 0.0
                    info = d.get("info_dict") or {}
                    if info.get("height"):
                        seen_height["h"] = info["height"]
                    # Yapıştırılan bağlantılarda başlık önizlemede BİLİNMİYOR
                    # (probe yapmadık); gerçek başlığı indirme sırasında yakala.
                    if _item.get("title_pending") and info.get("title"):
                        _item["title"] = str(info["title"])[:300]
                        _item["title_pending"] = False
                    status["current"] = {
                        "index": _item["index"],
                        "title": _item.get("title") or "",
                        "pct": pct,
                        "stage": "download",
                    }
                    save(force=False)
                elif d.get("status") == "finished":
                    status["current"] = {
                        "index": _item["index"],
                        "title": _item.get("title") or "",
                        "pct": 100.0,
                        "stage": "merge" if mode == "video" else "convert",
                    }
                    save()

            def pp_hook(d, _item=item):
                if _cancel_requested(job_id, status):
                    raise _Cancelled()
                stage = "convert" if mode == "audio" else "merge"
                status["current"] = {
                    "index": _item["index"],
                    "title": _item.get("title") or "",
                    "pct": 100.0,
                    "stage": stage,
                }
                save(force=False)

            def match_filter(info, incomplete=False):
                """Canlı yayın ve aşırı uzun içerik indirilmez."""
                if info.get("is_live") or info.get("live_status") in ("is_live", "is_upcoming"):
                    filtered["reason"] = "Canlı yayın, atlandı."
                    return "canli yayin"
                dur = info.get("duration")
                if dur and dur > dur_cap:
                    filtered["reason"] = f"{dur_cap // 3600} saatten uzun, atlandı."
                    return "cok uzun"
                return None

            prefix = f"{index:03d}-{video_id}"
            outtmpl = os.path.join(files_dir, prefix + ".%(ext)s")
            opts = web.build_opts(mode, quality, fmt, outtmpl)
            opts.update(
                {
                    "noprogress": True,
                    "ignoreerrors": False,
                    "socket_timeout": 20,
                    "retries": 3,
                    "fragment_retries": 5,
                    "progress_hooks": [progress_hook],
                    "postprocessor_hooks": [pp_hook],
                    "match_filter": match_filter,
                    # yt-dlp'nin kendi üst sınırı: bilinen boyutu bütçeyi aşan
                    # formatı hiç indirmeye başlamaz.
                    "max_filesize": room_left,
                }
            )
            # Alan yoksa bugünkü sabit YouTube adresi kurulur: oynatma listesi
            # işlerinde üretilen string KELİMESİ KELİMESİNE aynıdır.
            video_url = item.get("url") or f"https://www.youtube.com/watch?v={video_id}"

            error = None
            opts_try = opts
            for attempt in (0, 1):
                try:
                    with YoutubeDL(opts_try) as ydl:
                        ydl.download([video_url])
                    error = None
                    break
                except (_Cancelled, _ItemTimeout, _TooBig) as e:
                    error = e
                    break
                except Exception as e:  # noqa: BLE001 - girdi bazında izole
                    error = e
                    # YouTube web istemcisine 403 veriyorsa android ile tekrar
                    # dene (bkz. app.is_forbidden_error). Yarım parçaları at,
                    # yoksa _pick_file yanlış dosyayı seçebilir.
                    if attempt == 0 and platform == "youtube" and web.is_forbidden_error(e):
                        _sweep_leftovers(files_dir, prefix, None)
                        opts_try = web.with_android_client(opts)
                        continue
                    if attempt == 0 and _is_retryable(e):
                        time.sleep(2)
                        continue
                    break

            if isinstance(error, _Cancelled):
                item["state"] = "queued"
                _sweep_leftovers(files_dir, prefix, None)
                cancelled = True
                break

            if isinstance(error, _ItemTimeout):
                item["state"] = "failed"
                item["err"] = "Süre aşıldı (20 dk), atlandı."
                status["failed"] = status.get("failed", 0) + 1
                _sweep_leftovers(files_dir, prefix, None)
                status["current"] = None
                save()
                continue

            if isinstance(error, _TooBig):
                # Yarım dosya diskte kalmasın; kalan disk zaten dar.
                item["state"] = "skipped"
                item["err"] = "Kalan disk bütçesine sığmıyor, atlandı."
                _sweep_leftovers(files_dir, prefix, None)
                status["current"] = None
                save()
                if _budget_left(status) <= 0:
                    status["note"] = "Disk bütçesi doldu, kalan videolar indirilmedi."
                    break
                continue

            # Instagram fotoğraf gönderisi: alınacak akış yok. Yalnız Instagram'a
            # bakarız — YouTube tarafındaki son-işlem hataları gerçek arızadır ve
            # doğrulanmış müzik akışının çıktısı değişmemeli.
            if error is not None and platform == "instagram" and _is_media_missing(error):
                _sweep_leftovers(files_dir, prefix, None)
                if mode != "video":
                    item["state"] = "skipped"
                    item["err"] = "Bu gönderi fotoğraf, ses alınamaz."
                    status["current"] = None
                    save()
                    continue
                # Video modunda fotoğrafları medya ucundan al: tek fotoğraf
                # .jpg, carousel .zip. Başarılıysa kalem aşağıdaki normal
                # "hazır" yolundan (_pick_file + disk muhasebesi) geçer.
                try:
                    media = web.save_instagram_photos(video_url, files_dir, prefix)
                    error = None
                    if item.get("title_pending"):
                        item["title"] = (media["caption"] or media["owner"] or item["title"])[:300]
                        item["title_pending"] = False
                except Exception as e:  # noqa: BLE001 - girdi bazında izole
                    _sweep_leftovers(files_dir, prefix, None)
                    error = e

            if error is not None:
                if _is_botlike(error):
                    bot_streak[platform] = bot_streak.get(platform, 0) + 1
                else:
                    bot_streak[platform] = 0
                item["state"] = "failed"
                item["err"] = web._friendly_error(error, platform)
                status["failed"] = status.get("failed", 0) + 1
                _sweep_leftovers(files_dir, prefix, None)
                status["current"] = None
                save()
                if bot_streak.get(platform, 0) >= 3:
                    note = _bot_note(platform)
                    blocked[platform] = note
                    status["note"] = note
                    remaining = [
                        it
                        for it in all_items
                        if it.get("state") == "queued"
                        and (it.get("platform") or "youtube") not in blocked
                    ]
                    if not remaining:
                        # BUGÜNKÜ DAVRANIŞIN AYNISI: kalan uygun kalem yoksa iş
                        # burada biter (saf YouTube listesinde her zaman bu dal).
                        status["state"] = "failed"
                        save()
                        return 0
                    save()
                continue

            bot_streak[platform] = 0
            path, extras = _pick_file(files_dir, prefix)
            for extra in extras:
                try:
                    os.remove(extra)
                except OSError:
                    pass

            if not path:
                item["state"] = "skipped" if filtered["reason"] else "failed"
                item["err"] = filtered["reason"] or "Dosya oluşmadı."
                if item["state"] == "failed":
                    status["failed"] = status.get("failed", 0) + 1
                _sweep_leftovers(files_dir, prefix, None)
            else:
                size = os.path.getsize(path)
                item["state"] = "ready"
                item["file"] = os.path.basename(path)
                item["size"] = size
                item["height"] = seen_height["h"]
                item["err"] = None
                status["done"] = status.get("done", 0) + 1
                status["bytes"] = status.get("bytes", 0) + size

            status["current"] = None
            live["item_started"] = None
            save()

    except Exception:  # noqa: BLE001 - işin tamamı çökmesin
        # Not: istisna metni yt-dlp/ffmpeg'den, yani UZAKTAN gelen başlık ve
        # dosya adlarından beslenir. Arayüze ham metin taşımıyoruz.
        status["note"] = "Beklenmeyen bir hata oldu, iş durduruldu."
        status["state"] = "failed"
        status["current"] = None
        save()
        return 1
    finally:
        stop_event.set()

    status["current"] = None
    for item in status["items"]:
        if item["state"] in ("queued", "downloading"):
            item["state"] = "queued"

    if cancelled:
        status["state"] = "cancelled"
    elif status.get("done", 0) == 0:
        status["state"] = "failed"
    elif any(i["state"] != "ready" for i in status["items"]):
        status["state"] = "partial"
    else:
        status["state"] = "done"
    save()
    return 0


def main():
    # KİTAPLIK: AI kategorilendirme modu. JOB_RE doğrulamasından ÖNCE bakılır;
    # indirme yolunun tek satırı bile değişmez.
    if len(sys.argv) >= 3 and sys.argv[1] == "--ai":
        try:
            ai_run_id = int(sys.argv[2])
        except (TypeError, ValueError):
            return 2
        # KİLİT SIZINTISI KAPISI. run_ai()'nin kilidi bırakan finally'sine
        # ULAŞAMADAN patması mümkündür (paket yok, şema açılmıyor). O zaman
        # /data/.ailock ve 'running' tur 10 dakika takılı kalır, her deneme
        # 409 yer ve Popen(DEVNULL) yüzünden hiçbir teşhis izi kalmazdı.
        try:
            return run_ai(ai_run_id)
        except Exception:
            _ai_bail(
                ai_run_id,
                "Kategorilendirme süreci başlatılamadı (paket veya yapılandırma).",
            )
            return 1
        finally:
            try:
                web.lib_release_ailock(ai_run_id)
            except Exception:
                pass

    # KİTAPLIK: kapak indirme modu. Deseni --ai ile birebir aynıdır; indirme
    # yolunun tek satırı yine değişmez.
    if len(sys.argv) >= 3 and sys.argv[1] == "--cover":
        try:
            cover_run_id = int(sys.argv[2])
        except (TypeError, ValueError):
            return 2
        try:
            return run_cover(cover_run_id)
        except Exception:
            _cover_bail(cover_run_id, "Kapak indirme süreci başlatılamadı.")
            return 1
        finally:
            # KİLİDİ BIRAKAN TEK NOKTA burasıdır (run_cover kendi finally'sinde
            # BIRAKMAZ): iki ayrı yerden bırakmak, yeni alınmış ama henüz
            # damgalanmamış bir kilidi silme yarışı doğururdu.
            try:
                web.lib_release_covlock(cover_run_id)
            except Exception:
                pass

    if len(sys.argv) < 2 or not web.JOB_RE.match(sys.argv[1]):
        return 2
    job_id = sys.argv[1]
    try:
        signal.signal(signal.SIGTERM, _on_sigterm)
    except (ValueError, AttributeError):
        pass
    try:
        return run(job_id)
    finally:
        web.release_joblock(job_id)


# ===========================================================================
# KİTAPLIK — AI KATEGORİLENDİRME (ikinci mod: `plworker.py --ai <run_id>`)
# ---------------------------------------------------------------------------
# Neden burada, ayrı bir aiworker.py'de değil: Dockerfile'ın COPY listesi dar
# (app.py, plworker.py, templates). Yeni bir dosya eklenip COPY satırı
# unutulursa imajda dosya bulunmaz ve Popen stdout/stderr=DEVNULL olduğu için
# süreç SESSİZCE ölür — teşhis edilemez bir arıza. Buraya koyunca Dockerfile'a
# tek satır bile eklemek gerekmiyor.
#
# `import anthropic` KESİNLİKLE modül seviyesinde DEĞİL, run_ai()'nin İÇİNDE
# yapılır: aksi hâlde her indirme işçisinin soğuk başlangıcına bedelsiz yük
# biner ve paket sorunu indirmeyi de öldürür.
# ===========================================================================
import json

AI_MAX_TOKENS = 8000
AI_MAX_MINUTES = 30          # tur için duvar saati bütçesi
AI_MIN_TEXT = 12             # bundan kısa metin API'ye HİÇ gönderilmez
AI_SMALL_BATCH = 20          # max_tokens görülürse düşülen parti boyu


class _AiPause(Exception):
    """Hız limiti / bütçe: iş ÖLDÜRÜLMEZ, DURAKLATILIR."""


class _AiAuth(Exception):
    """401/403 — anahtar reddedildi. ai_enabled() yalnız 'boş mu' bakar,
    GEÇERLİLİĞE bakmaz. Bu ayrım olmadan geçersiz bir anahtar 951 kaydın
    hepsini 'failed' yapıp turu sessizce 'done' bitiriyordu."""


# Sistem promptu BYTE BAZINDA SABİT olmalı: tarih, sayı, parti numarası ASLA
# girmez. Girerse önbellek isabet etmez ve maliyet sessizce 10 katına çıkar.
_AI_SISTEM = (
    "Sen bir içerik sınıflandırıcısın. Sana numaralanmış gönderi satırları "
    "verilir. Her satır şu biçimdedir:\n"
    "<numara> | <platform>/<tur> | <metin>\n\n"
    "Her gönderiyi AŞAĞIDAKİ listeden TAM OLARAK BİR kategoriye ata.\n\n"
    + "\n".join(
        "- %s — %s" % (kod, web.KATEGORI_TANIMLARI.get(kod, ad))
        for kod, ad in web.KATEGORILER
    )
    + "\n\nKURALLAR:\n"
    "- Metinde gerçek bir konu ipucu yoksa kategori 'belirsiz', guven 'dusuk'.\n"
    "- Konu belli ama listedeki hiçbir kategoriye girmiyorsa 'diger'.\n"
    "- Kullanıcı adından, URL'den veya platformdan kategori UYDURMA.\n"
    "- Her girdi için ÇIKTIDA 'k' alanına o satırın numarasını AYNEN yaz.\n"
    "- Her girdiye tam bir sonuç döndür; hiçbirini atlama, sırayı değiştirebilirsin "
    "ama numarayı asla değiştirme.\n"
    "- guven: metin açıkça konuyu söylüyorsa 'yuksek', ima ediyorsa 'orta', "
    "zayıfsa 'dusuk'.\n"
    "- Metinler Türkçe, İngilizce ya da karışık olabilir; dil kategoriyi "
    "belirlemez, konu belirler.\n"
    "- Metinler çoğunlukla Instagram gönderilerinin kısa açıklamalarıdır ve "
    "bazıları otomatik üretilmiş, anlamsız olabilir. Zorlama yapma: emin "
    "olamadığında 'belirsiz' seç, yanlış kategori vermekten iyidir.\n"
    "- Bir gönderi birden çok başlığa uyuyorsa metnin ASIL konusunu seç; "
    "yan değinmelere göre karar verme.\n"
    "- Emoji, hashtag ve etiketler de sinyaldir, onları da değerlendir.\n\n"
    "ÖRNEKLER:\n"
    "1 | instagram/reel | 5 dakikada mercimek çorbası, tam kıvamında\n"
    "-> kategori 'yemek', guven 'yuksek'\n"
    "2 | youtube/video | Yeni RTX kartını test ettik — DonanımKanalı\n"
    "-> kategori 'teknoloji', guven 'yuksek'\n"
    "3 | instagram/p | text that says 'gununuz aydin'\n"
    "-> kategori 'belirsiz', guven 'dusuk'\n"
    "4 | instagram/reel | Kapadokya'da gün doğumu balon turu #gezi\n"
    "-> kategori 'seyahat', guven 'yuksek'\n"
    "5 | instagram/reel | Yeni ev dekorasyonu için raf montajı adım adım\n"
    "-> kategori 'egitim', guven 'orta'"
)

# ŞEMAYA maxItems / maxLength / minLength / sayısal kısıt KOYMA — structured
# outputs bunları desteklemiyor. Her nesnede additionalProperties:false ZORUNLU.
_AI_SEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["sonuclar"],
    "properties": {
        "sonuclar": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["k", "kategori", "guven"],
                "properties": {
                    "k": {"type": "integer"},
                    "kategori": {"type": "string", "enum": web.KATEGORI_KODLARI},
                    "guven": {"type": "string", "enum": ["yuksek", "orta", "dusuk"]},
                },
            },
        }
    },
}

# Instagram'ın otomatik ürettiği alt metin kalıpları: bunlar sinyal DEĞİL.
_AI_STRIP_HEAD = [
    re.compile(r"^\s*Photo by .*? on .*?\.\s*", re.I),
    re.compile(r"^\s*Video by .*? on .*?\.\s*", re.I),
    re.compile(r"^\s*Reel by .*? on .*?\.\s*", re.I),
    re.compile(r"^\s*Photo shared by .*? on .*?\.\s*", re.I),
]
_AI_GUVEN = ("yuksek", "orta", "dusuk")


def _ai_clean(text):
    """Otomatik alt metni soy; 'text that says ...' KORUNUR (gerçek sinyal)."""
    t = (text or "").strip()
    for rx in _AI_STRIP_HEAD:
        t = rx.sub("", t, count=1)
    low = t.lower()
    i = low.find("may be an image of")
    if i < 0:
        i = low.find("may be a video of")
    if i >= 0:
        j = low.find("text that says", i)
        t = (t[:i] + ((" " + t[j:]) if j >= 0 else "")).strip()
    return re.sub(r"\s+", " ", t).strip()


def _ai_text(row):
    """Modele verilecek metin. Elde YALNIZCA kısa açıklama/başlık var."""
    if row["platform"] == "youtube":
        parcalar = [row["title"] or "", row["uploader"] or ""]
        return re.sub(r"\s+", " ", " — ".join(p for p in parcalar if p)).strip()
    return _ai_clean(row["caption"] or row["title"] or "")


def _ai_retry_after(e, deneme):
    try:
        raw = e.response.headers.get("retry-after")
        if raw:
            return min(120.0, float(raw)) + random.random()
    except (AttributeError, TypeError, ValueError):
        pass
    return min(60, 2 ** deneme) + random.random()


def _ai_parse(resp):
    """JSON metni İLK text bloğundan alınır (output_config.format garanti eder)."""
    try:
        for blok in (resp.content or []):
            if getattr(blok, "type", None) == "text":
                veri = json.loads(blok.text)
                return veri.get("sonuclar") if isinstance(veri, dict) else None
    except (ValueError, TypeError, AttributeError):
        return None
    return None


def _ai_call(anthropic, client, toplu, butce):
    """Tek toplu sınıflandırma çağrısı.

    Dönüş: yanıt nesnesi, ya da kalıcı 4xx'te None. Hız limiti/bütçe biterse
    _AiPause fırlatır — tur öldürülmez, DURAKLATILIR ve kullanıcı düğmeye
    tekrar basınca kaldığı yerden devam eder (iş birimi kalemdir).
    """
    for deneme in range(6):
        if butce["calls"] >= butce["max_calls"] or time.time() > butce["deadline"]:
            raise _AiPause("butce")
        butce["calls"] += 1
        try:
            resp = client.messages.create(
                model=web.AI_MODEL,
                max_tokens=AI_MAX_TOKENS,
                output_config={
                    "effort": "low",
                    "format": {"type": "json_schema", "schema": _AI_SEMA},
                },
                system=[
                    {
                        "type": "text",
                        "text": _AI_SISTEM,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": toplu}],
            )
            try:
                butce["cache_read"] += int(
                    getattr(resp.usage, "cache_read_input_tokens", 0) or 0
                )
                # YAZIM da ölçülür: 'hiç okunmadı' ile 'zaten hiç yazılmadı'
                # (prompt ~1024 jeton eşiğinin altında) AYNI ŞEY DEĞİL.
                butce["cache_write"] += int(
                    getattr(resp.usage, "cache_creation_input_tokens", 0) or 0
                )
            except (AttributeError, TypeError, ValueError):
                pass
            return resp
        # EN ÖZELDEN GENELE: RateLimitError, APIStatusError'ın alt sınıfıdır.
        except anthropic.RateLimitError as e:
            bekle = _ai_retry_after(e, deneme)
        except anthropic.APIStatusError as e:
            try:
                kod = int(getattr(e, "status_code", 0) or 0)
            except (TypeError, ValueError):
                kod = 0
            if kod in (401, 403):
                # KALEMLERİ 'failed' YAPMA: sorun kalemde değil anahtarda.
                raise _AiAuth("anahtar")
            if kod < 500:
                return None  # 4xx: TEKRAR DENEME
            bekle = min(60, 2 ** deneme) + random.random()
        except anthropic.APIConnectionError:
            bekle = min(60, 2 ** deneme) + random.random()
        time.sleep(bekle)
    raise _AiPause("hiz-limiti")


def _ai_classify(anthropic, client, kalemler, butce, derinlik=0):
    """(durum, esleme) — durum: 'ok' | 'refused' | 'fail'.

    esleme: {item_pk: (kategori, guven)}. HİZALAMA DOĞRULAMASI sessiz
    bozulmanın tek panzehiridir: sonuç sayısı ya da numara kümesi tutmuyorsa
    parti İKİYE BÖLÜNÜR; yine tutmazsa HİÇBİR KATEGORİ YAZILMAZ.
    """
    if not kalemler:
        return "ok", {}

    satirlar = [
        "%d | %s/%s | %s" % (i + 1, k[1], k[2] or "", k[3][:300])
        for i, k in enumerate(kalemler)
    ]
    resp = _ai_call(anthropic, client, "\n".join(satirlar), butce)
    if resp is None:
        return "fail", {}

    stop = getattr(resp, "stop_reason", None)
    if stop == "refusal":
        # YANIT OKUNMADAN ÖNCE kontrol: içerik OKUNMAZ.
        return "refused", {}

    bol = stop == "max_tokens"
    if not bol:
        sonuclar = _ai_parse(resp)
        if not isinstance(sonuclar, list):
            bol = True
        else:
            numaralar, esleme = set(), {}
            for s in sonuclar:
                if not isinstance(s, dict):
                    continue
                try:
                    k = int(s.get("k"))
                except (TypeError, ValueError):
                    continue
                if not (1 <= k <= len(kalemler)):
                    continue
                numaralar.add(k)
                kat = s.get("kategori")
                if kat in web.KATEGORI_KODLARI:
                    guv = s.get("guven")
                    # KONUMA ASLA GÜVENME: kategori k-1 indisindeki kaleme yazılır.
                    esleme[kalemler[k - 1][0]] = (
                        kat,
                        guv if guv in _AI_GUVEN else None,
                    )
            if len(sonuclar) != len(kalemler) or numaralar != set(
                range(1, len(kalemler) + 1)
            ):
                bol = True
            else:
                return "ok", esleme

    if len(kalemler) == 1 or derinlik >= 3:
        return "fail", {}
    yari = len(kalemler) // 2
    d1, e1 = _ai_classify(anthropic, client, kalemler[:yari], butce, derinlik + 1)
    d2, e2 = _ai_classify(anthropic, client, kalemler[yari:], butce, derinlik + 1)
    if d1 == "refused" and d2 == "refused":
        return "refused", {}
    birlesik = {}
    birlesik.update(e1)
    birlesik.update(e2)
    return ("ok", birlesik) if birlesik else ("fail", {})


def _ai_bail(run_id, notu):
    """Turu 'failed' damgala. main()'in son çaresi: run_ai kendi finally'sine
    ulaşamadan patladığında arayüz 10 dakika sahte 'çalışıyor' göstermesin."""
    try:
        web.ensure_schema()
        con = web.lib_db()
        try:
            con.execute(
                "UPDATE ai_run SET state='failed', note=?, updated_at=? "
                "WHERE id=? AND state='running'",
                (notu, int(time.time()), run_id),
            )
            con.commit()
        finally:
            con.close()
    except Exception:
        pass


def run_ai(run_id):
    """Kopmuş AI işçisi. İndirme kilidine (.joblock) HİÇ DOKUNMAZ.

    KİLİDİ BURASI BIRAKMAZ. Tek bırakma noktası main()'in finally'sidir:
    iki ayrı yerden bırakmak, aradaki mikro-saniyede yeni alınmış ama henüz
    damgalanmamış bir kilidi silme yarışı doğururdu.
    """
    web.ensure_schema()

    def _tur_yaz(**alanlar):
        con = web.lib_db()
        try:
            sets = ", ".join("%s=?" % k for k in alanlar)
            con.execute(
                "UPDATE ai_run SET %s, updated_at=? WHERE id=?" % sets,
                list(alanlar.values()) + [int(time.time()), run_id],
            )
            con.commit()
        finally:
            con.close()
        # Kilidi de TAZELE: bayatlık artık kalp atışından okunuyor, ama
        # mtime'ı da güncel tutmak damgalanmamış kilit penceresini kapatır.
        try:
            os.utime(web.AI_LOCK, None)
        except OSError:
            pass

    # İŞÇİNİN İLK YAZIMI pid'dir: Popen DEVNULL'a yazdığı için "süreç hiç
    # başladı mı?" sorusunun TEK teşhis izi budur. Bu yüzden `import anthropic`
    # SATIRINDAN ÖNCE yazılır — paket eksikse bile iz kalsın.
    try:
        _tur_yaz(pid=os.getpid())
    except Exception:
        return 1

    import anthropic  # ← YALNIZCA burada; modül seviyesinde ASLA

    con = web.lib_db()
    try:
        satir = con.execute(
            "SELECT total, done, failed, scope FROM ai_run WHERE id=?", (run_id,)
        ).fetchone()
    finally:
        con.close()
    if not satir:
        return 1

    # KAPSAM. 'yeniden' dışındaki her turda seçim sorgusu BAYT BAYNA eskisidir.
    #
    # 'yeniden' turunda category NULL DEĞİLDİR ('diger'/'belirsiz' olarak
    # yerinde durur), o yüzden ayrı bir seçim gerekiyor. SONSUZ DÖNGÜ KAPISI:
    # başarı yazımı kategoriyle BİRLİKTE ai_state='done' yazıyor; buradaki
    # ai_state='pending' koşulu, model bir satırı YİNE 'diger' derse bile onu
    # kümeden çıkarır. Bu kapı olmasa gerçek 'diger' kayıtları ai_tries 3'e
    # dayanana kadar seçilir ve ÜÇ KEZ faturalanırdı.
    scope = satir["scope"] if "scope" in satir.keys() else "kategorisiz"
    if scope == "yeniden":
        _AI_SECIM = (
            "SELECT id, platform, kind, title, caption, uploader FROM item "
            "WHERE ai_state='pending' AND ai_tries < 3 "
            "AND category IN ('diger','belirsiz') AND category_src <> 'kullanici' "
            "ORDER BY id LIMIT ?"
        )
    else:
        _AI_SECIM = (
            "SELECT id, platform, kind, title, caption, uploader FROM item "
            "WHERE ai_state='pending' AND category IS NULL AND ai_tries < 3 "
            "ORDER BY id LIMIT ?"
        )

    toplam = satir["total"] or 0
    butce = {
        "calls": 0,
        # ceil(toplam/parti) × 1.5 — bisect ve tekrar denemeler için pay.
        "max_calls": max(4, int((toplam + web.AI_BATCH - 1) / web.AI_BATCH * 1.5) + 2),
        "deadline": time.time() + AI_MAX_MINUTES * 60,
        "cache_read": 0,
        "cache_write": 0,
    }

    islenen = 0
    biten = 0
    hatali = 0
    partiler = 0
    parti_boy = web.AI_BATCH
    durum = "done"
    notu = None
    simdi = lambda: int(time.time())

    try:
        # İstemci kurulumu TRY İÇİNDE: biçimsiz anahtarda kurucu fırlarsa tur
        # 'failed' damgalanır, dışarı sızıp kilidi askıda bırakmaz.
        client = anthropic.Anthropic(max_retries=0)  # SDK'nın kendi tekrarı
        # KAPALI: açık kalırsa kendi döngümüzle çarpılıp deneme sayısı 18'e çıkar.

        while islenen < toplam:
            if os.path.exists(web.AI_CANCEL):
                durum = "cancelled"
                notu = "Kullanıcı durdurdu."
                break

            kalan = toplam - islenen
            con = web.lib_db()
            try:
                rows = con.execute(_AI_SECIM, (min(parti_boy, kalan),)).fetchall()
            finally:
                con.close()
            if not rows:
                durum = "done"
                break

            # KURAL TABANLI ÖN-FİLTRE: anlamlı metni olmayan kalem API'ye
            # HİÇ gönderilmez. 951 kaydın büyük kısmını ücretsiz kapatır ve
            # modeli kullanıcı adından kategori uydurmaya itmez.
            kural, apiye = [], []
            for r in rows:
                metin = _ai_text(r)
                if len(metin) < AI_MIN_TEXT:
                    kural.append(r["id"])
                else:
                    apiye.append((r["id"], r["platform"], r["kind"], metin))

            if kural:
                con = web.lib_db()
                try:
                    con.execute("BEGIN")
                    # KAPSAM DUYARLI. 'yeniden' turunda category DOLU gelir
                    # ('diger' ya da 'belirsiz'); üzerine koşulsuz 'belirsiz'
                    # yazmak YIKICIDIR — düğmenin sözü "mevcut kategori yerinde
                    # durur". 'diger' (= konu belli, kutu yok) bilgisi
                    # 'belirsiz'e (= ipucu yok) düşürülemez.
                    con.executemany(
                        ("UPDATE item SET ai_state='done', ai_at=? WHERE id=?"
                         if scope == "yeniden" else
                         "UPDATE item SET category='belirsiz', category_src='kural', "
                         "ai_state='done', ai_at=? WHERE id=?"),
                        [(simdi(), i) for i in kural],
                    )
                    con.commit()
                finally:
                    con.close()
                islenen += len(kural)
                biten += len(kural)
                _tur_yaz(done=biten, failed=hatali, batches=partiler)

            if not apiye:
                continue

            # ai_tries PARTİ BAŞINDA artar: parti çökerse aynı kalemler
            # sonsuza dek yeniden seçilmesin (ai_tries < 3 kapısı).
            con = web.lib_db()
            try:
                con.execute("BEGIN")
                con.executemany(
                    "UPDATE item SET ai_tries = ai_tries + 1 WHERE id=?",
                    [(k[0],) for k in apiye],
                )
                con.commit()
            finally:
                con.close()

            sonuc, esleme = _ai_classify(anthropic, client, apiye, butce)
            partiler += 1
            simdi_ts = simdi()

            con = web.lib_db()
            try:
                con.execute("BEGIN")
                if sonuc == "refused":
                    # KAPSAM DUYARLI (yukarıdaki 'kural' dalıyla aynı gerekçe):
                    # tek reddedilmiş parti 40 kaydın kategorisini 'diger'den
                    # 'belirsiz'e düşürüp category_src'yi 'ai'den 'kural'a
                    # çeviriyordu ve bu bilgi geri gelmiyordu.
                    con.executemany(
                        ("UPDATE item SET ai_state='refused', ai_at=? WHERE id=?"
                         if scope == "yeniden" else
                         "UPDATE item SET category='belirsiz', category_src='kural', "
                         "ai_state='refused', ai_at=? WHERE id=?"),
                        [(simdi_ts, k[0]) for k in apiye],
                    )
                    biten += len(apiye)
                elif sonuc == "ok":
                    con.executemany(
                        "UPDATE item SET category=?, category_src='ai', ai_conf=?, "
                        "ai_state='done', ai_at=? WHERE id=?",
                        [
                            (esleme[k[0]][0], esleme[k[0]][1], simdi_ts, k[0])
                            for k in apiye
                            if k[0] in esleme
                        ],
                    )
                    eksik = [k[0] for k in apiye if k[0] not in esleme]
                    if eksik:
                        con.executemany(
                            "UPDATE item SET ai_state='failed', ai_at=? WHERE id=?",
                            [(simdi_ts, i) for i in eksik],
                        )
                    biten += len(apiye) - len(eksik)
                    hatali += len(eksik)
                else:
                    # HİÇBİR KATEGORİ YAZILMAZ.
                    con.executemany(
                        "UPDATE item SET ai_state='failed', ai_at=? WHERE id=?",
                        [(simdi_ts, k[0]) for k in apiye],
                    )
                    hatali += len(apiye)
                    if parti_boy > AI_SMALL_BATCH:
                        parti_boy = AI_SMALL_BATCH
                con.commit()
            finally:
                con.close()

            islenen += len(apiye)
            # updated_at her partide yazılır = KALP ATIŞI (600 sn'den eskisi
            # arayüzde "durmuş" görünür).
            _tur_yaz(done=biten, failed=hatali, batches=partiler)

    except _AiAuth:
        durum = "failed"
        notu = ("API anahtarı reddedildi (ANTHROPIC_API_KEY). Anahtarı düzeltip "
                + ("“Diğer/Belirsiz'i yeniden sınıflandır”a"
                   if scope == "yeniden" else "“Tekrar dene”ye")
                + " basın.")
    except _AiPause:
        durum = "paused"
        # DÜĞME ADI KAPSAMA GÖRE DEĞİŞİR. Sabit "Kategorilendir" metni 'yeniden'
        # turunda YANLIŞ yönlendiriyordu: o düğme category IS NULL satırları
        # arar, 'yeniden' kapsamındaki kayıtların kategorisi NULL değildir →
        # "Kategorilendirilecek kayıt yok" denir ve kullanıcı yarım kalan işi
        # bitmiş sanırdı.
        _dugme = {
            "yeniden": "“Diğer/Belirsiz'i yeniden sınıflandır”",
            "basarisiz": "“Tekrar dene”",
        }.get(scope, "“Kategorilendir”")
        notu = ("API sınırı veya bütçe doldu, %s düğmesine tekrar basıp kaldığı "
                "yerden devam edin." % _dugme)
    except Exception:
        durum = "failed"
        # HAM İSTİSNA METNİ ARAYÜZE TAŞINMAZ (anahtar sızıntısı riski).
        notu = "Kategorilendirme beklenmedik bir hatayla durdu."
    finally:
        try:
            if (butce["calls"] > 1 and butce["cache_read"] == 0
                    and butce["cache_write"] > 0 and durum == "done"):
                # Sessiz 10x maliyet uyarısı — ama YALNIZCA önbelleğe GERÇEKTEN
                # YAZILDIĞI hâlde hiç okunmadıysa. Sistem promptu 2409 karakter
                # (ölçüldü) ve önbelleğe alınabilir asgari önek ~1024 jetondur;
                # prompt eşiğin ALTINDA kalırsa API onu sessizce önbelleğe
                # almaz, cache_read her turda 0 olur ve uyarı HER BAŞARILI
                # TURDA yanlış yere yanardı. cache_write == 0 → sorun promptun
                # kısalığıdır, bozulması değil: kullanıcıyı olmayan bir hatanın
                # peşine düşürmeyiz.
                notu = "Uyarı: istem önbelleği hiç okunmadı."
            _tur_yaz(state=durum, done=biten, failed=hatali, batches=partiler, note=notu)
        except Exception:
            pass
        # Kilit BURADA bırakılmaz — main()'in finally'si bırakır.
        if durum == "cancelled":
            try:
                os.unlink(web.AI_CANCEL)
            except OSError:
                pass
    return 0


# ===========================================================================
# KİTAPLIK — KAPAK ÖNBELLEĞİ (üçüncü mod: `plworker.py --cover <run_id>`)
# ---------------------------------------------------------------------------
# Neden burada, ayrı bir coverworker.py'de değil: --ai ile aynı gerekçe —
# Dockerfile'ın COPY listesi dar (app.py, plworker.py, templates). Yeni dosya
# eklenip COPY satırı unutulursa Popen(DEVNULL) yüzünden süreç SESSİZCE ölür.
#
# `import urllib.request` KESİNLİKLE modül seviyesinde DEĞİL, run_cover()'ın
# İÇİNDE yapılır: her indirme işçisinin soğuk başlangıcına yük binmesin.
#
# MALİYET 0 USD. Instagram'ın API'sine İSTEK ATILMAZ: yalnız veritabanında
# ZATEN duran cover_url'e (statik CDN dosyası) gidilir, çerez ve referer
# GÖNDERİLMEZ (ölçümde HTTP 200 dönen yapılandırma tam olarak budur).
# ===========================================================================


class _CoverPause(Exception):
    """Hız limiti / disk / tavan: tur ÖLDÜRÜLMEZ, DURAKLATILIR. İş birimi
    kalemdir, kullanıcı düğmeye tekrar basınca kaldığı yerden devam eder."""


class _CoverCancel(Exception):
    """Kullanıcı durdurdu (/data/.covercancel)."""


def _cover_bail(run_id, notu):
    """Turu 'failed' damgala — main()'in son çaresi (_ai_bail'in kopyası)."""
    try:
        web.ensure_schema()
        con = web.lib_db()
        try:
            con.execute(
                "UPDATE cover_run SET state='failed', note=?, updated_at=? "
                "WHERE id=? AND state='running'",
                (notu, int(time.time()), run_id),
            )
            con.commit()
        finally:
            con.close()
    except Exception:
        pass


def _cover_opener(urlreq, urlerr):
    """Yönlendirme hedefini de beyaz listeden geçiren opener.

    Beyaz liste yalnız İLK adrese uygulansaydı, CDN'den gelen bir 302 sunucuyu
    listenin dışına taşıyabilirdi.
    """

    class _Yonlendirme(urlreq.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            if not web.cover_host_ok(newurl):
                raise urlerr.HTTPError(newurl, code, "yönlendirme reddedildi", headers, fp)
            return urlreq.HTTPRedirectHandler.redirect_request(
                self, req, fp, code, msg, headers, newurl
            )

    return urlreq.build_opener(_Yonlendirme)


def _cover_magic_ok(ext, ilk):
    """İlk baytların sihirli imzası uzantıyla tutuyor mu?

    Bilinmeyen/kısa girdide TRUE döner: bu bir güvenlik kapısı değil, CDN'in
    'image/jpeg' başlığıyla dönebildiği hata sayfalarını eleyen bir sağlamlık
    kontrolüdür — şüpheye düşünce dosyayı kabul etmek, doğru kapağı yanlışlıkla
    atmaktan iyidir.
    """
    if not ilk or len(ilk) < 12:
        return True
    if ext == "jpg":
        return ilk[:2] == b"\xff\xd8"
    if ext == "png":
        return ilk[:8] == b"\x89PNG\r\n\x1a\n"
    if ext == "webp":
        return ilk[:4] == b"RIFF" and ilk[8:12] == b"WEBP"
    return True


def _cover_fetch(urlreq, urlerr, opener, url):
    """Tek kapağı indirir.

    Dönüş: (durum, gorel_yol, boyut). durum:
      'ok'     — dosya yerinde
      'gone'   — bir daha DENENMEZ (beyaz liste dışı, 404/410, tür/tavan)
      'failed' — sonraki turda tekrar denenir (403/5xx/zaman aşımı)
      'limit'  — 429; SATIRA DOKUNULMAZ, tur seviyesinde geri çekilinir
    """
    if not web.cover_host_ok(url):
        return "gone", None, 0

    key = web.cover_key(url)
    part = os.path.join(web.COVER_TMP, key + ".part")
    try:
        istek = urlreq.Request(
            url,
            headers={
                "User-Agent": web.COVER_UA,
                "Accept": web.COVER_ACCEPT,
                # REFERER YOK, ÇEREZ YOK: istek kimliksiz olmalı.
                "Accept-Language": "en-US,en;q=0.8",
                "Connection": "close",
            },
        )
        with opener.open(istek, timeout=web.COVER_TIMEOUT) as r:
            ctype = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            ext = web.COVER_EXT.get(ctype)
            if not ext:
                return "gone", None, 0   # HEIC/AVIF/HTML: kapak değil
            try:
                clen = int(r.headers.get("Content-Length") or 0)
            except (TypeError, ValueError):
                clen = 0
            if clen > web.COVER_MAX_BYTES:
                return "gone", None, 0

            os.makedirs(web.COVER_TMP, exist_ok=True)
            boyut = 0
            asildi = False
            ilk = b""
            # YAZIM SIRASI KESİN: önce tam dosya, SONRA DB. Ters sırada yarım
            # bir JPEG kalıcı olarak 'ok' damgalanırdı.
            with open(part, "wb") as f:
                while True:
                    parca = r.read(65536)
                    if not parca:
                        break
                    if not ilk:
                        ilk = parca[:16]
                    boyut += len(parca)
                    # Content-Length yalan söyleyebilir: tavan AKIŞ SIRASINDA
                    # da kontrol edilir.
                    if boyut > web.COVER_MAX_BYTES:
                        asildi = True
                        break
                    f.write(parca)
            if asildi or not boyut:
                return "gone", None, 0

            # TAMLIK DOĞRULAMASI. http.client, chunked OLMAYAN bir yanıt erken
            # kesilirse IncompleteRead FIRLATMAZ: read() sessizce b'' döner.
            # Bu kontrol olmadan yarım bir JPEG 'ok' damgalanır, içerik adresli
            # ad değişmediği için bir daha asla yeniden indirilmez ve tarayıcı
            # onu 'immutable' başlığıyla BİR YIL önbellekte tutardı.
            # 'failed' seçildi ('gone' değil): sonraki turda tekrar denensin.
            if clen and boyut != clen:
                return "failed", None, 0

            # CDN hata sayfasını image/jpeg başlığıyla dönebiliyor: ilk baytların
            # sihirli imzası Content-Type ile tutmuyorsa dosya kapak değildir.
            if not _cover_magic_ok(ext, ilk):
                return "gone", None, 0

        gorel = web.cover_rel(key, ext)
        hedef = os.path.join(web.COVER_DIR, gorel)
        os.makedirs(os.path.dirname(hedef), exist_ok=True)
        os.replace(part, hedef)          # ATOMİK (tmp aynı dosya sisteminde)
        return "ok", gorel, boyut

    except urlerr.HTTPError as e:
        try:
            kod = int(getattr(e, "code", 0) or 0)
        except (TypeError, ValueError):
            kod = 0
        if kod in (404, 410):
            return "gone", None, 0
        if kod == 429:
            return "limit", None, 0
        return "failed", None, 0
    except Exception:
        # Ağ/zaman aşımı/çözümleme: tekrar denemeye değer.
        return "failed", None, 0
    finally:
        try:
            if os.path.exists(part):
                os.unlink(part)
        except OSError:
            pass


def _cover_prune():
    """ÖKSÜZ BUDAMA — yalnız turun BAŞINDA koşar.

    janitor() DEĞİŞTİRİLMEZ ve kapaklara TTL süpürücüsü YAZILMAZ: kapak, kaydı
    silinmediği sürece kalıcıdır. Silinen kayıtların dosyaları burada toplanır.
    """
    try:
        con = web.lib_db()
        try:
            kullanilan = {
                r[0] for r in con.execute(
                    "SELECT cover_file FROM item WHERE cover_file IS NOT NULL"
                )
            }
        finally:
            con.close()
    except Exception:
        return 0

    silinen = 0
    simdi = time.time()
    try:
        girdiler = os.listdir(web.COVER_DIR)
    except OSError:
        return 0
    for ad in girdiler:
        yol = os.path.join(web.COVER_DIR, ad)
        if ad == "tmp":
            try:
                for p in os.listdir(yol):
                    tam = os.path.join(yol, p)
                    try:
                        if simdi - os.path.getmtime(tam) > web.COVER_TMP_TTL:
                            os.unlink(tam)
                    except OSError:
                        pass
            except OSError:
                pass
            continue
        if len(ad) != 2 or not os.path.isdir(yol):
            continue
        try:
            dosyalar = os.listdir(yol)
        except OSError:
            continue
        for p in dosyalar:
            if ad + "/" + p in kullanilan:
                continue
            try:
                os.unlink(os.path.join(yol, p))
                silinen += 1
            except OSError:
                pass
    return silinen


def run_cover(run_id):
    """Kopmuş kapak işçisi. İndirme kilidine (.joblock) ve .ailock'a HİÇ
    DOKUNMAZ — kapak turu ile AI turu aynı anda çalışabilir.

    KİLİDİ BURASI BIRAKMAZ; tek bırakma noktası main()'in finally'sidir.
    """
    import shutil

    web.ensure_schema()

    def _tur_yaz(**alanlar):
        con = web.lib_db()
        try:
            sets = ", ".join("%s=?" % k for k in alanlar)
            con.execute(
                "UPDATE cover_run SET %s, updated_at=? WHERE id=?" % sets,
                list(alanlar.values()) + [int(time.time()), run_id],
            )
            con.commit()
        finally:
            con.close()
        try:
            os.utime(web.COVER_LOCK, None)   # kilidin mtime'ı da taze kalsın
        except OSError:
            pass

    # İLK YAZIM pid: Popen DEVNULL'a yazdığı için "süreç hiç başladı mı?"
    # sorusunun TEK teşhis izi budur — import satırından ÖNCE yazılır.
    try:
        _tur_yaz(pid=os.getpid())
    except Exception:
        return 1

    import urllib.request as urlreq   # ← YALNIZCA burada
    import urllib.error as urlerr

    con = web.lib_db()
    try:
        satir = con.execute(
            "SELECT total FROM cover_run WHERE id=?", (run_id,)
        ).fetchone()
        onceki = con.execute(
            "SELECT COALESCE(SUM(cover_bytes),0) b FROM item"
        ).fetchone()["b"] or 0
    finally:
        con.close()
    if not satir:
        return 1
    toplam = satir["total"] or 0

    try:
        os.makedirs(web.COVER_TMP, exist_ok=True)   # TEMBEL: modül seviyesinde ASLA
    except OSError:
        _cover_bail(run_id, "Kapak klasörü oluşturulamadı.")
        return 1

    _cover_prune()
    opener = _cover_opener(urlreq, urlerr)

    islenen = biten = hatali = olu = 0
    bayt = 0
    ardisik = 0
    son_id = 0
    durum = "done"
    notu = None

    # KALP ATIŞI KALEM SEVİYESİNDE. Grup sonuna bağlı kalsaydı: tek kalem 429
    # geri çekilmesiyle 125 sn'ye kadar sürebiliyor, beş böyle kalem 600 sn'lik
    # bayatlık eşiğini aşıyor ve arayüz ÇALIŞAN işçiyi "yanıt vermiyor" ilan
    # edip kilidini çaldırıyordu — sonuç İKİ eşzamanlı işçi ve CDN'e iki kat
    # istek. 60 sn'den seyrek yazmıyoruz: 25 kalemde tek yazımdan ~2 kat fazla,
    # SQLite için önemsiz.
    son_atim = [time.time()]

    def _kalp():
        if time.time() - son_atim[0] < 60:
            return
        try:
            _tur_yaz(done=biten, failed=hatali, gone=olu, bytes=onceki + bayt)
        except Exception:
            pass
        son_atim[0] = time.time()

    try:
        while islenen < toplam:
            if os.path.exists(web.COVER_CANCEL):
                raise _CoverCancel()

            con = web.lib_db()
            try:
                rows = con.execute(
                    "SELECT id, cover_url, cover_file FROM item "
                    "WHERE platform='instagram' AND cover_url IS NOT NULL "
                    "AND cover_state NOT IN ('ok','gone') AND cover_tries < 3 "
                    "AND id > ? ORDER BY id LIMIT ?",
                    # id imleci İLERİ GİDİŞİ GARANTİ EDER: 'failed' kalan satır
                    # (tries hâlâ < 3) aynı turda yeniden seçilip sonsuz döngü
                    # kurmasın. Ona bir sonraki turda dönülür.
                    (son_id, min(web.COVER_BATCH, toplam - islenen)),
                ).fetchall()
            finally:
                con.close()
            if not rows:
                break

            yazim, eski_dosyalar = [], []
            # GRUP YAZIMI HER YOLDA İŞLENİR. _cover_commit döngüden SONRA
            # çağrılsaydı, döngü İÇİNDEN fırlayan altı çıkış (iptal, disk,
            # tavan, hiz, ardisik, ilk20) 25 kayda kadar UPDATE'i çöpe atardı:
            # inen dosyalar öksüz kalıp bir sonraki turun _cover_prune()'unda
            # SİLİNİR ve cover_tries hiç artmadığı için 'ardisik' kapısı
            # KALICI KİLİTLENMEYE dönüşürdü (aynı 5 satır sonsuza dek).
            try:
                for r in rows:
                    son_id = r["id"]
                    if os.path.exists(web.COVER_CANCEL):
                        raise _CoverCancel()
                    # İndirme motoruyla AYNI rezerv sabiti: kapaklar indirmeyi
                    # asla aç bırakamaz.
                    try:
                        if shutil.disk_usage(web.DATA_DIR).free < web.RESERVE_FREE:
                            raise _CoverPause("disk")
                    except OSError:
                        pass
                    if onceki + bayt > web.COVER_RUN_MAX_BYTES:
                        raise _CoverPause("tavan")

                    bekleme = (5, 15, 45)
                    for deneme in range(4):
                        sonuc, gorel, boyut = _cover_fetch(
                            urlreq, urlerr, opener, r["cover_url"]
                        )
                        if sonuc != "limit":
                            break
                        if deneme < 3:
                            time.sleep(bekleme[deneme])
                            _kalp()   # 65 sn'lik geri çekilme kalbi durdurmasın
                    else:
                        raise _CoverPause("hiz")

                    if sonuc == "ok":
                        yazim.append(
                            ("UPDATE item SET cover_file=?, cover_state='ok', "
                             "cover_tries=0, cover_bytes=?, cover_dl_at=? WHERE id=?",
                             (gorel, boyut, int(time.time()), r["id"]))
                        )
                        if r["cover_file"] and r["cover_file"] != gorel:
                            eski_dosyalar.append(r["cover_file"])
                        biten += 1
                        bayt += boyut
                        ardisik = 0
                    elif sonuc == "gone":
                        yazim.append(
                            ("UPDATE item SET cover_state='gone', cover_dl_at=? WHERE id=?",
                             (int(time.time()), r["id"]))
                        )
                        olu += 1
                        ardisik = 0   # 404 sayaca girmez: içerik gerçekten yok
                    else:
                        yazim.append(
                            ("UPDATE item SET cover_state='failed', "
                             "cover_tries=cover_tries+1, cover_dl_at=? WHERE id=?",
                             (int(time.time()), r["id"]))
                        )
                        hatali += 1
                        ardisik += 1

                    islenen += 1
                    _kalp()

                    # OTOMATİK GÜVENLİK KAPILARI — kullanıcıya ek soru sormadan.
                    if ardisik >= 5:
                        raise _CoverPause("ardisik")
                    if islenen == 20 and (hatali + olu) >= 15:
                        raise _CoverPause("ilk20")

                    # ~2 istek/sn: normal bir Instagram sayfasını kaydırmaktan
                    # DAHA YAVAŞ. Tek bağlantı, seri.
                    time.sleep(web.COVER_DELAY + random.random() * 0.2)
            finally:
                # İstisna buradan SONRA dışarı yayılmaya devam eder; 'paused' /
                # 'cancelled' damgalama mantığı bozulmaz.
                _cover_commit(yazim, eski_dosyalar)
                yazim, eski_dosyalar = [], []
                try:
                    _tur_yaz(done=biten, failed=hatali, gone=olu, bytes=onceki + bayt)
                    son_atim[0] = time.time()
                except Exception:
                    pass

    except _CoverCancel:
        durum = "cancelled"
        notu = "Kullanıcı durdurdu."
    except _CoverPause as e:
        durum = "paused"
        notu = {
            "disk": "Disk bütçesi doldu — yer açıp tekrar başlatın.",
            "tavan": "Kapak önbelleği 512 MB tavanına ulaştı.",
            "hiz": "CDN hız sınırı — biraz sonra tekrar başlatın, kaldığı yerden devam eder.",
            "ardisik": "Üst üste 5 kapak alınamadı — ağ ya da CDN sorunu olabilir.",
            "ilk20": "İlk 20 kapağın çoğu alınamadı — CDN engelliyor olabilir. "
                     "Adresler tazelenmek üzere yeniden içe aktarım gerekebilir.",
        }.get(str(e), "Kapak indirme duraklatıldı.")
    except Exception:
        durum = "failed"
        # HAM İSTİSNA METNİ ARAYÜZE TAŞINMAZ.
        notu = "Kapak indirme beklenmedik bir hatayla durdu."
    finally:
        try:
            _tur_yaz(
                state=durum, done=biten, failed=hatali, gone=olu,
                bytes=onceki + bayt, note=notu,
            )
        except Exception:
            pass
        if durum == "cancelled":
            try:
                os.unlink(web.COVER_CANCEL)
            except OSError:
                pass
    return 0


def _cover_commit(yazim, eski_dosyalar):
    """Grup yazımı TEK transaction'da. SQLite yazar kilidi baskısı düşük kalsın
    diye 25'lik gruplar hâlinde: AI turu aynı anda item tablosuna yazıyor
    olabilir (WAL + busy_timeout=5000 bunu karşılar)."""
    if yazim:
        con = web.lib_db()
        try:
            con.execute("BEGIN")
            for sql, params in yazim:
                con.execute(sql, params)
            con.commit()
        finally:
            con.close()
    # Eski dosya ancak YENİSİ DB'ye yazıldıktan SONRA silinir.
    for gorel in eski_dosyalar:
        try:
            os.unlink(os.path.join(web.COVER_DIR, gorel))
        except OSError:
            pass


if __name__ == "__main__":
    sys.exit(main())
