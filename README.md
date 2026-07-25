# igdownloader

A tiny self-hosted web app to download YouTube and Instagram videos — paste a link, pick a quality, get an MP4. Or pull just the audio as MP3/M4A. Flask + [yt-dlp](https://github.com/yt-dlp/yt-dlp).

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.x-000000?logo=flask&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

## Why

I kept wanting to save the occasional YouTube clip or Instagram reel to my own devices without installing a sketchy app or pasting links into some random ad-riddled website. So I built a small one — a single page where you paste a link, see a thumbnail preview, choose the quality (or grab just the audio), and download. It runs on my own server, keeps nothing, and uses my own login only when a post needs it.

## How it works

A small Flask app wraps `yt-dlp`. Paste a YouTube or Instagram URL and the page calls `/api/info` to fetch the title, uploader, thumbnail and the resolutions the source actually offers, then `/api/download` streams the file to your browser. You can pick a **video** resolution (up to what the source provides) or switch to **audio** and export MP3/M4A at 128–320 kbps. Downloads are written to a temp folder and **deleted immediately after the file is sent** — nothing is stored. Video prefers a merged `mp4`, and only the first item of a carousel/playlist is downloaded.

## Run with Docker (recommended)

```bash
git clone https://github.com/0iroN/igdownloader.git
cd igdownloader
docker compose up -d --build
```

Then open **http://localhost:8600** (host port `8600` maps to the container's `8000`).
On your LAN, other devices can reach it at `http://<server-ip>:8600`.

```bash
docker compose logs -f      # follow logs
docker compose down         # stop
```

## Run locally (without Docker)

```bash
pip install -r requirements.txt
python app.py               # development server on http://localhost:5000
```

The bundled `Dockerfile` runs the app under gunicorn for production; `python app.py` is the simple dev server. ffmpeg is bundled in the Docker image and is needed locally for audio extraction and for merging higher-resolution video/audio streams.

## Usage

1. Open the page in your browser.
2. Paste a YouTube or Instagram link (e.g. `https://youtube.com/...` or `https://www.instagram.com/reel/...`).
3. Click **Getir** (Fetch) to preview the thumbnail, title and uploader.
4. Choose the **Video** tab and a resolution, or the **Müzik** (Audio) tab with a format (MP3/M4A) and bitrate.
5. Click **İndir** (Download) to save the file.

## Features

- YouTube and Instagram, from a single input.
- Video download with resolution selection, capped at what the source offers.
- Audio-only export as MP3 or M4A at 128 / 192 / 256 / 320 kbps.
- Thumbnail, title, uploader and duration preview before you download.
- Nothing is stored server-side: temp files are deleted right after sending.

## Downloading login-walled posts (your own cookies)

Some posts are public in a browser but the site still asks `yt-dlp` to be logged in. To handle those, supply **your own** cookies:

1. Export your cookies in **Netscape format** — e.g. with the "Get cookies.txt LOCALLY" browser extension while logged into the site.
2. Save the file as `data/cookies.txt` (see `data/cookies.txt.example` for the location and format).
3. Restart so it's picked up:

   ```bash
   docker compose restart
   ```

The `data/` folder is mounted into the container at `/data`, and the app automatically looks for cookies at `$COOKIES_FILE`, then `/data/cookies.txt`, then a `cookies.txt` next to `app.py`. Running locally without Docker, just place `cookies.txt` next to `app.py`.

> Your `cookies.txt` grants access to your account — treat it like a password. It is excluded by `.gitignore`; never share or commit it.

## Notes

- ffmpeg is included in the Docker image; it's used for audio extraction and for merging higher-resolution streams.
- Private accounts/posts cannot be downloaded.
- Carousels/playlists return the first video only.
- Downloaded files are temporary and removed right after they are sent.

## Disclaimer

This tool is intended for **personal, authorized downloading only** — for example, saving your own content or content you have permission to download. You are solely responsible for complying with each platform's terms of service, including [Instagram's Terms of Service](https://help.instagram.com/581066165581870) and [YouTube's Terms of Service](https://www.youtube.com/t/terms), and with applicable copyright law. If you use cookies, you do so with your own account and **at your own risk**. The author provides this software as-is and accepts no liability for misuse. Do not use it to download or redistribute content you do not have the right to.

## License

[MIT](LICENSE) © Osman Demir · [osmandemir.net](https://osmandemir.net)
