# Instagram Video İndirici

Instagram reel / gönderi / video bağlantısını yapıştır, videoyu indir. `yt-dlp` + Flask.

## Çalıştırma

```bash
pip install -r requirements.txt
python app.py
```

Sonra tarayıcıda: **http://localhost:5000**

Aynı ağdaki telefon/başka bilgisayardan erişmek için: `http://<bilgisayarin-ip-adresi>:5000`

## Giriş gerektiren videolar (cookies.txt)

Bazı videolar (gizli olmayan ama Instagram'ın giriş istediği) için tarayıcı
çerezleri gerekir. Bir tarayıcı eklentisiyle (ör. "Get cookies.txt LOCALLY")
Instagram çerezlerini **Netscape formatında** dışa aktarıp bu klasöre
`cookies.txt` adıyla koy. Uygulama otomatik kullanır.

## Notlar

- ffmpeg gerekmez (tek parça mp4 indirir).
- Yalnızca size ait veya izniniz olan içerikleri indirin; telife saygılı olun.
- Gizli hesaplar indirilemez.
