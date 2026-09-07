"""Local visual fixture: real public embeds, no Meta token or CRM calls."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flask import Flask, jsonify

app = Flask(__name__, static_folder=str(Path(__file__).resolve().parents[1] / 'static'))
@app.get('/api/public/instagram-feed')
def feed():
    urls = ['reel/Dcf85c0IcJe', 'reel/Da5g2fOIZuv', 'p/DZ5WpZXqiDv', 'p/DZgIwK5gxyq'] * 3
    return jsonify(items=[{'id': str(i), 'permalink': 'https://www.instagram.com/' + path + '/',
                          'source': 'own' if i % 3 == 2 else 'ugc'} for i, path in enumerate(urls)])

@app.get('/')
def index():
    return '''<!doctype html><html lang="sv"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Polarbär Instagram – lokal verifiering</title>
<style>body{font-family:Arial,sans-serif;margin:0;color:#171717}main{max-width:1200px;margin:auto;padding:0 24px}.spacer{height:1600px;padding-top:48px}#checks{position:fixed;top:0;left:0;background:#fff;padding:12px;z-index:9;font:12px monospace;border:1px solid #aaa}</style>
<div id="checks" role="status"></div><main><div class="spacer"><h1>Polarbär – test av sen laddning</h1><p>Scrolla ned till Instagramsektionen.</p></div>
<div data-polarbar-instagram style="min-height:1px;width:100%"></div><p>Resten av sidan fungerar oberoende av Instagram.</p></main>
<script defer src="/static/instagram-feed.js"></script>
<script>setInterval(()=>{const r=performance.getEntriesByType('resource');document.getElementById('checks').textContent=
'Feed requests: '+r.filter(x=>x.name.includes('/api/public/instagram-feed')).length+
' | Embed scripts: '+r.filter(x=>x.name.includes('instagram.com/embed.js')).length+
' | Loaded cards: '+document.querySelectorAll('.pb-ig-frame iframe,.pb-ig-frame blockquote').length+
' | Width: '+innerWidth+' | Page overflow: '+(document.documentElement.scrollWidth>innerWidth);},300);</script></html>'''

if __name__ == '__main__':
    app.run(port=5056, threaded=True)
