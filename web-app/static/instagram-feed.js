/* Polarbär Instagram section. No requests until it approaches the viewport. */
(() => {
  'use strict';
  const script = document.currentScript;
  if (!script) return;
  const origin = new URL(script.src).origin;
  let embedPromise;
  const element = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text) node.textContent = text;
    return node;
  };
  function loadEmbed() {
    if (window.instgrm?.Embeds) return Promise.resolve();
    if (embedPromise) return embedPromise;
    embedPromise = new Promise((resolve, reject) => {
      const existing = document.querySelector('script[src="https://www.instagram.com/embed.js"]');
      const js = existing || element('script');
      const timeout = setTimeout(() => reject(new Error('embed_timeout')), 15000);
      js.addEventListener('load', () => { clearTimeout(timeout); resolve(); }, {once: true});
      js.addEventListener('error', () => { clearTimeout(timeout); reject(new Error('embed_failed')); }, {once: true});
      if (!existing) { js.src = 'https://www.instagram.com/embed.js'; js.async = true; document.head.append(js); }
    });
    return embedPromise;
  }
  function valid(item) {
    return item && ['own', 'ugc'].includes(item.source) &&
      /^https:\/\/www\.instagram\.com\/(p|reel|tv)\/[A-Za-z0-9_-]+\/$/.test(item.permalink);
  }
  function mount(root) {
    if (root.dataset.pbMounted) return;
    root.dataset.pbMounted = 'true';
    root.classList.add('pb-instagram');
    // Keep a small observable mount point. Never load eagerly without an observer.
    if (!('IntersectionObserver' in window)) { root.hidden = true; return; }
    const observer = new IntersectionObserver(entries => {
      if (!entries.some(entry => entry.isIntersecting)) return;
      observer.disconnect();
      activate(root).catch(() => { root.hidden = true; });
    }, {rootMargin: '400px 0px'});
    observer.observe(root);
  }
  async function activate(root) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 35000);
    let data;
    try {
      const response = await fetch(origin + '/api/public/instagram-feed', {
        credentials: 'omit', signal: controller.signal, mode: 'cors'
      });
      if (!response.ok) throw new Error('feed_unavailable');
      data = await response.json();
    } finally { clearTimeout(timeout); }
    const items = Array.isArray(data.items) ? data.items.filter(valid).slice(0, 24) : [];
    if (!items.length) { root.hidden = true; return; }
    if (!document.querySelector('link[data-pb-instagram-css]')) {
      const css = element('link');
      css.rel = 'stylesheet'; css.href = origin + '/static/instagram-feed.css';
      css.dataset.pbInstagramCss = 'true'; document.head.append(css);
    }
    const section = element('section', 'pb-ig-section');
    section.setAttribute('aria-label', 'Polarbär hos er');
    const header = element('div', 'pb-ig-header');
    const copy = element('div');
    copy.append(element('h2', 'pb-ig-title', 'Polarbär hos er'));
    const intro = element('p', 'pb-ig-intro', 'Tagga ');
    const profile = element('a', '', '@polarbar.se');
    profile.href = 'https://www.instagram.com/polarbar.se/';
    profile.target = '_blank'; profile.rel = 'noopener noreferrer';
    intro.append(profile, document.createTextNode(' för chansen att synas här'));
    copy.append(intro); header.append(copy);
    const controls = element('div', 'pb-ig-controls');
    const previous = element('button', 'pb-ig-arrow', '←');
    const next = element('button', 'pb-ig-arrow', '→');
    previous.type = next.type = 'button';
    previous.setAttribute('aria-label', 'Föregående Instagram-inlägg');
    next.setAttribute('aria-label', 'Nästa Instagram-inlägg');
    controls.append(previous, next); header.append(controls);
    const track = element('div', 'pb-ig-track');
    track.tabIndex = 0;
    track.setAttribute('role', 'region');
    track.setAttribute('aria-label', 'Instagram-inlägg. Bläddra med piltangenterna eller svep.');
    const cards = items.map((item, index) => {
      const card = element('article', 'pb-ig-card');
      card.setAttribute('aria-label', `Inlägg ${index + 1} av ${items.length}`);
      const frame = element('div', 'pb-ig-frame');
      const fallback = element('a', 'pb-ig-original', 'Visa inlägget på Instagram ↗');
      fallback.href = item.permalink; fallback.target = '_blank'; fallback.rel = 'noopener noreferrer';
      // A permanent original link remains usable for private/deleted/blocked embeds.
      card.append(frame, fallback); track.append(card);
      return {card, frame, item, loaded: false};
    });
    section.append(header, track); root.replaceChildren(section);
    const updateButtons = () => {
      previous.disabled = track.scrollLeft <= 2;
      next.disabled = track.scrollLeft + track.clientWidth >= track.scrollWidth - 2;
    };
    const navigate = direction => {
      const step = cards[0].card.getBoundingClientRect().width + 24;
      track.scrollBy({left: direction * step, behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth'});
    };
    previous.addEventListener('click', () => navigate(-1));
    next.addEventListener('click', () => navigate(1));
    track.addEventListener('keydown', event => {
      if (event.target !== track || !['ArrowLeft', 'ArrowRight'].includes(event.key)) return;
      event.preventDefault(); navigate(event.key === 'ArrowRight' ? 1 : -1);
    });
    track.addEventListener('scroll', updateButtons, {passive: true});
    if ('ResizeObserver' in window) new ResizeObserver(updateButtons).observe(track);
    updateButtons();
    // Root clipping prevents off-screen cards from becoming Instagram blockquotes.
    const cardObserver = new IntersectionObserver(entries => {
      for (const entry of entries) {
        if (!entry.isIntersecting) continue;
        const record = cards.find(value => value.card === entry.target);
        if (record.loaded) continue;
        record.loaded = true; cardObserver.unobserve(record.card);
        const quote = element('blockquote', 'instagram-media');
        quote.dataset.instgrmPermalink = record.item.permalink;
        quote.dataset.instgrmVersion = '14';
        const link = element('a', '', 'Visa inlägget på Instagram');
        link.href = record.item.permalink; quote.append(link); record.frame.append(quote);
        loadEmbed().then(() => window.instgrm?.Embeds?.process()).catch(() => {
          record.frame.replaceChildren(); record.card.classList.add('pb-ig-unavailable');
        });
      }
    }, {root: track, rootMargin: '0px 100px', threshold: 0.01});
    // Wait for the small stylesheet before measuring cards, avoiding eager loading
    // caused by an unstyled vertical list when CSS arrives slowly.
    const stylesheet = document.querySelector('link[data-pb-instagram-css]');
    if (!stylesheet.sheet) {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('css_timeout')), 10000);
        stylesheet.addEventListener('load', () => { clearTimeout(timer); resolve(); }, {once: true});
        stylesheet.addEventListener('error', () => { clearTimeout(timer); reject(new Error('css_failed')); }, {once: true});
      });
    }
    cards.forEach(({card}) => cardObserver.observe(card));
    updateButtons();
  }
  document.querySelectorAll('[data-polarbar-instagram]').forEach(mount);
})();
