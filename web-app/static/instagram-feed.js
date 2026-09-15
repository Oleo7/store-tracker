/* Polarbär Instagram carousel. No requests until it approaches the viewport. */
(() => {
  'use strict';
  const script = document.currentScript;
  if (!script) return;
  const origin = new URL(script.src).origin;
  const reducedMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const element = (tag, className, text) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text) node.textContent = text;
    return node;
  };
  function valid(item) {
    return item && ['own', 'ugc'].includes(item.source) &&
      /^https:\/\/www\.instagram\.com\/(p|reel|tv)\/[A-Za-z0-9_-]+\/$/.test(item.permalink) &&
      (!item.preview_url || /^https:\/\/[A-Za-z0-9.-]+\/(?:.*)$/.test(item.preview_url));
  }
  function mount(root) {
    if (root.dataset.pbMounted) return;
    root.dataset.pbMounted = 'true';
    root.classList.add('pb-instagram');
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
    track.setAttribute('aria-label', 'Instagram-inlägg. Svep eller använd pilarna för att bläddra.');

    const cards = items.map((item, index) => {
      const link = element('a', 'pb-ig-card');
      link.href = item.permalink;
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      link.setAttribute('aria-label', `Öppna Instagram-inlägg ${index + 1} av ${items.length}${item.username ? ' från @' + item.username : ''}`);

      const media = element('div', 'pb-ig-media');
      if (item.preview_url) {
        const image = element('img', 'pb-ig-image');
        image.src = item.preview_url;
        image.alt = item.username ? `Instagram-inlägg från @${item.username}` : 'Instagram-inlägg';
        image.loading = index < 2 ? 'eager' : 'lazy';
        image.decoding = 'async';
        media.append(image);
      } else {
        media.append(element('div', 'pb-ig-placeholder', 'Visa på Instagram'));
      }
      if (item.media_type === 'VIDEO') {
        const play = element('span', 'pb-ig-play', '▶');
        play.setAttribute('aria-hidden', 'true');
        media.append(play);
      }
      const badge = element('span', 'pb-ig-badge', '◎');
      badge.setAttribute('aria-hidden', 'true');
      media.append(badge);

      const meta = element('div', 'pb-ig-meta');
      meta.append(element('span', 'pb-ig-user', item.username ? '@' + item.username : 'Instagram'));
      const open = element('span', 'pb-ig-open', 'Öppna ↗');
      meta.append(open);
      link.append(media, meta);
      track.append(link);
      return link;
    });

    section.append(header, track); root.replaceChildren(section);

    const stylesheet = document.querySelector('link[data-pb-instagram-css]');
    if (!stylesheet.sheet) {
      await new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('css_timeout')), 10000);
        stylesheet.addEventListener('load', () => { clearTimeout(timer); resolve(); }, {once: true});
        stylesheet.addEventListener('error', () => { clearTimeout(timer); reject(new Error('css_failed')); }, {once: true});
      });
    }

    let autoplayTimer = null;
    let resumeTimer = null;
    let sectionVisible = true;
    const stepSize = () => {
      const style = getComputedStyle(track);
      const gap = parseFloat(style.columnGap || style.gap || '0') || 0;
      return cards[0].getBoundingClientRect().width + gap;
    };
    const nearEnd = () => track.scrollLeft + track.clientWidth >= track.scrollWidth - 4;
    const navigate = (direction, manual = false) => {
      if (!cards.length) return;
      if (manual) pauseAutoplay();
      const left = direction > 0 && nearEnd() ? 0 : Math.max(0, track.scrollLeft + direction * stepSize());
      track.scrollTo({left, behavior: reducedMotion ? 'auto' : 'smooth'});
    };
    const updateButtons = () => {
      previous.disabled = track.scrollLeft <= 2;
      next.disabled = false;
    };
    const stopAutoplay = () => {
      if (autoplayTimer) clearInterval(autoplayTimer);
      autoplayTimer = null;
    };
    const startAutoplay = () => {
      if (reducedMotion || autoplayTimer || !sectionVisible || document.hidden || cards.length < 2) return;
      autoplayTimer = setInterval(() => navigate(1, false), 4200);
    };
    const pauseAutoplay = () => {
      stopAutoplay();
      if (resumeTimer) clearTimeout(resumeTimer);
      resumeTimer = setTimeout(startAutoplay, 8000);
    };

    previous.addEventListener('click', () => navigate(-1, true));
    next.addEventListener('click', () => navigate(1, true));
    track.addEventListener('keydown', event => {
      if (event.target !== track || !['ArrowLeft', 'ArrowRight'].includes(event.key)) return;
      event.preventDefault(); navigate(event.key === 'ArrowRight' ? 1 : -1, true);
    });
    ['pointerdown', 'touchstart', 'wheel'].forEach(name => track.addEventListener(name, pauseAutoplay, {passive: true}));
    track.addEventListener('scroll', updateButtons, {passive: true});
    section.addEventListener('mouseenter', pauseAutoplay);
    section.addEventListener('mouseleave', startAutoplay);
    document.addEventListener('visibilitychange', () => document.hidden ? stopAutoplay() : startAutoplay());
    if ('ResizeObserver' in window) new ResizeObserver(updateButtons).observe(track);
    if ('IntersectionObserver' in window) {
      const visibilityObserver = new IntersectionObserver(entries => {
        sectionVisible = entries.some(entry => entry.isIntersecting && entry.intersectionRatio >= 0.35);
        sectionVisible ? startAutoplay() : stopAutoplay();
      }, {threshold: [0, .35, .75]});
      visibilityObserver.observe(section);
    }
    updateButtons();
    startAutoplay();
  }
  document.querySelectorAll('[data-polarbar-instagram]').forEach(mount);
})();
