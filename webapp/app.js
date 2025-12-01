// file: webapp/app.js
(function () {
  'use strict';

  try {
    console.log('[EYYE] app.js starting');

    const feedEl = document.getElementById('feed');
    const feedLoadingEl = document.getElementById('feed-loading');
    const feedErrorEl = document.getElementById('feed-error');
    const footerStatusEl = document.getElementById('footer-status');
    const onboardingEl = document.getElementById('theme-onboarding');
    const themeButtons = document.querySelectorAll('.theme-btn');

    if (!feedEl || !feedLoadingEl || !feedErrorEl || !footerStatusEl || !onboardingEl) {
      console.error('[EYYE] Missing some DOM elements', {
        feedEl: !!feedEl,
        feedLoadingEl: !!feedLoadingEl,
        feedErrorEl: !!feedErrorEl,
        footerStatusEl: !!footerStatusEl,
        onboardingEl: !!onboardingEl,
      });
    }

    const params = new URLSearchParams(window.location.search);
    const tgId = params.get('tg_id');

    if (!tgId) {
      console.error('[EYYE] Missing tg_id in URL');
      if (feedLoadingEl) feedLoadingEl.classList.add('hidden');
      if (feedErrorEl) {
        feedErrorEl.textContent = 'Missing tg_id. Please open EYYE from Telegram bot.';
        feedErrorEl.classList.remove('hidden');
      }
      if (footerStatusEl) footerStatusEl.textContent = 'Error: no tg_id';
      return;
    }

    const THEME_KEY = 'eyye_theme_' + tgId;

    function setFooter(text) {
      if (footerStatusEl) {
        footerStatusEl.textContent = text;
      }
    }

    function clearThemes() {
      ['theme-light', 'theme-dark', 'theme-glass', 'theme-classic'].forEach((cls) => {
        document.body.classList.remove(cls);
      });
    }

    function applyTheme(theme) {
      clearThemes();
      const cls = 'theme-' + theme;
      document.body.classList.add(cls);
      setFooter('Theme: ' + theme);
      console.log('[EYYE] Applied theme', cls);
    }

    function saveTheme(theme) {
      try {
        localStorage.setItem(THEME_KEY, theme);
        console.log('[EYYE] Saved theme', theme, 'for', THEME_KEY);
      } catch (e) {
        console.warn('[EYYE] Failed to save theme', e);
      }
    }

    function getSavedTheme() {
      try {
        return localStorage.getItem(THEME_KEY);
      } catch (e) {
        console.warn('[EYYE] Failed to read theme from localStorage', e);
        return null;
      }
    }

    function renderFeed(items) {
      if (!feedEl) return;

      // Удаляем все карточки, но не статусы
      const statusIds = new Set(['feed-loading', 'feed-error']);
      Array.from(feedEl.children).forEach((child) => {
        if (child.id && statusIds.has(child.id)) return;
        feedEl.removeChild(child);
      });

      if (!items || !items.length) {
        if (feedErrorEl) {
          feedErrorEl.textContent = 'No posts yet for your profile. Come back later ✨';
          feedErrorEl.classList.remove('hidden');
        }
        return;
      }

      items.forEach((item) => {
        const card = document.createElement('article');
        card.className = 'feed-item';

        const titleEl = document.createElement('h3');
        titleEl.className = 'feed-item-title';
        titleEl.textContent = item.title || '';

        const bodyEl = document.createElement('p');
        bodyEl.className = 'feed-item-body';
        bodyEl.textContent = item.body || '';

        const metaEl = document.createElement('div');
        metaEl.className = 'feed-item-meta';
        const category = item.category || '';
        const created = item.created_at || '';
        metaEl.textContent = [category, created].filter(Boolean).join(' • ');

        card.appendChild(titleEl);
        card.appendChild(bodyEl);
        card.appendChild(metaEl);

        feedEl.appendChild(card);
      });
    }

    async function loadFeed() {
      console.log('[EYYE] loadFeed() called for tg_id=', tgId);

      if (feedErrorEl) {
        feedErrorEl.classList.add('hidden');
        feedErrorEl.textContent = '';
      }
      if (feedLoadingEl) {
        feedLoadingEl.classList.remove('hidden');
        feedLoadingEl.textContent = 'Loading your feed...';
      }
      setFooter('Loading feed...');

      try {
        const resp = await fetch(`/api/feed?tg_id=${encodeURIComponent(tgId)}&limit=20`);
        console.log('[EYYE] /api/feed response status', resp.status);

        if (!resp.ok) {
          throw new Error(`HTTP ${resp.status}`);
        }

        const data = await resp.json();
        const items = data && Array.isArray(data.items) ? data.items : [];
        console.log('[EYYE] Loaded items count', items.length);

        if (feedLoadingEl) {
          feedLoadingEl.classList.add('hidden');
        }

        renderFeed(items);

        setFooter(
          items.length
            ? `Loaded ${items.length} posts`
            : 'No posts yet for your profile. Come back later ✨'
        );
      } catch (err) {
        console.error('[EYYE] loadFeed error', err);
        if (feedLoadingEl) {
          feedLoadingEl.classList.add('hidden');
        }
        if (feedErrorEl) {
          feedErrorEl.textContent = 'Failed to load feed. Please try again.';
          feedErrorEl.classList.remove('hidden');
        }
        setFooter('Error loading feed');
      }
    }

    // Навешиваем обработчики на кнопки выбора темы
    themeButtons.forEach((btn) => {
      btn.addEventListener('click', () => {
        const theme = btn.dataset.theme;
        console.log('[EYYE] Theme button clicked', theme);
        if (!theme) return;

        applyTheme(theme);
        saveTheme(theme);

        if (onboardingEl) {
          onboardingEl.classList.add('hidden');
        }

        // Сразу загружаем ленту после выбора темы
        loadFeed();
      });
    });

    const savedTheme = getSavedTheme();
    if (savedTheme) {
      console.log('[EYYE] Found saved theme', savedTheme);
      if (onboardingEl) {
        onboardingEl.classList.add('hidden');
      }
      applyTheme(savedTheme);
      loadFeed();
    } else {
      console.log('[EYYE] No saved theme, show onboarding');
      if (onboardingEl) {
        onboardingEl.classList.remove('hidden');
      }
      setFooter('Choose your theme to start');
    }
  } catch (err) {
    console.error('[EYYE] Fatal init error in app.js', err);
  }
})();
