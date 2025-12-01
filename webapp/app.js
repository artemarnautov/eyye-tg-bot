// file: webapp/app.js

const FOOTER_STATUS = document.getElementById("footer-status");
const feedEl = document.getElementById("feed");
const feedLoadingEl = document.getElementById("feed-loading");
const feedErrorEl = document.getElementById("feed-error");
const themeOnboardingEl = document.getElementById("theme-onboarding");

const THEME_KEY_PREFIX = "eyye_theme_";

// ===========================
// Утилиты
// ===========================

function getQueryParam(name) {
  const url = new URL(window.location.href);
  return url.searchParams.get(name);
}

function themeStorageKey(tgId) {
  return `${THEME_KEY_PREFIX}${tgId}`;
}

function applyTheme(theme) {
  document.body.dataset.theme = theme;
}

function setFooter(text) {
  if (FOOTER_STATUS) FOOTER_STATUS.textContent = text;
}

function formatDateTime(isoString) {
  if (!isoString) return "";
  try {
    const d = new Date(isoString);
    return d.toLocaleString(undefined, {
      hour: "2-digit",
      minute: "2-digit",
      day: "2-digit",
      month: "short",
    });
  } catch (e) {
    return "";
  }
}

function escapeHtml(str) {
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// ===========================
// Тема: выбор один раз на пользователя
// ===========================

function initThemeForUser(tgId) {
  if (!themeOnboardingEl) return;

  const key = themeStorageKey(tgId);
  const saved = localStorage.getItem(key);
  const buttons = themeOnboardingEl.querySelectorAll(".theme-btn");

  // Клики по кнопкам — сохраняем тему **под этим tg_id**
  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const theme = btn.dataset.theme || "classic";
      localStorage.setItem(key, theme);
      applyTheme(theme);
      themeOnboardingEl.classList.add("hidden");
    });
  });

  if (saved) {
    // Пользователь уже выбрал: применяем и убираем онбординг
    applyTheme(saved);
    themeOnboardingEl.classList.add("hidden");
  } else {
    // Первый заход этого tg_id: просим выбрать стиль
    themeOnboardingEl.classList.remove("hidden");
  }
}

// ===========================
// API
// ===========================

async function fetchFeed(tgId, limit = 20) {
  const url = `/api/feed?tg_id=${encodeURIComponent(tgId)}&limit=${limit}`;
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Feed request failed: ${res.status}`);
  }
  const data = await res.json();
  return data.items || [];
}

async function sendTelemetry(tgId, eventType, cardId, meta = {}) {
  const payload = {
    tg_id: Number(tgId),
    event_type: eventType,
    card_id: cardId,
    meta,
  };
  try {
    await fetch("/api/telemetry", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (e) {
    console.debug("Telemetry send failed", e);
  }
}

// ===========================
// Рендер карточек
// ===========================

function createPostCard(tgId, card) {
  const wrapper = document.createElement("article");
  wrapper.className = "post-card";
  wrapper.dataset.cardId = card.id;

  const createdAt = formatDateTime(card.created_at);

  wrapper.innerHTML = `
    <div class="post-header">
      <div class="post-header-avatar">E</div>
      <div class="post-header-main">
        <div class="post-header-title">EYYE · ${card.category || "News"}</div>
        <div class="post-header-meta">${createdAt}</div>
      </div>
    </div>
    <div class="post-title">${escapeHtml(card.title || "")}</div>
    <div class="post-body">${escapeHtml(card.body || "")}</div>
    <div class="post-footer">
      <div class="post-tags"></div>
      <div class="post-actions">
        <button class="post-action post-action-like">
          <span class="post-action-icon">👍</span>
          <span>Like</span>
        </button>
        <button class="post-action post-action-more">
          <span class="post-action-icon">⋯</span>
        </button>
      </div>
    </div>
  `;

  const tagsEl = wrapper.querySelector(".post-tags");
  const tags = Array.isArray(card.tags) ? card.tags : [];
  tags.slice(0, 4).forEach((t) => {
    const tag = document.createElement("span");
    tag.className = "post-tag";
    tag.textContent = String(t);
    tagsEl.appendChild(tag);
  });

  wrapper.addEventListener("click", (event) => {
    const isActionButton = event.target.closest(".post-action");
    if (!isActionButton) {
      sendTelemetry(tgId, "card_click", card.id, { source: "card_body" });
    }
  });

  const likeBtn = wrapper.querySelector(".post-action-like");
  likeBtn.addEventListener("click", (event) => {
    event.stopPropagation();
    likeBtn.classList.add("active");
    sendTelemetry(tgId, "card_like", card.id, {});
  });

  const moreBtn = wrapper.querySelector(".post-action-more");
  moreBtn.addEventListener("click", (event) => {
    event.stopPropagation();
    sendTelemetry(tgId, "card_read_full", card.id, {});
  });

  sendTelemetry(tgId, "card_view", card.id, { position: card.position });

  return wrapper;
}

// ===========================
// Инициализация фида
// ===========================

async function initFeedForUser(tgId) {
  document.body.classList.add("loading");
  setFooter("Loading feed...");

  try {
    const items = await fetchFeed(tgId, 25);

    if (feedLoadingEl) feedLoadingEl.classList.add("hidden");
    if (feedErrorEl) feedErrorEl.classList.add("hidden");

    if (!items.length) {
      const emptyEl = document.createElement("div");
      emptyEl.className = "feed-status";
      emptyEl.textContent =
        "No posts yet for your profile. Come back a bit later, we’re generating content for you.";
      feedEl.appendChild(emptyEl);
      setFooter("No items in feed");
    } else {
      items.forEach((item, index) => {
        item.position = index;
        const card = createPostCard(tgId, item);
        feedEl.appendChild(card);
      });
      setFooter(`Loaded ${items.length} posts`);
    }
  } catch (e) {
    console.error(e);
    if (feedLoadingEl) feedLoadingEl.classList.add("hidden");
    if (feedErrorEl) {
      feedErrorEl.textContent = "Failed to load feed. Please try again.";
      feedErrorEl.classList.remove("hidden");
    }
    setFooter("Error loading feed");
  } finally {
    document.body.classList.remove("loading");
  }
}

// ===========================
// Точка входа
// ===========================

document.addEventListener("DOMContentLoaded", () => {
  const tgId = getQueryParam("tg_id");

  if (!tgId) {
    if (feedLoadingEl) feedLoadingEl.classList.add("hidden");
    if (feedErrorEl) {
      feedErrorEl.textContent =
        "Missing tg_id in URL. Launch this WebApp from Telegram bot.";
      feedErrorEl.classList.remove("hidden");
    }
    setFooter("tg_id missing");
    return;
  }

  // 1) выбор темы — привязан к этому tg_id
  initThemeForUser(tgId);

  // 2) загрузка ленты для этого tg_id
  initFeedForUser(tgId);
});
