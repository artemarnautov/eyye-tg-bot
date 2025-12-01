// file: webapp/app.js
// Минимальный фронт без выбора тем:
// - Берём tg_id из URL (?tg_id=123)
// - Если tg_id валиден — грузим ленту из /api/feed
// - Рендерим карточки в тёмной теме

(function () {
  const feedEl = document.getElementById("feed");
  const feedLoadingEl = document.getElementById("feed-loading");
  const feedErrorEl = document.getElementById("feed-error");
  const footerStatusEl = document.getElementById("footer-status");

  if (!feedEl) {
    console.error("EYYE: #feed element not found");
    return;
  }

  function log(msg, extra) {
    try {
      console.log("[EYYE]", msg, extra || "");
    } catch (e) {}
  }

  // --- Читаем tg_id из URL ---
  const params = new URLSearchParams(window.location.search);
  const tgIdRaw = params.get("tg_id");
  const tgId = tgIdRaw ? parseInt(tgIdRaw, 10) : null;

  if (!tgId || Number.isNaN(tgId)) {
    // Если tg_id нет или он не число — просто показываем ошибку
    if (feedLoadingEl) feedLoadingEl.classList.add("hidden");
    if (feedErrorEl) {
      feedErrorEl.textContent =
        "No Telegram user id. Open this WebApp from the EYYE bot.";
      feedErrorEl.classList.remove("hidden");
    }
    if (footerStatusEl) {
      footerStatusEl.textContent = "Missing tg_id in URL";
    }
    log("No valid tg_id in URL, stopping");
    return;
  }

  // --- Нормализация ответа от API ---
  function normalizeItems(data) {
    if (!data) return [];
    if (Array.isArray(data)) return data;
    if (Array.isArray(data.items)) return data.items;
    if (Array.isArray(data.cards)) return data.cards;
    if (Array.isArray(data.data)) return data.data;
    return [];
  }

  // --- Загрузка фида ---
  async function loadFeed() {
    log("Loading feed for tg_id=" + tgId);

    if (feedErrorEl) {
      feedErrorEl.classList.add("hidden");
      feedErrorEl.textContent = "";
    }
    if (feedLoadingEl) {
      feedLoadingEl.classList.remove("hidden");
    }
    if (footerStatusEl) {
      footerStatusEl.textContent = "Loading feed...";
    }

    try {
      const resp = await fetch(
        "/api/feed?tg_id=" + encodeURIComponent(tgId) + "&limit=20",
        { method: "GET" }
      );

      if (!resp.ok) {
        throw new Error("HTTP " + resp.status);
      }

      const data = await resp.json();
      const items = normalizeItems(data);
      log("Feed loaded, items:", items.length);

      renderFeed(items);

      if (footerStatusEl) {
        footerStatusEl.textContent = "Loaded " + items.length + " posts";
      }
    } catch (err) {
      console.error("EYYE: failed to load feed", err);
      if (feedErrorEl) {
        feedErrorEl.textContent =
          "Could not load feed. Please try again later.";
        feedErrorEl.classList.remove("hidden");
      }
      if (footerStatusEl) {
        footerStatusEl.textContent = "Error loading feed";
      }
    } finally {
      if (feedLoadingEl) {
        feedLoadingEl.classList.add("hidden");
      }
    }
  }

  // --- Рендер карточек ---
  function renderFeed(items) {
    feedEl.innerHTML = "";

    if (!items || items.length === 0) {
      const emptyEl = document.createElement("div");
      emptyEl.className = "feed-status feed-status-empty";
      emptyEl.textContent =
        "No posts yet for your profile. Come back a bit later ✨";
      feedEl.appendChild(emptyEl);
      return;
    }

    items.forEach((item) => {
      const card = document.createElement("article");
      card.className = "feed-item";

      const title =
        item.title ||
        item.headline ||
        (item.meta && item.meta.title) ||
        "Untitled";

      const bodyText =
        item.body ||
        item.text ||
        item.content ||
        (item.meta && item.meta.summary) ||
        "";

      const tags =
        item.tags ||
        item.topics ||
        (item.meta && item.meta.tags) ||
        [];

      const createdAt = item.created_at || item.published_at || null;

      const titleEl = document.createElement("h3");
      titleEl.className = "feed-item-title";
      titleEl.textContent = title;

      const bodyEl = document.createElement("p");
      bodyEl.className = "feed-item-body";
      bodyEl.textContent = bodyText;

      const metaEl = document.createElement("div");
      metaEl.className = "feed-item-meta";

      if (createdAt) {
        const dt = new Date(createdAt);
        const dateSpan = document.createElement("span");
        dateSpan.className = "feed-item-date";
        dateSpan.textContent = dt.toLocaleString();
        metaEl.appendChild(dateSpan);
      }

      if (tags && tags.length) {
        const tagsSpan = document.createElement("span");
        tagsSpan.className = "feed-item-tags";
        tagsSpan.textContent = "#" + tags.slice(0, 3).join(" #");
        metaEl.appendChild(tagsSpan);
      }

      card.appendChild(titleEl);
      card.appendChild(bodyEl);
      card.appendChild(metaEl);

      feedEl.appendChild(card);
    });
  }

  // Старт
  loadFeed();
})();
