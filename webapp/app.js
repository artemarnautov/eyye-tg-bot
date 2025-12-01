// file: webapp/app.js
(function () {
  const feedEl = document.getElementById("feed");
  const loadingEl = document.getElementById("feed-loading");
  const errorEl = document.getElementById("feed-error");
  const footerStatusEl = document.getElementById("footer-status");

  function setFooter(text) {
    if (footerStatusEl) {
      footerStatusEl.textContent = text;
    }
  }

  function getTgId() {
    // 1) Пытаемся взять tg_id из URL: ?tg_id=...
    try {
      const params = new URLSearchParams(window.location.search);
      const fromUrl = params.get("tg_id");
      if (fromUrl) {
        const n = Number(fromUrl);
        if (!Number.isNaN(n) && n > 0) {
          return n;
        }
      }
    } catch (e) {
      console.warn("Failed to read tg_id from URL", e);
    }

    // 2) Пытаемся взять из Telegram WebApp, если есть
    try {
      const tgUser = window.Telegram?.WebApp?.initDataUnsafe?.user;
      if (tgUser && tgUser.id) {
        return tgUser.id;
      }
    } catch (e) {
      console.warn("Failed to read Telegram WebApp user", e);
    }

    return null;
  }

  function showError(message) {
    if (loadingEl) {
      loadingEl.classList.add("hidden");
    }
    if (errorEl) {
      errorEl.textContent = message || "Something went wrong.";
      errorEl.classList.remove("hidden");
    }
    setFooter("Error loading feed");
  }

  function renderEmpty() {
    if (!feedEl) return;
    feedEl.innerHTML = "";

    const emptyEl = document.createElement("div");
    emptyEl.className = "feed-status";
    emptyEl.textContent =
      "No posts yet. We are preparing your personal EYYE feed.";
    feedEl.appendChild(emptyEl);

    if (loadingEl) {
      loadingEl.classList.add("hidden");
    }
    if (errorEl) {
      errorEl.classList.add("hidden");
    }
    setFooter("Feed is empty for now");
  }

  function renderFeedItems(items) {
    if (!feedEl) return;

    feedEl.innerHTML = "";

    if (!items || !Array.isArray(items) || items.length === 0) {
      renderEmpty();
      return;
    }

    items.forEach((item) => {
      const card = document.createElement("article");
      card.className = "feed-item";

      const titleEl = document.createElement("h3");
      titleEl.className = "feed-item-title";
      titleEl.textContent = item.title || "Untitled";

      const bodyEl = document.createElement("p");
      bodyEl.className = "feed-item-body";
      bodyEl.textContent = item.body || "";

      const metaEl = document.createElement("div");
      metaEl.className = "feed-item-meta";

      const tags = Array.isArray(item.tags) ? item.tags : [];
      if (tags.length > 0) {
        const tagsEl = document.createElement("div");
        tagsEl.className = "feed-item-tags";
        tags.forEach((t) => {
          const tagSpan = document.createElement("span");
          tagSpan.className = "feed-item-tag";
          tagSpan.textContent = String(t);
          tagsEl.appendChild(tagSpan);
        });
        metaEl.appendChild(tagsEl);
      }

      card.appendChild(titleEl);
      card.appendChild(bodyEl);
      if (metaEl.childNodes.length > 0) {
        card.appendChild(metaEl);
      }

      feedEl.appendChild(card);
    });

    if (loadingEl) {
      loadingEl.classList.add("hidden");
    }
    if (errorEl) {
      errorEl.classList.add("hidden");
    }
    setFooter("Feed is up to date");
  }

  async function loadFeedOnce() {
    const tgId = getTgId();
    console.log("EYYE WebApp: tg_id =", tgId);

    if (!tgId) {
      showError("Telegram user id is missing. Open this from the EYYE bot.");
      return;
    }

    try {
      if (loadingEl) {
        loadingEl.classList.remove("hidden");
        loadingEl.textContent = "Loading your feed...";
      }
      if (errorEl) {
        errorEl.classList.add("hidden");
      }
      setFooter("Loading feed...");

      const resp = await fetch(
        `/api/feed?tg_id=${encodeURIComponent(tgId)}&limit=20`
      );
      if (!resp.ok) {
        throw new Error("HTTP " + resp.status);
      }

      const data = await resp.json();
      const items = data && Array.isArray(data.items) ? data.items : [];
      console.log("EYYE WebApp: received items =", items.length);
      renderFeedItems(items);
    } catch (err) {
      console.error("EYYE WebApp: loadFeed error", err);
      showError("Could not load feed. Please try again later.");
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    console.log("EYYE WebApp front started");
    loadFeedOnce();
  });
})();
