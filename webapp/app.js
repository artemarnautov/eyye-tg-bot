// file: webapp/app.js
(function () {
  const mainEl = document.getElementById("main");

  const TOPICS = [
    { tag: "world_news", label: "Главные новости" },
    { tag: "business", label: "Бизнес и экономика" },
    { tag: "finance", label: "Финансы и крипто" },
    { tag: "tech", label: "Технологии и гаджеты" },
    { tag: "science", label: "Наука" },
    { tag: "history", label: "История" },
    { tag: "politics", label: "Политика" },
    { tag: "society", label: "Общество и культура" },
    { tag: "entertainment", label: "Кино и сериалы" },
    { tag: "gaming", label: "Игры и киберспорт" },
    { tag: "sports", label: "Спорт" },
    { tag: "lifestyle", label: "Жизнь и лайфстайл" },
    { tag: "education", label: "Образование и карьера" },
    { tag: "city", label: "Город и локальные новости" },
    { tag: "uk_students", label: "Студенческая жизнь в Великобритании" },
  ];

  const state = {
    userId: null,
    step: "city", // city | topics | feed
    selectedTags: new Set(),
    feedItems: [],
  };

  const tg =
    window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;

  function escapeHtml(str) {
    if (!str) return "";
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function getUserId() {
    if (
      tg &&
      tg.initDataUnsafe &&
      tg.initDataUnsafe.user &&
      tg.initDataUnsafe.user.id
    ) {
      return tg.initDataUnsafe.user.id;
    }
    const params = new URLSearchParams(window.location.search);
    const fromQuery = params.get("tg_id");
    if (fromQuery) {
      const n = Number(fromQuery);
      if (!Number.isNaN(n)) return n;
      return fromQuery;
    }
    return null;
  }

  function apiFetch(path, options) {
    return fetch(path, {
      headers: {
        "Content-Type": "application/json",
      },
      ...options,
    });
  }

  function renderCityScreen() {
    state.step = "city";
    mainEl.innerHTML = `
      <section class="screen screen-city">
        <h1 class="screen-title">Где ты живёшь?</h1>
        <p class="screen-subtitle">
          Это нужно, чтобы подмешивать в ленту локальные новости и контекст.
          Можно пропустить — тогда лента будет глобальной.
        </p>
        <div class="city-form">
          <input
            id="city-input"
            class="input"
            type="text"
            placeholder="Например: Москва, Лондон, Дубай"
            autocomplete="off"
          />
        </div>
        <div class="buttons-row">
          <button id="city-continue" class="btn btn-primary">
            Продолжить
          </button>
          <button id="city-skip" class="btn btn-secondary">
            Пропустить
          </button>
        </div>
      </section>
    `;

    const cityInput = document.getElementById("city-input");
    const continueBtn = document.getElementById("city-continue");
    const skipBtn = document.getElementById("city-skip");

    continueBtn.addEventListener("click", function () {
      const city = (cityInput.value || "").trim();
      saveCityAndGoNext(city);
    });

    cityInput.addEventListener("keydown", function (e) {
      if (e.key === "Enter") {
        const city = (cityInput.value || "").trim();
        saveCityAndGoNext(city);
      }
    });

    skipBtn.addEventListener("click", function () {
      saveCityAndGoNext("");
    });

    cityInput.focus();
  }

  function saveCityAndGoNext(city) {
    if (!state.userId) {
      renderError(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    apiFetch("/api/profile/city", {
      method: "POST",
      body: JSON.stringify({
        user_id: state.userId,
        city: city || null,
      }),
    })
      .then(function (resp) {
        if (!resp.ok) {
          console.warn("Failed to save city", resp.status);
        }
      })
      .catch(function (err) {
        console.warn("Error saving city", err);
      })
      .finally(function () {
        renderTopicsScreen();
      });
  }

  function renderTopicsScreen() {
    state.step = "topics";

    const topicHtml = TOPICS.map(function (t) {
      const active = state.selectedTags.has(t.tag);
      return `
        <button
          class="topic-pill ${active ? "topic-pill--selected" : ""}"
          data-tag="${t.tag}"
        >
          <span class="topic-pill-label">${escapeHtml(t.label)}</span>
        </button>
      `;
    }).join("");

    mainEl.innerHTML = `
      <section class="screen screen-topics">
        <h1 class="screen-title">Что тебе интересно читать?</h1>
        <p class="screen-subtitle">
          Выбери несколько тем — лента будет под тебя. Можно изменить выбор позже.
        </p>
        <div class="topics-grid">
          ${topicHtml}
        </div>
        <div class="buttons-row">
          <button id="topics-continue" class="btn btn-primary">
            Сформировать ленту
          </button>
        </div>
      </section>
    `;

    Array.prototype.forEach.call(
      document.querySelectorAll(".topic-pill"),
      function (el) {
        el.addEventListener("click", function () {
          const tag = el.getAttribute("data-tag");
          if (!tag) return;
          if (state.selectedTags.has(tag)) {
            state.selectedTags.delete(tag);
            el.classList.remove("topic-pill--selected");
          } else {
            state.selectedTags.add(tag);
            el.classList.add("topic-pill--selected");
          }
        });
      }
    );

    const continueBtn = document.getElementById("topics-continue");
    continueBtn.addEventListener("click", function () {
      saveTopicsAndLoadFeed();
    });
  }

  function saveTopicsAndLoadFeed() {
    if (!state.userId) {
      renderError(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    const tags = Array.from(state.selectedTags);

    apiFetch("/api/profile/topics", {
      method: "POST",
      body: JSON.stringify({
        user_id: state.userId,
        tags: tags,
      }),
    })
      .then(function (resp) {
        if (!resp.ok) {
          console.warn("Failed to save topics", resp.status);
        }
      })
      .catch(function (err) {
        console.warn("Error saving topics", err);
      })
      .finally(function () {
        loadFeed();
      });
  }

  function loadFeed() {
    if (!state.userId) {
      renderError(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    state.step = "feed";

    mainEl.innerHTML = `
      <section class="screen screen-feed screen-feed--loading">
        <p class="screen-subtitle">
          Собираю для тебя персональную ленту…
        </p>
      </section>
    `;

    const params = new URLSearchParams({
      tg_id: String(state.userId),
      limit: "25",
    });

    apiFetch("/api/feed?" + params.toString(), {
      method: "GET",
    })
      .then(function (resp) {
        if (!resp.ok) {
          throw new Error("Feed HTTP " + resp.status);
        }
        return resp.json();
      })
      .then(function (data) {
        const items = (data && data.items) || [];
        state.feedItems = items;
        renderFeedScreen();
      })
      .catch(function (err) {
        console.error("Failed to load feed", err);
        renderError(
          "Не получилось загрузить ленту. Попробуй вернуться в WebApp через пару минут."
        );
      });
  }

  function renderFeedScreen() {
    state.step = "feed";

    if (!state.feedItems || state.feedItems.length === 0) {
      mainEl.innerHTML = `
        <section class="screen screen-feed">
          <p class="screen-subtitle">
            Пока для тебя нет готовых карточек. Я уже готовлю контент —
            загляни сюда чуть позже.
          </p>
        </section>
      `;
      return;
    }

    const cardsHtml = state.feedItems
      .map(function (item) {
        const title = escapeHtml(item.title || "");
        const body = escapeHtml(item.body || "").replace(/\n/g, "<br />");
        const createdAt = item.created_at || item.createdAt;

        let meta = "";
        if (createdAt) {
          meta = `<div class="feed-card-meta">Опубликовано: ${escapeHtml(
            String(createdAt)
          )}</div>`;
        }

        return `
          <article class="feed-card">
            <h2 class="feed-card-title">${title}</h2>
            <div class="feed-card-body">${body}</div>
            ${meta}
          </article>
        `;
      })
      .join("");

    mainEl.innerHTML = `
      <section class="screen screen-feed">
        ${cardsHtml}
        <div class="feed-footer">
          <span class="feed-footer-text">Лента обновлена только что</span>
        </div>
      </section>
    `;
  }

  function renderError(message) {
    mainEl.innerHTML = `
      <section class="screen screen-error">
        <p class="screen-subtitle">
          ${escapeHtml(message)}
        </p>
      </section>
    `;
  }

  function init() {
    state.userId = getUserId();

    if (tg) {
      try {
        tg.expand();
        tg.ready();
      } catch (e) {
        console.warn("Telegram WebApp init error", e);
      }
    }

    if (!state.userId) {
      renderError(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    renderCityScreen();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
