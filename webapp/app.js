// file: webapp/app.js
(function () {
  const API_BASE = ""; // тот же домен/порт, где крутится backend

  const AVAILABLE_TOPICS = [
    { id: "world_news", label: "Мир" },
    { id: "business", label: "Бизнес" },
    { id: "finance", label: "Финансы / Крипто" },
    { id: "tech", label: "Технологии" },
    { id: "science", label: "Наука" },
    { id: "history", label: "История" },
    { id: "politics", label: "Политика" },
    { id: "society", label: "Общество" },
    { id: "entertainment", label: "Кино / Сериалы" },
    { id: "gaming", label: "Игры" },
    { id: "sports", label: "Спорт" },
    { id: "lifestyle", label: "Лайфстайл" },
    { id: "education", label: "Образование / Карьера" },
    { id: "city", label: "Город / Локальные новости" },
    { id: "uk_students", label: "Студенческая жизнь в UK" }
  ];

  const state = {
    tgId: null,

    // онбординг
    onboardingDone: false,
    cityDraft: "",
    selectedTopics: new Set(),

    // фид
    feedItems: [],
    currentIndex: 0,
    loadingFeed: false,
    hasMore: true,
    nextOffset: 0,
    seenIds: new Set(),

    // свайпы
    wheelLocked: false,
    touchStartY: null,
    swipeListenersAttached: false
  };

  const tg =
    window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;

  // ==========
  // УТИЛИТЫ
  // ==========

  function escapeHtml(str) {
    if (!str) return "";
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function apiFetch(path, options) {
    return fetch(API_BASE + path, {
      headers: {
        "Content-Type": "application/json"
      },
      ...options
    });
  }

  function getTgId() {
    // 1) Берём из Telegram WebApp
    if (
      tg &&
      tg.initDataUnsafe &&
      tg.initDataUnsafe.user &&
      tg.initDataUnsafe.user.id
    ) {
      return tg.initDataUnsafe.user.id;
    }

    // 2) Фолбек — из query-параметра tg_id (для локальной отладки)
    const params = new URLSearchParams(window.location.search);
    const fromQuery = params.get("tg_id");
    if (fromQuery) {
      const n = Number(fromQuery);
      if (!Number.isNaN(n)) return n;
      return fromQuery;
    }

    return null;
  }

  // ==========
  // ОНБОРДИНГ: UI
  // ==========

  function setupOnboardingUI() {
    const cityInput = document.getElementById("city-input");
    const citySkipBtn = document.getElementById("city-skip-btn");
    const cityNextBtn = document.getElementById("city-next-btn");
    const topicsContainer = document.getElementById("topics-container");
    const topicsSubmitBtn = document.getElementById("topics-submit-btn");

    if (!cityInput || !topicsContainer || !topicsSubmitBtn) {
      console.warn("Onboarding DOM elements not found");
      return;
    }

    // Подставляем уже известный город (если есть)
    if (state.cityDraft) {
      cityInput.value = state.cityDraft;
    }

    cityInput.addEventListener("input", function (e) {
      state.cityDraft = e.target.value;
    });

    if (citySkipBtn) {
      citySkipBtn.addEventListener("click", function () {
        state.cityDraft = "";
        goToTopicsStep();
      });
    }

    if (cityNextBtn) {
      cityNextBtn.addEventListener("click", function () {
        goToTopicsStep();
      });
    }

    // Рендерим чипы тем
    topicsContainer.innerHTML = "";
    AVAILABLE_TOPICS.forEach(function (topic) {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "topic-chip";
      chip.dataset.topicId = topic.id;
      chip.textContent = topic.label;

      if (state.selectedTopics.has(topic.id)) {
        chip.classList.add("selected");
      }

      chip.addEventListener("click", function () {
        const id = chip.dataset.topicId;
        if (!id) return;
        if (state.selectedTopics.has(id)) {
          state.selectedTopics.delete(id);
          chip.classList.remove("selected");
        } else {
          state.selectedTopics.add(id);
          chip.classList.add("selected");
        }
      });

      topicsContainer.appendChild(chip);
    });

    topicsSubmitBtn.addEventListener("click", submitOnboarding);
  }

  function showOnboardingScreen() {
    const onboardingScreen = document.getElementById("onboarding-screen");
    const feedScreen = document.getElementById("feed-screen");
    const cityStep = document.getElementById("onboarding-step-city");
    const topicsStep = document.getElementById("onboarding-step-topics");

    if (feedScreen) feedScreen.classList.add("hidden");
    if (onboardingScreen) onboardingScreen.classList.remove("hidden");
    if (cityStep) cityStep.classList.remove("hidden");
    if (topicsStep) topicsStep.classList.add("hidden");
  }

  function goToTopicsStep() {
    const cityStep = document.getElementById("onboarding-step-city");
    const topicsStep = document.getElementById("onboarding-step-topics");

    if (cityStep) cityStep.classList.add("hidden");
    if (topicsStep) topicsStep.classList.remove("hidden");
  }

  async function submitOnboarding() {
    if (!state.tgId) {
      renderError(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    const payload = {
      tg_id: state.tgId,
      city: state.cityDraft || null,
      selected_topics: Array.from(state.selectedTopics)
    };

    try {
      const resp = await apiFetch("/api/profile/onboarding", {
        method: "POST",
        body: JSON.stringify(payload)
      });
      if (!resp.ok) {
        console.warn("Onboarding save failed:", resp.status);
      }
      state.onboardingDone = true;
      showFeedScreen();
      await loadMoreFeed(true);
      attachSwipeListeners();
    } catch (err) {
      console.error("submitOnboarding error:", err);
      renderError(
        "Не удалось сохранить настройки профиля. Попробуй закрыть и снова открыть WebApp."
      );
    }
  }

  // ==========
  // ПРОФИЛЬ: загрузка состояния онбординга
  // ==========

  async function checkProfileAndStart() {
    if (!state.tgId) {
      renderError(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    try {
      const resp = await apiFetch("/api/profile?tg_id=" + String(state.tgId), {
        method: "GET"
      });
      if (!resp.ok) {
        throw new Error("Profile HTTP " + resp.status);
      }

      const data = await resp.json();
      state.onboardingDone = !!data.has_onboarding;

      if (data.city) {
        state.cityDraft = data.city;
      }

      if (Array.isArray(data.selected_topics)) {
        state.selectedTopics = new Set(data.selected_topics);
      }

      if (state.onboardingDone) {
        showFeedScreen();
        await loadMoreFeed(true);
        attachSwipeListeners();
      } else {
        showOnboardingScreen();
      }
    } catch (err) {
      console.error("checkProfileAndStart error:", err);
      // Если не удалось получить профиль — показываем онбординг как фолбек
      showOnboardingScreen();
    }
  }

  // ==========
  // ФИД: TikTok-стиль (одна карточка на экран)
  // ==========

  function showFeedScreen() {
    const onboardingScreen = document.getElementById("onboarding-screen");
    const feedScreen = document.getElementById("feed-screen");

    if (onboardingScreen) onboardingScreen.classList.add("hidden");
    if (feedScreen) feedScreen.classList.remove("hidden");
  }

  async function loadMoreFeed(initial) {
    if (state.loadingFeed) return;
    if (!initial && (!state.hasMore || state.nextOffset == null)) return;
    if (!state.tgId) return;

    state.loadingFeed = true;

    try {
      const params = new URLSearchParams({
        tg_id: String(state.tgId),
        limit: "20"
      });

      if (!initial && typeof state.nextOffset === "number") {
        params.set("offset", String(state.nextOffset));
      } else {
        params.set("offset", "0");
      }

      const resp = await apiFetch("/api/feed?" + params.toString(), {
        method: "GET"
      });
      if (!resp.ok) {
        throw new Error("Feed HTTP " + resp.status);
      }

      const data = await resp.json();
      const items = Array.isArray(data.items) ? data.items : [];
      const cursor = data.cursor || {};

      // Фильтруем дубликаты по id
      const newItems = items.filter(function (item) {
        const id = item.id;
        if (id == null) return true;
        if (state.seenIds.has(id)) return false;
        state.seenIds.add(id);
        return true;
      });

      if (initial) {
        state.feedItems = newItems;
        state.currentIndex = 0;
      } else {
        state.feedItems = state.feedItems.concat(newItems);
      }

      state.hasMore = !!cursor.has_more;
      state.nextOffset =
        typeof cursor.next_offset === "number" ? cursor.next_offset : null;

      if (initial) {
        renderCurrentCard();
      }
    } catch (err) {
      console.error("loadMoreFeed error:", err);
      if (initial) {
        renderError(
          "Не получилось загрузить ленту. Попробуй вернуться в WebApp через пару минут."
        );
      }
    } finally {
      state.loadingFeed = false;
    }
  }

  function renderCurrentCard() {
    const container = document.getElementById("feed-card-container");
    if (!container) return;

    const card = state.feedItems[state.currentIndex];

    if (!card) {
      container.innerHTML = `
        <div class="card">
          <div class="card-content">
            <h2 class="card-title">Пока карточек нет</h2>
            <p class="card-body">
              Я уже собираю для тебя свежий контент — загляни чуть позже.
            </p>
          </div>
        </div>
      `;
      return;
    }

    const sourceName =
      (card.meta && card.meta.source_name) || "EYYE • AI-подборка";

    const title = escapeHtml(card.title || "");
    const body = escapeHtml(card.body || "").replace(/\n/g, "<br />");

    container.innerHTML = `
      <div class="card">
        <div class="card-header">
          <span class="card-source">${escapeHtml(sourceName)}</span>
        </div>
        <div class="card-content">
          <h2 class="card-title">${title}</h2>
          <p class="card-body">${body}</p>
        </div>
        <div class="card-footer">
          <span class="card-tagline">Свайпай, чтобы увидеть следующую</span>
        </div>
      </div>
    `;
  }

  function attachSwipeListeners() {
    if (state.swipeListenersAttached) return;
    const container = document.getElementById("feed-card-container");
    if (!container) return;

    container.addEventListener("wheel", onWheel, { passive: true });
    container.addEventListener("touchstart", onTouchStart, { passive: true });
    container.addEventListener("touchend", onTouchEnd, { passive: true });

    state.swipeListenersAttached = true;
  }

  function onWheel(e) {
    if (state.wheelLocked) return;

    const threshold = 40;
    if (e.deltaY > threshold) {
      goToNextCard();
    } else if (e.deltaY < -threshold) {
      goToPrevCard();
    }

    state.wheelLocked = true;
    setTimeout(function () {
      state.wheelLocked = false;
    }, 350);
  }

  function onTouchStart(e) {
    if (e.touches.length !== 1) return;
    state.touchStartY = e.touches[0].clientY;
  }

  function onTouchEnd(e) {
    if (state.touchStartY == null) return;
    const endY = e.changedTouches[0].clientY;
    const deltaY = endY - state.touchStartY;

    const threshold = 40;
    if (deltaY < -threshold) {
      // свайп вверх → следующая
      goToNextCard();
    } else if (deltaY > threshold) {
      // свайп вниз → предыдущая
      goToPrevCard();
    }

    state.touchStartY = null;
  }

  function goToNextCard() {
    if (state.currentIndex < state.feedItems.length - 1) {
      state.currentIndex += 1;
      renderCurrentCard();

      const remaining = state.feedItems.length - state.currentIndex - 1;
      if (remaining < 3) {
        loadMoreFeed(false);
      }
    } else if (state.hasMore) {
      // карточки кончились, но бекенд говорит, что ещё можно подгрузить
      loadMoreFeed(false).then(function () {
        if (state.currentIndex < state.feedItems.length - 1) {
          state.currentIndex += 1;
          renderCurrentCard();
        }
      });
    }
  }

  function goToPrevCard() {
    if (state.currentIndex > 0) {
      state.currentIndex -= 1;
      renderCurrentCard();
    }
  }

  function renderError(message) {
    const onboardingScreen = document.getElementById("onboarding-screen");
    const feedScreen = document.getElementById("feed-screen");
    const container = document.getElementById("feed-card-container");

    if (onboardingScreen) onboardingScreen.classList.add("hidden");
    if (feedScreen) feedScreen.classList.remove("hidden");

    if (container) {
      container.innerHTML = `
        <div class="card">
          <div class="card-content">
            <p class="card-body">
              ${escapeHtml(message)}
            </p>
          </div>
        </div>
      `;
    }
  }

  // ==========
  // ИНИЦИАЛИЗАЦИЯ
  // ==========

  function init() {
    state.tgId = getTgId();

    if (tg) {
      try {
        tg.expand();
        tg.ready();
      } catch (e) {
        console.warn("Telegram WebApp init error", e);
      }
    }

    if (!state.tgId) {
      renderError(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    setupOnboardingUI();
    checkProfileAndStart();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
