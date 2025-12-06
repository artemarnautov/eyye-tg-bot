// file: webapp/app.js
(function () {
  "use strict";

  // ==== Настройки и константы ====

  // Бекенд у нас на том же домене/порте
  var API_BASE = "";

  // Доступные темы для онбординга
  var AVAILABLE_TOPICS = [
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

  // Состояние приложения
  var state = {
    tgId: null,

    // онбординг
    onboardingDone: false,
    cityDraft: "",
    selectedTopics: {}, // объект как set: {tag: true}

    // фид
    feedItems: [],
    currentIndex: 0,
    loadingFeed: false,
    hasMore: true,
    nextOffset: 0,
    seenIds: {},

    // свайпы
    wheelLocked: false,
    lastWheelTime: 0,
    touchStartY: null
  };

  // DOM элементы
  var elOnboardingScreen = null;
  var elOnboardingCityStep = null;
  var elOnboardingTopicsStep = null;
  var elCityInput = null;
  var elCityNextBtn = null;
  var elCitySkipBtn = null;
  var elTopicsContainer = null;
  var elTopicsSubmitBtn = null;
  var elFeedScreen = null;
  var elFeedCardContainer = null;

  // Telegram WebApp объект (если есть)
  var tg = null;

  // ==== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====

  function logDebug() {
    if (typeof console !== "undefined" && console.log) {
      console.log.apply(console, arguments);
    }
  }

  function $(id) {
    return document.getElementById(id);
  }

  function escapeHtml(str) {
    if (!str) return "";
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function getTgIdFromLocation() {
    var params = new URLSearchParams(window.location.search);
    var raw = params.get("tg_id");
    if (!raw) return null;
    var n = Number(raw);
    if (!isNaN(n)) return n;
    return raw;
  }

  function resolveTgId() {
    try {
      if (
        window.Telegram &&
        window.Telegram.WebApp &&
        window.Telegram.WebApp.initDataUnsafe &&
        window.Telegram.WebApp.initDataUnsafe.user &&
        window.Telegram.WebApp.initDataUnsafe.user.id
      ) {
        return window.Telegram.WebApp.initDataUnsafe.user.id;
      }
    } catch (e) {
      // ignore
    }
    return getTgIdFromLocation();
  }

  function apiFetch(path, options) {
    options = options || {};
    if (!options.headers) {
      options.headers = {};
    }
    if (!options.headers["Content-Type"]) {
      options.headers["Content-Type"] = "application/json";
    }

    return fetch(API_BASE + path, options);
  }

  function showScreen(name) {
    if (!elOnboardingScreen || !elFeedScreen) return;

    if (name === "onboarding") {
      elOnboardingScreen.classList.remove("hidden");
      elFeedScreen.classList.add("hidden");
    } else if (name === "feed") {
      elFeedScreen.classList.remove("hidden");
      elOnboardingScreen.classList.add("hidden");
    }
  }

  function showOnboardingStep(stepName) {
    if (!elOnboardingCityStep || !elOnboardingTopicsStep) return;

    if (stepName === "city") {
      elOnboardingCityStep.classList.remove("hidden");
      elOnboardingTopicsStep.classList.add("hidden");
    } else if (stepName === "topics") {
      elOnboardingTopicsStep.classList.remove("hidden");
      elOnboardingCityStep.classList.add("hidden");
    }
  }

  function showOnboardingError(message) {
    // Простой вывод ошибки как alert, чтобы точно увидеть
    alert(message);
  }

  // ==== ОНБОРДИНГ ====

  function renderTopicsChips() {
    if (!elTopicsContainer) return;
    var html = "";
    for (var i = 0; i < AVAILABLE_TOPICS.length; i++) {
      var t = AVAILABLE_TOPICS[i];
      var selected = !!state.selectedTopics[t.id];
      html +=
        '<button class="topic-chip' +
        (selected ? " selected" : "") +
        '" data-topic-id="' +
        escapeHtml(t.id) +
        '">' +
        escapeHtml(t.label) +
        "</button>";
    }
    elTopicsContainer.innerHTML = html;

    // навешиваем обработчики
    var buttons = elTopicsContainer.querySelectorAll(".topic-chip");
    for (var j = 0; j < buttons.length; j++) {
      (function (btn) {
        btn.addEventListener("click", function () {
          var tag = btn.getAttribute("data-topic-id");
          if (!tag) return;
          if (state.selectedTopics[tag]) {
            delete state.selectedTopics[tag];
            btn.classList.remove("selected");
          } else {
            state.selectedTopics[tag] = true;
            btn.classList.add("selected");
          }
        });
      })(buttons[j]);
    }
  }

  function handleCityNext() {
    if (!elCityInput) return;
    state.cityDraft = (elCityInput.value || "").trim();
    showOnboardingStep("topics");
    renderTopicsChips();
  }

  function handleCitySkip() {
    state.cityDraft = "";
    showOnboardingStep("topics");
    renderTopicsChips();
  }

  function submitOnboarding() {
    if (!state.tgId) {
      showOnboardingError(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    var tags = [];
    for (var key in state.selectedTopics) {
      if (Object.prototype.hasOwnProperty.call(state.selectedTopics, key)) {
        tags.push(key);
      }
    }

    var payload = {
      user_id: state.tgId,
      city: state.cityDraft || null,
      tags: tags
    };

    logDebug("Submitting onboarding", payload);

    apiFetch("/api/profile/onboarding", {
      method: "POST",
      body: JSON.stringify(payload)
    })
      .then(function (resp) {
        if (!resp.ok) {
          logDebug("Onboarding save HTTP error:", resp.status);
        }
        // В любом случае после онбординга пробуем открыть фид
        state.onboardingDone = true;
        showScreen("feed");
        loadFeed(true);
      })
      .catch(function (err) {
        logDebug("Onboarding save error:", err);
        state.onboardingDone = true;
        showScreen("feed");
        loadFeed(true);
      });
  }

  function loadProfileAndMaybeSkipOnboarding() {
    if (!state.tgId) {
      showOnboardingError(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    apiFetch("/api/profile?tg_id=" + encodeURIComponent(String(state.tgId)), {
      method: "GET"
    })
      .then(function (resp) {
        if (!resp.ok) {
          logDebug("GET /api/profile HTTP error:", resp.status);
          // если ошибка — просто показываем онбординг
          showScreen("onboarding");
          showOnboardingStep("city");
          return null;
        }
        return resp.json();
      })
      .then(function (data) {
        if (!data) return;
        logDebug("Profile data:", data);

        if (data.has_onboarding) {
          // онбординг уже пройден — сразу в фид
          state.onboardingDone = true;
          showScreen("feed");
          loadFeed(true);
        } else {
          showScreen("onboarding");
          showOnboardingStep("city");
        }
      })
      .catch(function (err) {
        logDebug("GET /api/profile error:", err);
        // при ошибке профиля всё равно даём онбординг
        showScreen("onboarding");
        showOnboardingStep("city");
      });
  }

  // ==== ФИД ====

  function renderEmptyFeed(message) {
    if (!elFeedCardContainer) return;
    var text =
      message ||
      "Пока для тебя нет готовых карточек. Я уже готовлю контент — загляни сюда чуть позже.";
    elFeedCardContainer.innerHTML =
      '<div class="card">' +
      '<div class="card-content">' +
      '<p class="card-body">' +
      escapeHtml(text) +
      "</p>" +
      "</div>" +
      "</div>";
  }

  function renderCurrentCard() {
    if (!elFeedCardContainer) return;

    if (!state.feedItems || state.feedItems.length === 0) {
      renderEmptyFeed();
      return;
    }

    if (state.currentIndex < 0) {
      state.currentIndex = 0;
    }
    if (state.currentIndex >= state.feedItems.length) {
      state.currentIndex = state.feedItems.length - 1;
    }

    var item = state.feedItems[state.currentIndex];
    if (!item) {
      renderEmptyFeed();
      return;
    }

    var title = escapeHtml(item.title || "");
    var body = escapeHtml(item.body || "");
    body = body.replace(/\n/g, "<br />");

    var sourceName = "";
    if (item.meta && item.meta.source_name) {
      sourceName = String(item.meta.source_name);
    } else if (item.source_type === "telegram") {
      sourceName = "Telegram";
    } else if (item.source_type === "llm") {
      sourceName = "EYYE • AI-подборка";
    }

    var html =
      '<div class="card">' +
      '<div class="card-header">' +
      '<div class="card-source">' +
      (sourceName ? "Источник: " + escapeHtml(sourceName) : "") +
      "</div>" +
      "</div>" +
      '<div class="card-content">' +
      '<h2 class="card-title">' +
      title +
      "</h2>" +
      '<p class="card-body">' +
      body +
      "</p>" +
      "</div>" +
      '<div class="card-footer">' +
      '<div class="card-tagline">Свайпай вверх/вниз, чтобы листать ленту</div>' +
      "</div>" +
      "</div>";

    elFeedCardContainer.innerHTML = html;

    // === ТЕЛЕМЕТРИЯ: карточка показана пользователю ===
    if (
      window.EYYETelemetry &&
      typeof window.EYYETelemetry.onCardShown === "function" &&
      item.id != null
    ) {
      window.EYYETelemetry.onCardShown({
        tgId: state.tgId,
        cardId: item.id,
        position: state.currentIndex
      });
    }
  }

  function appendNewItems(items) {
    if (!items || !items.length) return;

    for (var i = 0; i < items.length; i++) {
      var it = items[i];
      var id = it.id;
      if (id == null) {
        state.feedItems.push(it);
        continue;
      }
      var key = String(id);
      if (!state.seenIds[key]) {
        state.seenIds[key] = true;
        state.feedItems.push(it);
      }
    }
  }

  function loadFeed(initial) {
    if (!state.tgId) {
      renderEmptyFeed(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    if (state.loadingFeed) {
      return;
    }
    if (!initial && !state.hasMore) {
      return;
    }

    state.loadingFeed = true;

    if (initial) {
      state.feedItems = [];
      state.currentIndex = 0;
      state.hasMore = true;
      state.nextOffset = 0;
      state.seenIds = {};
    }

    var limit = 20;
    var offset = state.nextOffset;

    var url =
      "/api/feed?tg_id=" +
      encodeURIComponent(String(state.tgId)) +
      "&limit=" +
      String(limit) +
      "&offset=" +
      String(offset);

    logDebug("Loading feed:", url);

    apiFetch(url, { method: "GET" })
      .then(function (resp) {
        if (!resp.ok) {
          logDebug("GET /api/feed HTTP error:", resp.status);
          renderEmptyFeed(
            "Не получилось загрузить ленту. Попробуй открыть WebApp чуть позже."
          );
          state.loadingFeed = false;
          return null;
        }
        return resp.json();
      })
      .then(function (data) {
        if (!data) return;
        var items = data.items || [];
        var debug = data.debug || {};
        logDebug("Feed items batch:", items.length, "debug:", debug);

        appendNewItems(items);

        // Используем серверную логику пагинации, если она есть
        if (typeof debug.has_more === "boolean") {
          state.hasMore = debug.has_more;
        } else {
          // Fallback на старое поведение
          state.hasMore = items.length >= limit;
        }

        if (typeof debug.next_offset === "number") {
          state.nextOffset = debug.next_offset;
        } else {
          state.nextOffset = offset + items.length;
        }

        if (state.feedItems.length === 0) {
          // Спец-сообщение, если сервер честно сказал, что карточек нет
          if (debug.reason === "no_cards") {
            renderEmptyFeed(
              "Пока нет новостей по выбранным темам. Я обновлю ленту, как только появятся свежие карточки."
            );
          } else {
            renderEmptyFeed();
          }
        } else {
          renderCurrentCard();
        }

        state.loadingFeed = false;
      })
      .catch(function (err) {
        logDebug("GET /api/feed error:", err);
        renderEmptyFeed(
          "Не получилось загрузить ленту. Проверь интернет и попробуй ещё раз."
        );
        state.loadingFeed = false;
      });
  }

  function goToNextCard() {
    if (!state.feedItems || state.feedItems.length === 0) {
      return;
    }

    // === ТЕЛЕМЕТРИЯ: свайп вперёд по текущей карточке ===
    var prevItem =
      state.feedItems &&
      state.feedItems.length > 0 &&
      state.currentIndex >= 0 &&
      state.currentIndex < state.feedItems.length
        ? state.feedItems[state.currentIndex]
        : null;

    if (
      prevItem &&
      prevItem.id != null &&
      window.EYYETelemetry &&
      typeof window.EYYETelemetry.onSwipeNext === "function"
    ) {
      window.EYYETelemetry.onSwipeNext({
        tgId: state.tgId,
        cardId: prevItem.id,
        position: state.currentIndex
      });
    }

    if (state.currentIndex < state.feedItems.length - 1) {
      state.currentIndex += 1;
      renderCurrentCard();

      // если мы приближаемся к концу — подгружаем
      if (state.currentIndex >= state.feedItems.length - 3) {
        loadFeed(false);
      }
    } else {
      // дошли до конца текущего набора — пробуем ещё подгрузить
      loadFeed(false);
    }
  }

  function goToPrevCard() {
    if (!state.feedItems || state.feedItems.length === 0) {
      return;
    }
    if (state.currentIndex > 0) {
      state.currentIndex -= 1;
      renderCurrentCard();
    }
  }

  function handleWheel(event) {
    event = event || window.event;
    var now = Date.now();
    if (state.wheelLocked && now - state.lastWheelTime < 250) {
      return;
    }
    state.wheelLocked = true;
    state.lastWheelTime = now;
    setTimeout(function () {
      state.wheelLocked = false;
    }, 250);

    var deltaY = event.deltaY || event.wheelDeltaY || event.wheelDelta;
    if (typeof deltaY !== "number") {
      return;
    }

    if (deltaY > 0) {
      goToNextCard();
    } else if (deltaY < 0) {
      goToPrevCard();
    }
  }

  function handleTouchStart(event) {
    if (!event.touches || event.touches.length === 0) return;
    state.touchStartY = event.touches[0].clientY;
  }

  function handleTouchEnd(event) {
    if (state.touchStartY == null) return;
    if (!event.changedTouches || event.changedTouches.length === 0) return;
    var endY = event.changedTouches[0].clientY;
    var diff = state.touchStartY - endY;
    state.touchStartY = null;

    var threshold = 40;
    if (diff > threshold) {
      // свайп вверх
      goToNextCard();
    } else if (diff < -threshold) {
      // свайп вниз
      goToPrevCard();
    }
  }

  function attachSwipeHandlers() {
    if (!elFeedCardContainer) return;

    elFeedCardContainer.addEventListener("wheel", handleWheel, {
      passive: true
    });

    elFeedCardContainer.addEventListener("touchstart", handleTouchStart, {
      passive: true
    });
    elFeedCardContainer.addEventListener("touchend", handleTouchEnd, {
      passive: true
    });
  }

  // ==== ИНИЦИАЛИЗАЦИЯ ====

  function initDomRefs() {
    elOnboardingScreen = $("onboarding-screen");
    elOnboardingCityStep = $("onboarding-step-city");
    elOnboardingTopicsStep = $("onboarding-step-topics");
    elCityInput = $("city-input");
    elCityNextBtn = $("city-next-btn");
    elCitySkipBtn = $("city-skip-btn");
    elTopicsContainer = $("topics-container");
    elTopicsSubmitBtn = $("topics-submit-btn");
    elFeedScreen = $("feed-screen");
    elFeedCardContainer = $("feed-card-container");
  }

  function initEventHandlers() {
    if (elCityNextBtn) {
      elCityNextBtn.addEventListener("click", handleCityNext);
    }
    if (elCitySkipBtn) {
      elCitySkipBtn.addEventListener("click", handleCitySkip);
    }
    if (elTopicsSubmitBtn) {
      elTopicsSubmitBtn.addEventListener("click", submitOnboarding);
    }
    attachSwipeHandlers();
  }

  function initTelegramWebApp() {
    try {
      if (window.Telegram && window.Telegram.WebApp) {
        tg = window.Telegram.WebApp;
        tg.ready();
        tg.expand && tg.expand();
      }
    } catch (e) {
      logDebug("Telegram WebApp init error:", e);
    }
  }

  function init() {
    initDomRefs();
    initTelegramWebApp();

    state.tgId = resolveTgId();
    logDebug("Resolved tgId:", state.tgId);

    if (!state.tgId) {
      // Если мы вообще не знаем user id — показываем ошибку в онбординге
      if (elOnboardingScreen) {
        showScreen("onboarding");
        showOnboardingStep("city");
      }
      showOnboardingError(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    // === ТЕЛЕМЕТРИЯ: инициализация для этого пользователя ===
    if (
      window.EYYETelemetry &&
      typeof window.EYYETelemetry.init === "function"
    ) {
      window.EYYETelemetry.init({
        tgId: state.tgId,
        source: "webapp"
      });
    }

    initEventHandlers();
    // Пытаемся загрузить профиль и решить, показывать ли онбординг
    loadProfileAndMaybeSkipOnboarding();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
