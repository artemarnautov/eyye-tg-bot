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
    { id: "city", label: "Город / Локальные новости" }
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

    // ✅ NEW: режим пагинации и курсор
    cursorMode: "cursor", // "cursor" | "offset"
    nextCursor: null,     // string | null

    // fallback (если сервер вернёт offset-mode)
    nextOffset: 0,

    // dedup на фронте
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
    // ==== Sources sheet (UI) ====

    var sourcesSheetEl = null;
    var sourcesSheetVisible = false;
    var lastSourcesOpenAtMs = 0;
  
    function uniqueStrings(arr) {
      var out = [];
      var seen = {};
      for (var i = 0; i < (arr || []).length; i++) {
        var s = String(arr[i] || "").trim();
        if (!s) continue;
        if (seen[s]) continue;
        seen[s] = true;
        out.push(s);
      }
      return out;
    }
  
    function guessSourceNameFromUrl(url) {
      try {
        var u = new URL(url);
        var host = u.hostname || "";
        host = host.replace(/^www\./, "");
        return host || "Источник";
      } catch (e) {
        return "Источник";
      }
    }
  
    function extractSourcesForItem(item, fallbackSourceName) {
      // Мы намеренно НЕ показываем ссылки. Только названия.
      // Поддерживаем несколько форматов meta, чтобы не зависеть от структуры.
      var sources = [];
  
      if (item && item.meta) {
        // ожидаемые варианты
        if (Array.isArray(item.meta.sources)) {
          sources = sources.concat(item.meta.sources);
        }
        if (Array.isArray(item.meta.source_names)) {
          sources = sources.concat(item.meta.source_names);
        }
        if (Array.isArray(item.meta.supporting_sources)) {
          // supporting_sources может быть массивом строк или объектов
          for (var i = 0; i < item.meta.supporting_sources.length; i++) {
            var x = item.meta.supporting_sources[i];
            if (typeof x === "string") sources.push(x);
            else if (x && typeof x === "object") {
              if (x.name) sources.push(String(x.name));
              else if (x.title) sources.push(String(x.title));
              else if (x.source_name) sources.push(String(x.source_name));
            }
          }
        }
      }
  
      // если ничего нет — хотя бы основной источник карточки
      if ((!sources || sources.length === 0) && fallbackSourceName) {
        sources = [fallbackSourceName];
      }
  
      // если и его нет, но есть source_ref — достанем домен (без клика)
      if ((!sources || sources.length === 0) && item && item.source_ref) {
        sources = [guessSourceNameFromUrl(String(item.source_ref))];
      }
  
      return uniqueStrings(sources);
    }
  
    function ensureSourcesSheet() {
      if (sourcesSheetEl) return sourcesSheetEl;
  
      var el = document.createElement("div");
      el.setAttribute("id", "sources-sheet");
      el.style.position = "fixed";
      el.style.right = "14px";
      el.style.bottom = "14px";
      el.style.width = "min(340px, 86vw)";
      el.style.maxHeight = "46vh";
      el.style.overflow = "hidden";
      el.style.zIndex = "9999";
      el.style.borderRadius = "14px";
      el.style.boxShadow = "0 12px 30px rgba(0,0,0,0.35)";
      el.style.border = "1px solid rgba(255,255,255,0.10)";
      el.style.background = "rgba(18,18,18,0.92)";
      el.style.backdropFilter = "blur(10px)";
      el.style.color = "rgba(255,255,255,0.92)";
      el.style.fontFamily = "system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif";
      el.style.display = "none";
  
      el.innerHTML =
      '<div style="display:flex;align-items:center;justify-content:space-between;padding:10px 12px;border-bottom:1px solid rgba(255,255,255,0.10)">' +
      '<div style="font-weight:600;font-size:14px;letter-spacing:0.2px">Источники</div>' +
      '<div style="opacity:0.55;font-size:12px">нажми Sources ещё раз, чтобы закрыть</div>' +
      "</div>" +
      '<div id="sources-sheet-body" style="padding:10px 12px;overflow:auto;max-height:calc(46vh - 44px)"></div>';

  
      document.body.appendChild(el);
  
      // закрытие по кнопке
      var btnClose = el.querySelector("#sources-sheet-close");
      if (btnClose) {
        btnClose.addEventListener("click", function (e) {
          e.preventDefault();
          e.stopPropagation();
          hideSourcesSheet();
        });
      }
  
      // закрытие по клику вне (легкий вариант)
      document.addEventListener("click", function (e) {
        if (!sourcesSheetVisible) return;
        if (!sourcesSheetEl) return;
        // если кликнули внутри — не закрываем
        if (sourcesSheetEl.contains(e.target)) return;
        hideSourcesSheet();
      });
  
      // чтобы свайпы/скролл внутри работали и не “пробивали” фон
      
  
      sourcesSheetEl = el;
      return sourcesSheetEl;
    }
  
    function showSourcesSheet(sourceNames) {
      var el = ensureSourcesSheet();
      var body = el.querySelector("#sources-sheet-body");
      if (!body) return;
  
      var list = sourceNames || [];
      if (!list.length) {
        body.innerHTML =
          '<div style="opacity:0.75;font-size:13px;line-height:1.4">Источники для этой карточки не указаны.</div>';
      } else {
        var html = "";
        for (var i = 0; i < list.length; i++) {
          html +=
            '<div style="padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.08)">' +
            '<div style="font-size:13px;line-height:1.35;opacity:0.92;user-select:text;pointer-events:none">' +
            escapeHtml(list[i]) +
            "</div>" +
            "</div>";
        }
        // убираем последнюю границу визуально
        body.innerHTML = html.replace(/border-bottom[^"]+"$/, "");
      }
  
      el.style.display = "block";
      sourcesSheetVisible = true;
    }
  
    function hideSourcesSheet() {
      if (!sourcesSheetEl) return;
      sourcesSheetEl.style.display = "none";
      sourcesSheetVisible = false;
    }
  
    function toggleSourcesSheet(sourceNames) {
      if (sourcesSheetVisible) hideSourcesSheet();
      else showSourcesSheet(sourceNames);
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
    var sourcesList = extractSourcesForItem(item, sourceName);
    var hasSources = sourcesList && sourcesList.length > 0;


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
    '<div style="display:flex;gap:10px;align-items:center;justify-content:flex-end;margin-bottom:10px;">' +
    (hasSources
      ? '<button id="btn-sources" type="button" style="appearance:none;border:0;border-radius:12px;padding:10px 12px;font-weight:600;font-size:13px;cursor:pointer;background:rgba(255,255,255,0.10);color:rgba(255,255,255,0.92)">Sources</button>'
      : "") +
    "</div>" +
    '<div class="card-tagline">Свайпай вверх/вниз, чтобы листать ленту</div>' +
    "</div>" +
    "</div>";


    elFeedCardContainer.innerHTML = html;
        // закрываем sources sheet при перерисовке карточки (чтобы не висел от прошлой)
        hideSourcesSheet();

        if (hasSources) {
          var btnSources = elFeedCardContainer.querySelector("#btn-sources");
          if (btnSources) {
            btnSources.addEventListener("click", function (e) {
              e.preventDefault();
              e.stopPropagation();
    
              // анти-даблклик: не спамим событиями
              var nowMs = Date.now();
              if (nowMs - lastSourcesOpenAtMs < 350) return;
              lastSourcesOpenAtMs = nowMs;
    
              // ✅ телеметрия: "пошёл смотреть источники" = сильный сигнал интереса
              if (window.EYYETelemetry && typeof window.EYYETelemetry.clickSource === "function") {
                window.EYYETelemetry.clickSource(item.id, state.currentIndex);
              }
    
              // ✅ показываем аккуратный список названий (НЕ кликабельно)
              toggleSourcesSheet(sourcesList);
            });
          }
        }
    

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

  // ✅ REPLACE THIS FUNCTION COMPLETELY
  function loadFeed(initial, opts) {
    opts = opts || {};

    var force = opts.force === true;                 // разрешаем загрузку даже если hasMore=false
    var resetCursor = opts.resetCursor === true;     // ✅ для cursor-mode: начать заново без курсора
    var forceOffsetZero = opts.forceOffsetZero === true; // fallback: refresh offset=0
    var onDone = typeof opts.onDone === "function" ? opts.onDone : null;

    if (!state.tgId) {
      renderEmptyFeed(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    if (state.loadingFeed) return;

    if (!initial && !state.hasMore && !force) return;

    state.loadingFeed = true;

    if (initial) {
      state.feedItems = [];
      state.currentIndex = 0;
      state.hasMore = true;

      // ✅ сбрасываем обе пагинации
      state.nextOffset = 0;
      state.nextCursor = null;

      // важно: в initial можно сбрасывать dedup, т.к. это новая сессия экрана
      state.seenIds = {};
    }

    var limit = 20;

    // ---- строим URL в зависимости от режима ----
    var url = "/api/feed?tg_id=" + encodeURIComponent(String(state.tgId)) + "&limit=" + String(limit);

    // cursor-mode (по умолчанию)
    if (state.cursorMode === "cursor") {
      // offset всё равно передаём как 0, чтобы backend был доволен любым валидатором
      url += "&offset=0";

      if (!resetCursor && state.nextCursor) {
        url += "&cursor=" + encodeURIComponent(String(state.nextCursor));
      }
    } else {
      // offset-mode fallback
      var offset = forceOffsetZero ? 0 : state.nextOffset;
      url += "&offset=" + String(offset);
    }

    logDebug("Loading feed:", url, "opts:", opts, "cursorMode:", state.cursorMode, "nextCursor:", state.nextCursor, "nextOffset:", state.nextOffset);

    apiFetch(url, { method: "GET" })
      .then(function (resp) {
        if (!resp.ok) {
          logDebug("GET /api/feed HTTP error:", resp.status);
          renderEmptyFeed("Не получилось загрузить ленту. Попробуй открыть WebApp чуть позже.");
          state.loadingFeed = false;
          if (onDone) onDone(0, [], null);
          return null;
        }
        return resp.json();
      })
      .then(function (data) {
        if (!data) return;

        var items = data.items || [];
        var debug = data.debug || {};
        var cursor = data.cursor || {};

        logDebug("Feed batch:", items.length, "cursor:", cursor, "debug:", debug);

        var beforeLen = state.feedItems.length;
        appendNewItems(items);
        var afterLen = state.feedItems.length;
        var addedCount = afterLen - beforeLen;

        // --- has_more ---
        if (typeof cursor.has_more === "boolean") {
          state.hasMore = cursor.has_more;
        } else if (typeof debug.has_more === "boolean") {
          state.hasMore = debug.has_more;
        } else {
          state.hasMore = items.length >= limit;
        }

        // --- режим пагинации и "следующая страница" ---
        // Приоритет: cursor.mode -> наличие next_cursor -> наличие next_offset
        if (cursor && cursor.mode === "cursor") {
          state.cursorMode = "cursor";
          state.nextCursor = cursor.next_cursor ? String(cursor.next_cursor) : null;

          // для отладки/совместимости
          if (typeof cursor.next_offset === "number") state.nextOffset = cursor.next_offset;
        } else if (cursor && cursor.mode === "offset") {
          state.cursorMode = "offset";
          state.nextCursor = null;

          if (typeof cursor.next_offset === "number") {
            state.nextOffset = cursor.next_offset;
          } else if (typeof debug.next_offset === "number") {
            state.nextOffset = debug.next_offset;
          } else {
            // безопаснее чем offset + items.length
            state.nextOffset = state.nextOffset + limit;
          }
        } else if (cursor && cursor.next_cursor) {
          // если сервер не прислал mode, но прислал next_cursor — считаем это cursor-mode
          state.cursorMode = "cursor";
          state.nextCursor = String(cursor.next_cursor);
        } else if (typeof cursor.next_offset === "number") {
          state.cursorMode = "offset";
          state.nextCursor = null;
          state.nextOffset = cursor.next_offset;
        } else if (typeof debug.next_offset === "number") {
          state.cursorMode = "offset";
          state.nextCursor = null;
          state.nextOffset = debug.next_offset;
        } else {
          // последний fallback: эвристика
          if (state.cursorMode === "offset") {
            state.nextOffset = state.nextOffset + limit;
          }
        }

        if (state.feedItems.length === 0) {
          if (debug.reason === "no_cards") {
            renderEmptyFeed("Пока нет новостей по выбранным темам. Я обновлю ленту, как только появятся свежие карточки.");
          } else {
            renderEmptyFeed();
          }
        } else {
          if (initial) {
            renderCurrentCard();
          }
        }

        state.loadingFeed = false;

        if (onDone) onDone(addedCount, items, debug);
      })
      .catch(function (err) {
        logDebug("GET /api/feed error:", err);
        renderEmptyFeed("Не получилось загрузить ленту. Проверь интернет и попробуй ещё раз.");
        state.loadingFeed = false;
        if (onDone) onDone(0, [], null);
      });
  }

  // ✅ REPLACE THIS FUNCTION COMPLETELY
  function goToNextCard() {
    hideSourcesSheet();
    if (!state.feedItems || state.feedItems.length === 0) return;

    // telemetry: swipe next
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

    // обычный шаг вперёд
    if (state.currentIndex < state.feedItems.length - 1) {
      state.currentIndex += 1;
      renderCurrentCard();

      // предзагрузка рядом с концом
      if (state.currentIndex >= state.feedItems.length - 3) {
        loadFeed(false);
      }
      return;
    }

    // мы на последней карточке
    if (state.hasMore) {
      loadFeed(false, {
        onDone: function (addedCount) {
          if (addedCount > 0) {
            state.currentIndex += 1;
            renderCurrentCard();
          } else {
            // если сервер сказал has_more=true, но новых не добавили (dedup),
            // просто ждём следующего свайпа — курсор уже обновлён.
          }
        }
      });
      return;
    }

    // has_more=false: пробуем "мягко обновить", НЕ сбрасывая seenIds
    // cursor-mode: resetCursor=true (без cursor), offset-mode: forceOffsetZero=true
    if (state.cursorMode === "cursor") {
      loadFeed(false, {
        force: true,
        resetCursor: true,
        onDone: function (addedCount) {
          if (addedCount > 0) {
            state.currentIndex += 1;
            renderCurrentCard();
          }
        }
      });
    } else {
      loadFeed(false, {
        force: true,
        forceOffsetZero: true,
        onDone: function (addedCount) {
          if (addedCount > 0) {
            state.currentIndex += 1;
            renderCurrentCard();
          }
        }
      });
    }
  }

  function goToPrevCard() {
    hideSourcesSheet();
    if (!state.feedItems || state.feedItems.length === 0) return;
    if (state.currentIndex > 0) {
      state.currentIndex -= 1;
      renderCurrentCard();
    }
  }

  function handleWheel(event) {
    event = event || window.event;
    var now = Date.now();
    if (state.wheelLocked && now - state.lastWheelTime < 250) return;

    state.wheelLocked = true;
    state.lastWheelTime = now;
    setTimeout(function () {
      state.wheelLocked = false;
    }, 250);

    var deltaY = event.deltaY || event.wheelDeltaY || event.wheelDelta;
    if (typeof deltaY !== "number") return;

    if (deltaY > 0) goToNextCard();
    else if (deltaY < 0) goToPrevCard();
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
    if (diff > threshold) goToNextCard();
    else if (diff < -threshold) goToPrevCard();
  }

  function attachSwipeHandlers() {
    if (!elFeedCardContainer) return;

    elFeedCardContainer.addEventListener("wheel", handleWheel, { passive: true });
    elFeedCardContainer.addEventListener("touchstart", handleTouchStart, { passive: true });
    elFeedCardContainer.addEventListener("touchend", handleTouchEnd, { passive: true });
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
    if (elCityNextBtn) elCityNextBtn.addEventListener("click", handleCityNext);
    if (elCitySkipBtn) elCitySkipBtn.addEventListener("click", handleCitySkip);
    if (elTopicsSubmitBtn) elTopicsSubmitBtn.addEventListener("click", submitOnboarding);
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
      if (elOnboardingScreen) {
        showScreen("onboarding");
        showOnboardingStep("city");
      }
      showOnboardingError(
        "Не удалось получить твой Telegram ID. Открой WebApp через кнопку в боте и попробуй снова."
      );
      return;
    }

    if (window.EYYETelemetry && typeof window.EYYETelemetry.init === "function") {
      window.EYYETelemetry.init({
        tgId: state.tgId,
        source: "webapp"
      });
    }

    initEventHandlers();
    loadProfileAndMaybeSkipOnboarding();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
