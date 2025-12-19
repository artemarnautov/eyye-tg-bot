// file: webapp/telemetry.js
(function () {
  "use strict";

  const API_PATH = "/api/events";

  // ====== Настройки (профессиональная телеметрия под TikTok-UX) ======
  const FLUSH_INTERVAL_MS = 2000; // как часто пытаемся отправлять пачку
  const MAX_BATCH_SIZE = 50;

  // Heartbeat: как часто обновляем dwell, пока карточка на экране
  const HEARTBEAT_MS = 5000;

  // Минимальный прирост dwell, чтобы отправлять очередной view (анти-спам)
  const VIEW_SEND_STEP_MS = 1200;

  // Жёсткий кламп (защита от вкладки в фоне / багов)
  const MAX_DWELL_MS = 120000;

  // Фикс: открытие списка источников — сильный сигнал.
  // Перед этим иногда полезно "зафиксировать" view (вес сигнала на бэке).
  const SEND_VIEW_BEFORE_SOURCES_OPEN = true;

  // ====== session meta ======
  const SESSION_ID = (function makeSessionId() {
    try {
      // короткий, но уникальный для текущей вкладки
      return (
        Date.now().toString(36) +
        "-" +
        Math.random().toString(36).slice(2, 10)
      );
    } catch (e) {
      return String(Date.now());
    }
  })();

  // ====== tg_id / source ======

  function getTgIdFromUrl() {
    try {
      const params = new URLSearchParams(window.location.search);
      const raw = params.get("tg_id");
      if (!raw) return null;
      const num = Number(raw);
      return Number.isFinite(num) ? num : null;
    } catch (e) {
      console.warn("[EYYE Telemetry] Failed to parse tg_id from URL", e);
      return null;
    }
  }

  let TG_ID = getTgIdFromUrl();
  let SOURCE = "webapp";

  if (!TG_ID) {
    console.warn("[EYYE Telemetry] tg_id is missing (will wait for init/setTgId).");
  }

  // ====== Очередь/отправка ======

  const queue = [];
  let flushTimer = null;

  function nowIso() {
    return new Date().toISOString();
  }

  function clampInt(v, lo, hi) {
    v = Math.round(Number(v) || 0);
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
  }

  function setTgIdInternal(newTgId) {
    const n = Number(newTgId);
    if (!Number.isFinite(n)) {
      console.warn("[EYYE Telemetry] Invalid tg_id passed", newTgId);
      return;
    }
    TG_ID = n;
  }

  // Coalesce: если в очереди уже есть view по этой карточке — обновляем dwell, не создаём новую запись.
  function enqueue(event) {
    if (!TG_ID) return;

    // Ограничим память
    if (queue.length > 3 * MAX_BATCH_SIZE) {
      queue.splice(0, queue.length - 3 * MAX_BATCH_SIZE);
    }

    if (event && event.type === "view" && Number.isFinite(event.card_id)) {
      const cid = event.card_id;

      // ищем с конца ближайший view по этой карточке
      for (let i = queue.length - 1; i >= 0; i--) {
        const ev = queue[i];
        if (ev && ev.type === "view" && ev.card_id === cid) {
          // max dwell + обновим ts/position/source/extra (если есть)
          const prev = Number(ev.dwell_ms || 0);
          const cur = Number(event.dwell_ms || 0);
          if (cur > prev) ev.dwell_ms = cur;
          if (event.ts) ev.ts = event.ts;
          if (event.position != null) ev.position = event.position;
          if (event.source) ev.source = event.source;
          if (event.extra != null) ev.extra = event.extra;
          scheduleFlush();
          return;
        }
      }
    }

    queue.push(event);
    scheduleFlush();
  }

  function scheduleFlush() {
    if (flushTimer !== null) return;
    flushTimer = window.setTimeout(flush, FLUSH_INTERVAL_MS);
  }

  async function flush() {
    if (!queue.length || !TG_ID) {
      flushTimer = null;
      return;
    }

    const batch = queue.splice(0, MAX_BATCH_SIZE);
    flushTimer = null;

    const payload = {
      tg_id: TG_ID,
      events: batch,
    };

    try {
      await fetch(API_PATH, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        keepalive: true,
      });
    } catch (e) {
      console.warn("[EYYE Telemetry] Failed to send events", e);
      // вернём события обратно (best-effort)
      queue.unshift(...batch.slice(-MAX_BATCH_SIZE));
    }
  }

  // Надёжнее beforeunload: pagehide + visibilitychange
  function flushBeaconBestEffort() {
    if (!queue.length || !TG_ID) return;

    const payload = JSON.stringify({
      tg_id: TG_ID,
      events: queue.slice(0, MAX_BATCH_SIZE),
    });

    if (navigator.sendBeacon) {
      try {
        const blob = new Blob([payload], { type: "application/json" });
        navigator.sendBeacon(API_PATH, blob);
        return;
      } catch (e) {
        // fallthrough
      }
    }

    // фоллбек: sync XHR
    try {
      const xhr = new XMLHttpRequest();
      xhr.open("POST", API_PATH, false);
      xhr.setRequestHeader("Content-Type", "application/json");
      xhr.send(payload);
    } catch (e) {
      // ignore
    }
  }

  window.addEventListener("pagehide", () => {
    finalizeCurrentCard("pagehide", { flushNow: true });
    flushBeaconBestEffort();
  });

  window.addEventListener("beforeunload", () => {
    finalizeCurrentCard("beforeunload", { flushNow: true });
    flushBeaconBestEffort();
  });

  // ====== Канонизация типов ======

  function normalizeCardId(cardId) {
    const n = Number(cardId);
    if (!Number.isFinite(n)) return null;
    return n;
  }

  function mapEventTypeForBackend(eventType) {
    switch (eventType) {
      case "view":
        return "view";
      case "like":
        return "like";
      case "dislike":
        return "dislike";

      // ВАЖНО:
      // В текущей схеме бэка есть только open_source.
      // Мы используем open_source как “opened sources list” (а не клик по ссылке).
      case "sources_open":
      case "open_source":
      case "click_source": // legacy
        return "open_source";

      default:
        return null;
    }
  }

  function baseEvent(cardId, eventType, position, dwellMs, extra) {
    const cid = normalizeCardId(cardId);
    if (cid === null) return;

    const backendType = mapEventTypeForBackend(eventType);
    if (!backendType) return;

    enqueue({
      type: backendType,
      card_id: cid,
      ts: nowIso(),
      dwell_ms: dwellMs == null ? null : clampInt(dwellMs, 0, MAX_DWELL_MS),

      // поля ниже бэк может игнорировать (если модель не принимает extra),
      // но мы их держим для будущего расширения:
      position: typeof position === "number" ? position : null,
      source: SOURCE || "webapp",
      extra: extra || null,
      session_id: SESSION_ID,
    });
  }

  // ====== TikTok-like dwell engine ======
  // Считаем только "видимое" время на карточке: пауза на hidden, финализация на swipe / hide / уход.
  let currentCardId = null;
  let currentPosition = null;

  // (опционально) мета карточки — пригодится позже для нормализации “скорости чтения”
  // если начнёшь передавать, например, word_count из app.js.
  let currentCardMeta = null;

  let isVisible = document.visibilityState === "visible";
  let lastVisStartPerf = null; // performance.now() когда карточка стала видимой
  let accumulatedVisibleMs = 0;

  let heartbeatTimer = null;
  let lastSentDwellMs = 0;

  function perfNow() {
    return (window.performance && performance.now) ? performance.now() : Date.now();
  }

  function startVisibleWindow() {
    if (currentCardId == null) return;
    if (!isVisible) return;
    if (lastVisStartPerf != null) return;
    lastVisStartPerf = perfNow();
  }

  function stopVisibleWindow() {
    if (currentCardId == null) return;
    if (lastVisStartPerf == null) return;
    const delta = perfNow() - lastVisStartPerf;
    if (delta > 0) accumulatedVisibleMs += delta;
    lastVisStartPerf = null;
  }

  function getCurrentDwellMs() {
    if (currentCardId == null) return 0;
    let ms = accumulatedVisibleMs;
    if (isVisible && lastVisStartPerf != null) {
      ms += (perfNow() - lastVisStartPerf);
    }
    return clampInt(ms, 0, MAX_DWELL_MS);
  }

  function maybeSendView(reason, force) {
    if (currentCardId == null) return;

    const dwell = getCurrentDwellMs();

    // анти-спам: отправляем только если dwell вырос заметно
    if (!force && dwell < lastSentDwellMs + VIEW_SEND_STEP_MS) return;

    lastSentDwellMs = dwell;

    baseEvent(currentCardId, "view", currentPosition, dwell, {
      reason: reason || "heartbeat",
      visible: !!isVisible,
      meta: currentCardMeta || null,
    });
  }

  function startHeartbeat() {
    stopHeartbeat();
    heartbeatTimer = window.setInterval(() => {
      if (currentCardId == null) return;
      if (!isVisible) return;
      maybeSendView("heartbeat", false);
    }, HEARTBEAT_MS);
  }

  function stopHeartbeat() {
    if (heartbeatTimer != null) {
      window.clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  }

  function finalizeCurrentCard(reason, opts) {
    opts = opts || {};
    if (currentCardId == null) return;

    stopVisibleWindow();
    maybeSendView(reason || "finalize", true);

    if (opts.flushNow) {
      flushBeaconBestEffort();
    }

    // сброс состояния
    currentCardId = null;
    currentPosition = null;
    currentCardMeta = null;
    accumulatedVisibleMs = 0;
    lastVisStartPerf = null;
    lastSentDwellMs = 0;
    stopHeartbeat();
  }

  document.addEventListener("visibilitychange", () => {
    const nowVisible = document.visibilityState === "visible";
    if (nowVisible === isVisible) return;
    isVisible = nowVisible;

    if (isVisible) {
      startVisibleWindow();
    } else {
      stopVisibleWindow();
      maybeSendView("visibility_hidden", true);
      flushBeaconBestEffort();
    }
  });

  // ====== Событие: открытие/закрытие списка источников ======
  // По твоей новой UX-логике:
  // - источники не кликабельны
  // - есть только toggle (open/close) по нажатию на поле “Sources”
  // Мы логируем strong signal только на OPEN (по умолчанию), close — опционально.
  function logSourcesToggle(cardId, position, opened, sourcesCount) {
    if (currentCardId == null) return;
    const cid = normalizeCardId(cardId);
    if (cid == null) return;

    // Если открыли список источников на текущей карточке —
    // можно зафиксировать view прямо перед этим (чтобы не потерять dwell).
    if (opened && SEND_VIEW_BEFORE_SOURCES_OPEN && cid === Number(currentCardId)) {
      stopVisibleWindow();
      maybeSendView("pre_sources_open", true);
      // продолжим считать дальше
      if (document.visibilityState === "visible") {
        isVisible = true;
        startVisibleWindow();
      }
    }

    // ЛОГИРУЕМ ТОЛЬКО OPEN (чтобы не зашумлять события)
    if (opened) {
      baseEvent(cid, "sources_open", position, null, {
        reason: "sources_list_open",
        sources_count: Number.isFinite(Number(sourcesCount)) ? Number(sourcesCount) : null,
      });
    } else {
      // Если захочешь логировать close — раскомментируй:
      // baseEvent(cid, "sources_open", position, null, { reason: "sources_list_close" });
    }
  }

  // ====== Публичный API ======

  const Telemetry = {
    init(options) {
      options = options || {};
      if (options.tgId != null) setTgIdInternal(options.tgId);
      if (options.source) SOURCE = String(options.source);
    },

    setTgId(newTgId) {
      setTgIdInternal(newTgId);
    },

    logEvent(cardId, eventType, options = {}) {
      baseEvent(cardId, eventType, options.position, options.dwellMs, options.extra);
    },

    view(cardId, position, dwellMs) {
      baseEvent(cardId, "view", position, dwellMs);
    },

    // ====== NEW: toggle источников (под новую UX-логику) ======
    sourcesToggle(cardId, position, opened, sourcesCount) {
      logSourcesToggle(cardId, position, !!opened, sourcesCount);
    },

    // ====== LEGACY: раньше это был “клик по источнику”.
    // Теперь трактуем как “открыли список источников”, чтобы ничего не сломалось.
    clickSource(cardId, position) {
      logSourcesToggle(cardId, position, true, null);
    },

    like(cardId, position) {
      baseEvent(cardId, "like", position, null, null);
    },

    dislike(cardId, position) {
      baseEvent(cardId, "dislike", position, null, null);
    },

    // ====== Интеграция с app.js ======

    /**
     * Вызывается при показе карточки (renderCurrentCard).
     * Стартуем “watch session”: считаем только видимое время.
     *
     * Можно расширять ctx:
     * - ctx.wordCount / ctx.textLen / ctx.lang (для нормализации скорости чтения)
     */
    onCardShown(ctx) {
      ctx = ctx || {};
      const tgId = ctx.tgId;
      if (tgId != null) setTgIdInternal(tgId);

      const cid = normalizeCardId(ctx.cardId);
      if (cid === null) return;

      // если предыдущая карточка не была финализирована — финализируем
      if (currentCardId != null && currentCardId !== cid) {
        finalizeCurrentCard("switch_card", { flushNow: false });
      }

      currentCardId = cid;
      currentPosition = typeof ctx.position === "number" ? ctx.position : null;

      // мета карточки (опционально, для будущей “скорости чтения”)
      currentCardMeta = null;
      if (ctx.wordCount != null || ctx.textLen != null || ctx.lang) {
        currentCardMeta = {
          word_count: ctx.wordCount != null ? Number(ctx.wordCount) : null,
          text_len: ctx.textLen != null ? Number(ctx.textLen) : null,
          lang: ctx.lang ? String(ctx.lang) : null,
        };
      }

      accumulatedVisibleMs = 0;
      lastSentDwellMs = 0;
      lastVisStartPerf = null;

      if (document.visibilityState === "visible") {
        isVisible = true;
        startVisibleWindow();
      } else {
        isVisible = false;
      }

      startHeartbeat();
    },

    /**
     * Вызывается при свайпе вперёд (goToNextCard).
     * Финализируем dwell и отправляем финальный view (force).
     */
    onSwipeNext(ctx) {
      ctx = ctx || {};
      const tgId = ctx.tgId;
      if (tgId != null) setTgIdInternal(tgId);

      const cid = normalizeCardId(ctx.cardId);
      if (cid === null) return;

      if (currentCardId == null) return;

      finalizeCurrentCard("swipe_next", { flushNow: false });
    },

    // опционально на будущее, если добавишь телеметрию назад:
    onSwipePrev(ctx) {
      // можешь включить позже (сейчас app.js это не вызывает)
      // finalizeCurrentCard("swipe_prev", { flushNow: false });
    },
  };

  window.EYYETelemetry = Telemetry;
})();
