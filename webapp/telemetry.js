// file: webapp/telemetry.js

(function () {
    const API_PATH = "/api/events";
  
    // Порог, после которого считаем, что была "view", а не просто пролистали (мс)
    const VIEW_DWELL_THRESHOLD_MS = 2500;
  
    // Разбираем tg_id из query-параметра: ?tg_id=123 (фоллбек)
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
      console.warn(
        "[EYYE Telemetry] tg_id is missing in URL (will wait for init/setTgId)."
      );
    }
  
    /** Очередь событий, которые ещё не отправлены */
    const queue = [];
    let flushTimer = null;
    const FLUSH_INTERVAL_MS = 2000; // раз в 2 секунды
    const MAX_BATCH_SIZE = 50;
  
    // Локальное состояние текущей карточки для расчёта dwell_ms
    let currentCardId = null;
    let currentPosition = null;
    let currentShownAtMs = null;
  
    function setTgIdInternal(newTgId) {
      const n = Number(newTgId);
      if (!Number.isFinite(n)) {
        console.warn("[EYYE Telemetry] Invalid tg_id passed", newTgId);
        return;
      }
      TG_ID = n;
    }
  
    function enqueue(event) {
      if (!TG_ID) {
        // Не знаем пользователя — не логируем
        return;
      }
  
      queue.push(event);
  
      // Ограничим очередь, чтобы не раздувать память
      if (queue.length > 3 * MAX_BATCH_SIZE) {
        queue.splice(0, queue.length - 3 * MAX_BATCH_SIZE);
      }
  
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
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
          keepalive: true, // на случай закрытия вкладки
        });
      } catch (e) {
        console.warn("[EYYE Telemetry] Failed to send events", e);
        // На фейле можно вернуть часть событий обратно
        const leftovers = batch.slice(-MAX_BATCH_SIZE);
        queue.unshift(...leftovers);
      }
    }
  
    // Попытка дослать события при закрытии/перезагрузке
    window.addEventListener("beforeunload", () => {
      if (!queue.length || !TG_ID) return;
  
      const payload = JSON.stringify({
        tg_id: TG_ID,
        events: queue.slice(0, MAX_BATCH_SIZE),
      });
  
      // Если есть sendBeacon — используем его
      if (navigator.sendBeacon) {
        try {
          const blob = new Blob([payload], { type: "application/json" });
          navigator.sendBeacon(API_PATH, blob);
          return;
        } catch (e) {
          console.warn("[EYYE Telemetry] sendBeacon failed", e);
        }
      }
  
      // Фоллбек — синхронный XHR (не идеален, но лучше, чем ничего)
      try {
        const xhr = new XMLHttpRequest();
        xhr.open("POST", API_PATH, false); // false = sync
        xhr.setRequestHeader("Content-Type", "application/json");
        xhr.send(payload);
      } catch (e) {
        // забиваем, вкладка всё равно закрывается
      }
    });
  
    // ========= ВСПОМОГАТЕЛЬНОЕ =========
  
    function normalizeCardId(cardId) {
      const n = Number(cardId);
      if (!Number.isFinite(n)) return null;
      return n;
    }
  
    function baseEvent(cardId, eventType, position, dwellMs, extra) {
      const cid = normalizeCardId(cardId);
      if (cid === null) return;
  
      enqueue({
        card_id: cid,
        event_type: eventType,
        dwell_ms: dwellMs == null ? null : dwellMs,
        position: typeof position === "number" ? position : null,
        source: SOURCE || "webapp",
        extra: extra || null,
      });
    }
  
    // ========= Публичный API =========
  
    const Telemetry = {
      /** Инициализация из WebApp (предпочтительный способ) */
      init(options) {
        options = options || {};
        if (options.tgId != null) {
          setTgIdInternal(options.tgId);
        }
        if (options.source) {
          SOURCE = String(options.source);
        }
      },
  
      /** Можно вызвать, если хочешь явно установить tg_id (например, из JS) */
      setTgId(newTgId) {
        setTgIdInternal(newTgId);
      },
  
      /** Простой логгер произвольного события, если захочешь что-то своё */
      logEvent(cardId, eventType, options = {}) {
        baseEvent(
          cardId,
          eventType,
          options.position,
          options.dwellMs,
          options.extra
        );
      },
  
      // ===== Специализированные сахарные методы =====
  
      view(cardId, position, dwellMs) {
        baseEvent(cardId, "view", position, dwellMs);
      },
  
      impression(cardId, position) {
        baseEvent(cardId, "impression", position, null);
      },
  
      swipeNext(cardId, position, dwellMs) {
        baseEvent(cardId, "swipe_next", position, dwellMs);
      },
  
      clickMore(cardId, position) {
        baseEvent(cardId, "click_more", position, null);
      },
  
      clickSource(cardId, position) {
        baseEvent(cardId, "click_source", position, null);
      },
  
      like(cardId, position) {
        baseEvent(cardId, "like", position, null);
      },
  
      dislike(cardId, position) {
        baseEvent(cardId, "dislike", position, null);
      },
  
      share(cardId, position) {
        baseEvent(cardId, "share", position, null);
      },
  
      // ===== Обёртки под текущий app.js =====
  
      /**
       * Вызывается при показе карточки (renderCurrentCard).
       * Логируем impression и запоминаем старт времени просмотра.
       */
      onCardShown(ctx) {
        ctx = ctx || {};
        const cardId = ctx.cardId;
        const position = ctx.position;
        const tgId = ctx.tgId;
  
        if (tgId != null) {
          setTgIdInternal(tgId);
        }
  
        const cid = normalizeCardId(cardId);
        if (cid === null) return;
  
        currentCardId = cid;
        currentPosition = typeof position === "number" ? position : null;
        currentShownAtMs = Date.now();
  
        // Лёгкий позитивный сигнал – impression
        Telemetry.impression(cid, currentPosition);
      },
  
      /**
       * Вызывается при свайпе вперёд (goToNextCard).
       * Считаем dwell и отправляем:
       *  - view (если долго читал)
       *  - swipe_next (всегда, как сигнал смены карточки)
       */
      onSwipeNext(ctx) {
        ctx = ctx || {};
        const cardId = ctx.cardId;
        const position = ctx.position;
        const tgId = ctx.tgId;
  
        if (tgId != null) {
          setTgIdInternal(tgId);
        }
  
        const cid = normalizeCardId(cardId);
        if (cid === null) return;
  
        let dwell = null;
        if (currentCardId === cid && typeof currentShownAtMs === "number") {
          dwell = Date.now() - currentShownAtMs;
          if (dwell < 0) dwell = 0;
        }
  
        const pos = typeof position === "number" ? position : null;
  
        // Если пользователь задержался на карточке — считаем это "view"
        if (dwell !== null && dwell >= VIEW_DWELL_THRESHOLD_MS) {
          Telemetry.view(cid, pos, dwell);
        }
  
        // В любом случае фиксируем swipe_next как факт скролла
        Telemetry.swipeNext(cid, pos, dwell);
  
        // Сбрасываем текущее состояние; следующая карточка вызовет onCardShown
        currentCardId = null;
        currentPosition = null;
        currentShownAtMs = null;
      },
    };
  
    // Вешаем в глобал
    window.EYYETelemetry = Telemetry;
  })();
  