// file: webapp/app.js

document.addEventListener("DOMContentLoaded", () => {
    const root = document.getElementById("app");
  
    // Пробуем достать данные пользователя из Telegram WebApp
    const tg = window.Telegram ? window.Telegram.WebApp : null;
    const user = tg?.initDataUnsafe?.user;
  
    if (tg) {
      tg.expand(); // разворачиваем WebApp на всю высоту
      tg.ready();
    }
  
    if (user) {
      const name = user.first_name || user.username || "друг";
  
      root.innerHTML = `
        <h1 class="title">EYYE feed (MVP)</h1>
        <p class="text">Привет, ${name}! 👋</p>
        <p class="text">
          Это заглушка WebApp. Чуть позже здесь появится бесконечная лента карточек,
          построенная под твой профиль.
        </p>
      `;
    } else {
      // Если открыли не из Telegram (например, просто в браузере)
      root.innerHTML = `
        <h1 class="title">EYYE feed (MVP)</h1>
        <p class="text">
          Открой эту страницу через Telegram-бот EYYE, чтобы увидеть персональную ленту.
        </p>
      `;
    }
  });
  