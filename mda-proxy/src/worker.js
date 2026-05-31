// ──────────────────────────────────────────────────────────────────────────
// Cloudflare Worker — каждые 4 часа тянет расписание сдачи крови MDA
// и хранит ЕДИНСТВЕННУЮ пару key/value в KV:
//   key:   "mda"
//   value: строка JSON вида `[{"DateDonation":"2025-08-17T00:00:00", ...}, ...]`
//
// HTTP-интерфейс:
//   • GET /              — отдаёт содержимое ключа "mda" (или [] если пусто)
//   • GET /?date=latest  — то же самое (для обратной совместимости)
//
// Дополнительно:
//   • GET /refresh       — ручной запуск обновления (удобно для проверки)
//   • Обрабатывает CORS (OPTIONS) и не использует cacheTtl=0.
// ──────────────────────────────────────────────────────────────────────────

const ENDPOINT = "https://www.mdais.org/umbraco/api/invoker/execute";
const SINGLE_KEY = "mda"; // единственный ключ в KV

const PAYLOAD = {
  RequestHeader: {
    Application: 101,
    Module:      "BloodBank",
    Function:    "GetAllDetailsDonations",
    Token:       ""
  },
  RequestData: ""
};

/** Выполнить запрос к MDA и перезаписать ключ SINGLE_KEY в KV */
async function refreshKV(env) {
  const resp = await fetch(ENDPOINT, {
    method:  "POST",
    headers: {
      "Content-Type": "application/json",
      "Referer":      "https://www.mdais.org/blood-donation",
      "User-Agent":   "Mozilla/5.0 (compatible; MDA-Pipeline/1.0)",
      "Accept":       "application/json, text/plain, */*"
    },
    body: JSON.stringify(PAYLOAD)
  });

  if (!resp.ok) {
    throw new Error(`MDA API HTTP ${resp.status}`);
  }

  const payload = await resp.json();

  // У API поле Result — это СТРОКА с JSON-массивом.
  // Валидируем, что это действительно массив, и кладём исходную строку в KV.
  let resultStr = "";
  if (payload && typeof payload.Result === "string") {
    // Проверим, что парсится в массив (чтобы не хранить мусор)
    const parsed = JSON.parse(payload.Result);
    if (!Array.isArray(parsed)) {
      throw new Error("Unexpected MDA API format: Result is not an array");
    }
    resultStr = payload.Result;
  } else {
    throw new Error("Unexpected MDA API payload: missing Result string");
  }

  // Перезапишем единственный ключ. TTL не ставим, чтобы ключ не «протухал».
  await env.MDA_DATA.put(SINGLE_KEY, resultStr);
}

function jsonResponse(body, status = 200) {
  return new Response(body, {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "no-store, no-cache, must-revalidate"
    }
  });
}

export default {
  /* ───────── 1) Периодическое обновление по cron (каждые 4 часа) ───────── */
  async scheduled(_event, env) {
    await refreshKV(env);
  },

  /* ───────── 2) HTTP-интерфейс ───────── */
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, OPTIONS",
          "Access-Control-Allow-Headers": "*",
          "Access-Control-Max-Age": "86400"
        }
      });
    }

    // Ручной запуск обновления
    if (url.pathname === "/refresh") {
      try {
        await refreshKV(env);
        return new Response("OK", {
          headers: { "Access-Control-Allow-Origin": "*" }
        });
      } catch (e) {
        return new Response(String(e?.message || e), {
          status: 500,
          headers: { "Access-Control-Allow-Origin": "*" }
        });
      }
    }

    // Единая точка чтения: и /, и /?date=latest возвращают один и тот же ключ
    // (любые другие значения date игнорируем и отдаём тот же снимок)
    const data = await env.MDA_DATA.get(SINGLE_KEY);
    // Если ключа нет — вернём пустой массив (валидный JSON)
    return jsonResponse(data ?? "[]");
  }
};
