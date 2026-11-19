(() => {
  const state = {
    venues: [],
    events: [],
    selectedEventId: null,
    seats: [],
    selectedSeat: null,
    bookings: [],
    paymentsByBooking: new Map(),
    ticketsByBooking: new Map(),
    notifications: [],
    cancelBookingId: null,
  };

  const $ = (id) => document.getElementById(id);

  function el(tag, className, text) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined && text !== null) node.textContent = String(text);
    return node;
  }

  function createStatusIcon(type) {
    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("width", "16");
    svg.setAttribute("height", "16");
    svg.setAttribute("viewBox", "0 0 24 24");
    svg.setAttribute("fill", "none");
    svg.setAttribute("stroke", "currentColor");
    svg.setAttribute("stroke-width", "2");
    svg.setAttribute("stroke-linecap", "round");
    svg.setAttribute("stroke-linejoin", "round");
    svg.setAttribute("aria-hidden", "true");
    svg.style.flexShrink = "0";
    svg.style.marginTop = "2px";

    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    if (type === "ok") {
      svg.style.color = "var(--ok)";
      path.setAttribute("d", "M20 6 9 17l-5-5");
    } else if (type === "err") {
      svg.style.color = "var(--err)";
      path.setAttribute("d", "M12 9v4m0 4h.01M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0Z");
    } else {
      svg.style.color = "var(--accent)";
      path.setAttribute("d", "M12 16v-4m0-4h.01M22 12a10 10 0 1 1-20 0 10 10 0 0 1 20 0Z");
    }
    svg.appendChild(path);
    return svg;
  }

  function showToast(message, type = "info") {
    const region = $("toast-region");
    const toast = el("div", `toast ${type === "ok" ? "toast-ok" : type === "err" ? "toast-err" : ""}`);
    toast.appendChild(createStatusIcon(type));
    toast.appendChild(el("span", "", message));
    region.appendChild(toast);
    setTimeout(() => toast.remove(), 4500);
  }

  function formatDate(iso) {
    if (!iso) return "—";
    try {
      return new Intl.DateTimeFormat("ru-RU", {
        dateStyle: "medium",
        timeStyle: "short",
      }).format(new Date(iso));
    } catch {
      return iso;
    }
  }

  function formatMoney(val) {
    if (val === undefined || val === null) return "—";
    return `${val} ₽`;
  }

  function shortId(id) {
    if (!id) return "—";
    return `${String(id).slice(0, 8)}…`;
  }

  function newIdempotencyKey() {
    if (crypto?.randomUUID) return `ui-${crypto.randomUUID()}`;
    return `ui-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;
  }

  async function apiRequest(url, options = {}) {
    const resp = await fetch(url, options);
    const text = await resp.text();
    let data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = text;
    }
    if (!resp.ok) {
      const detail =
        data && typeof data === "object" && data.detail
          ? typeof data.detail === "string"
            ? data.detail
            : JSON.stringify(data.detail)
          : `HTTP ${resp.status}`;
      const err = new Error(detail);
      err.status = resp.status;
      throw err;
    }
    return data;
  }

  function statusBadge(status) {
    const s = String(status || "").toUpperCase();
    let cls = "badge-neutral";
    if (["CONFIRMED", "SUCCEEDED", "ACTIVE", "AVAILABLE", "OK", "PUBLISHED"].includes(s)) {
      cls = "badge-ok";
    } else if (["PENDING", "RESERVED"].includes(s)) {
      cls = "badge-warn";
    } else if (["CANCELLED", "FAILED", "REFUNDED"].includes(s)) {
      cls = "badge-err";
    }
    return el("span", `badge ${cls}`, s || "—");
  }

  // --- Dialog closedby="any" fallback & returnValue reset ---
  function setupDialogs() {
    document.querySelectorAll("dialog").forEach((dlg) => {
      dlg.addEventListener("close", () => {
        dlg.returnValue = "";
      });
    });

    if (!("closedBy" in HTMLDialogElement.prototype)) {
      const outside = (e, dialog) => {
        const r = dialog.getBoundingClientRect();
        return e.clientX < r.left || e.clientX > r.right || e.clientY < r.top || e.clientY > r.bottom;
      };
      document.querySelectorAll('dialog[closedby="any"]').forEach((dialog) => {
        let pressedOutside = false;
        dialog.addEventListener("pointerdown", (e) => {
          pressedOutside = outside(e, dialog);
        });
        dialog.addEventListener("pointerup", (e) => {
          if (pressedOutside && outside(e, dialog)) dialog.close();
        });
      });
    }
  }

  // --- Accessible :user-invalid + ARIA sync ---
  function setupFormValidation() {
    const linkError = (input, shown) => {
      const errorId = input.dataset.error;
      if (!errorId) return;
      const ids = (input.getAttribute("aria-describedby") || "")
        .split(/\s+/)
        .filter((id) => id && id !== errorId);
      if (shown) ids.push(errorId);
      if (ids.length) input.setAttribute("aria-describedby", ids.join(" "));
      else input.removeAttribute("aria-describedby");
    };

    const syncAria = (input) => {
      if (!input.matches?.("input, select, textarea")) return;
      const invalid = input.matches(":user-invalid") || input.classList.contains("user-invalid-fallback");
      input.setAttribute("aria-invalid", invalid ? "true" : "false");
      linkError(input, invalid);
    };

    document.querySelectorAll("input, select, textarea").forEach((input) => {
      if (!input.hasAttribute("aria-invalid")) input.setAttribute("aria-invalid", "false");
    });

    document.addEventListener("blur", (e) => syncAria(e.target), true);
    document.addEventListener("input", (e) => {
      if (e.target.hasAttribute?.("aria-invalid")) syncAria(e.target);
    });
    document.addEventListener("change", (e) => syncAria(e.target), true);
    document.addEventListener("invalid", (e) => syncAria(e.target), true);
    document.addEventListener(
      "reset",
      (e) => {
        setTimeout(() => [...e.target.elements].forEach((input) => syncAria(input)));
      },
      true
    );
  }

  // --- Tabs ---
  function switchTab(tabName) {
    document.querySelectorAll(".tab-btn").forEach((btn) => {
      const active = btn.dataset.tab === tabName;
      btn.setAttribute("aria-selected", active ? "true" : "false");
    });
    document.querySelectorAll(".panel").forEach((panel) => {
      const active = panel.id === `panel-${tabName}`;
      panel.classList.toggle("active", active);
      panel.hidden = !active;
    });
    if (tabName === "bookings") {
      loadBookingsAndRelated();
    } else if (tabName === "admin") {
      loadVenues();
    }
  }

  // --- Health ---
  async function loadHealth() {
    const container = $("health-badges");
    try {
      const data = await apiRequest("/health");
      container.replaceChildren();
      container.appendChild(statusBadge(`gateway: ${data.status}`));
      Object.entries(data.services || {}).forEach(([name, st]) => {
        container.appendChild(el("span", `badge ${st === "ok" ? "badge-ok" : "badge-err"}`, `${name}: ${st}`));
      });
    } catch (err) {
      container.replaceChildren(el("span", "badge badge-err", `Ошибка /health: ${err.message}`));
    }
  }

  // --- Venues & Events ---
  async function loadVenues() {
    try {
      state.venues = await apiRequest("/api/venues");
      renderVenues();
    } catch (err) {
      showToast(`Не удалось загрузить площадки: ${err.message}`, "err");
    }
  }

  function renderVenues() {
    const select = $("event-venue");
    const current = select.value;
    select.replaceChildren(el("option", "", "Выберите площадку…"));
    select.firstChild.value = "";
    state.venues.forEach((v) => {
      const opt = el("option", "", `${v.name} (${v.address}, ${v.capacity} мест)`);
      opt.value = v.id;
      select.appendChild(opt);
    });
    if (current) select.value = current;

    const list = $("venues-list");
    list.replaceChildren();
    if (!state.venues.length) {
      list.appendChild(el("div", "empty-box", "Площадок пока нет. Добавьте первую площадку в форме выше."));
      return;
    }
    state.venues.forEach((v) => {
      const item = el("div", "event-item");
      item.style.cursor = "default";
      item.appendChild(el("div", "event-item-title", v.name));
      item.appendChild(el("div", "event-item-meta tabular", `${v.address} • Вместимость: ${v.capacity}`));
      list.appendChild(item);
    });
  }

  async function loadEvents() {
    try {
      state.events = await apiRequest("/api/events");
      renderEvents();
      if (state.selectedEventId && state.events.some((ev) => ev.id === state.selectedEventId)) {
        await loadSeats(state.selectedEventId);
      }
    } catch (err) {
      showToast(`Не удалось загрузить мероприятия: ${err.message}`, "err");
    }
  }

  function renderEvents() {
    const list = $("events-list");
    $("events-count").textContent = String(state.events.length);
    list.replaceChildren();

    if (!state.events.length) {
      const box = el("div", "empty-box");
      box.appendChild(el("p", "", "Мероприятий пока нет."));
      const cta = el("button", "btn btn-sm btn-primary", "Создать мероприятие");
      cta.type = "button";
      cta.style.marginTop = "0.6rem";
      cta.addEventListener("click", () => switchTab("admin"));
      box.appendChild(cta);
      list.appendChild(box);
      return;
    }

    const venueMap = new Map(state.venues.map((v) => [v.id, v]));
    state.events.forEach((ev) => {
      const venue = venueMap.get(ev.venue_id);
      const btn = el("button", `event-item ${ev.id === state.selectedEventId ? "selected" : ""}`);
      btn.type = "button";
      btn.appendChild(el("div", "event-item-title", ev.title));
      const venueText = venue ? `${venue.name} • ` : "";
      btn.appendChild(el("div", "event-item-meta tabular", `${venueText}${formatDate(ev.starts_at)} • ${formatMoney(ev.base_price)}`));
      if (ev.description) {
        btn.appendChild(el("div", "event-item-meta", ev.description));
      }
      btn.addEventListener("click", () => selectEvent(ev.id));
      list.appendChild(btn);
    });
  }

  async function selectEvent(eventId) {
    state.selectedEventId = eventId;
    state.selectedSeat = null;
    resetCheckoutPanel();
    renderEvents();
    const ev = state.events.find((e) => e.id === eventId);
    if (!ev) return;

    $("selected-event-title").textContent = ev.title;
    $("selected-event-meta").textContent = `${formatDate(ev.starts_at)} • Базовая цена: ${formatMoney(ev.base_price)}`;
    $("reload-seats-btn").hidden = false;
    $("seat-map-area").hidden = false;
    await loadSeats(eventId);
  }

  async function loadSeats(eventId) {
    try {
      state.seats = await apiRequest(`/api/events/${eventId}/seats`);
      renderSeatMap();
    } catch (err) {
      showToast(`Не удалось загрузить места: ${err.message}`, "err");
    }
  }

  function renderSeatMap() {
    const container = $("seat-rows");
    container.replaceChildren();
    if (!state.seats.length) {
      container.appendChild(el("div", "empty-box", "Места для этого мероприятия не найдены."));
      return;
    }

    const byRow = new Map();
    state.seats.forEach((seat) => {
      if (!byRow.has(seat.row_num)) byRow.set(seat.row_num, []);
      byRow.get(seat.row_num).push(seat);
    });

    const sortedRows = [...byRow.keys()].sort((a, b) => a - b);
    sortedRows.forEach((rowNum) => {
      const rowDiv = el("div", "seat-row");
      rowDiv.appendChild(el("span", "row-label tabular", `Ряд ${rowNum}`));
      const seatsInRow = byRow.get(rowNum).sort((a, b) => a.seat_num - b.seat_num);

      seatsInRow.forEach((seat) => {
        const st = seat.status.toUpperCase();
        const isSelected = state.selectedSeat && state.selectedSeat.id === seat.id;
        const cls = isSelected
          ? "seat-selected"
          : st === "AVAILABLE"
          ? "seat-available"
          : st === "RESERVED"
          ? "seat-reserved"
          : "seat-sold";
        const btn = el("button", `seat-btn tabular ${cls}`, seat.seat_num);
        btn.type = "button";
        btn.disabled = st !== "AVAILABLE";
        btn.setAttribute("aria-label", `Ряд ${seat.row_num}, место ${seat.seat_num}, ${formatMoney(seat.price)}, статус ${st}`);
        rowDiv.appendChild(btn);

        if (st === "AVAILABLE") {
          btn.addEventListener("click", () => selectSeatForCheckout(seat));
        }
      });

      container.appendChild(rowDiv);
    });
  }

  // --- Inline Checkout & Live Saga Timeline ---
  function resetCheckoutPanel() {
    $("checkout-empty").hidden = false;
    $("booking-form").hidden = true;
    $("saga-tracker").hidden = true;
  }

  function selectSeatForCheckout(seat) {
    state.selectedSeat = seat;
    renderSeatMap();

    const ev = state.events.find((e) => e.id === state.selectedEventId);
    $("booking-seat-summary").textContent = `${ev ? ev.title : "Мероприятие"} • Ряд ${seat.row_num}, Место ${seat.seat_num} • ${formatMoney(seat.price)}`;
    $("booking-idem-key").value = newIdempotencyKey();
    $("booking-fail-sim").checked = false;
    $("checkout-empty").hidden = true;
    $("booking-form").hidden = false;
    $("booking-name").focus();
  }

  function renderSagaSteps(booking, payment, ticket) {
    const tracker = $("saga-tracker");
    tracker.hidden = false;
    tracker.replaceChildren();

    const makeStep = (label, statusText) => {
      const row = el("div", "saga-step");
      row.appendChild(el("span", "", label));
      row.appendChild(statusBadge(statusText));
      return row;
    };

    tracker.appendChild(makeStep(`1. Бронь (${shortId(booking.id)})`, booking.status));
    tracker.appendChild(makeStep("2. Оплата (payment-service)", payment ? payment.status : "PENDING"));
    tracker.appendChild(
      makeStep(
        ticket ? `3. Билет (${ticket.ticket_code})` : "3. Билет (ticket-service)",
        ticket ? ticket.status : booking.status === "CANCELLED" ? "CANCELLED" : "PENDING"
      )
    );
  }

  async function pollSagaStatus(booking) {
    renderSagaSteps(booking, null, null);

    for (let i = 0; i < 20; i++) {
      await new Promise((r) => setTimeout(r, 700));
      try {
        const currentBooking = await apiRequest(`/api/bookings/${booking.id}`);
        let payment = null;
        let ticket = null;
        try {
          payment = await apiRequest(`/api/payments/by-booking/${booking.id}`);
        } catch {
          // pending
        }
        try {
          ticket = await apiRequest(`/api/tickets/by-booking/${booking.id}`);
        } catch {
          // pending
        }

        renderSagaSteps(currentBooking, payment, ticket);

        if (currentBooking.status === "CONFIRMED" && (ticket || i >= 4)) {
          if (state.selectedEventId) await loadSeats(state.selectedEventId);
          showToast(`Бронь подтверждена! Билет: ${ticket ? ticket.ticket_code : "выпущен"}`, "ok");
          return;
        }
        if (currentBooking.status === "CANCELLED") {
          if (state.selectedEventId) await loadSeats(state.selectedEventId);
          showToast(`Бронь отменена: ${currentBooking.cancel_reason || "ошибка оплаты"}`, "err");
          return;
        }
      } catch {
        // continue polling
      }
    }
    if (state.selectedEventId) await loadSeats(state.selectedEventId);
  }

  // --- Bookings, Payments, Tickets, Notifications ---
  async function loadBookingsAndRelated() {
    try {
      const [bookings, payments, tickets, notifications] = await Promise.all([
        apiRequest("/api/bookings"),
        apiRequest("/api/payments"),
        apiRequest("/api/tickets"),
        apiRequest("/api/notifications"),
      ]);
      state.bookings = bookings;
      state.paymentsByBooking = new Map(payments.map((p) => [p.booking_id, p]));
      state.ticketsByBooking = new Map(tickets.map((t) => [t.booking_id, t]));
      state.notifications = notifications;

      renderBookingsTable();
      renderNotificationsTable();
    } catch (err) {
      showToast(`Ошибка загрузки данных: ${err.message}`, "err");
    }
  }

  function renderBookingsTable() {
    const tbody = $("bookings-tbody");
    const query = ($("bookings-search").value || "").trim().toLowerCase();
    const filtered = state.bookings.filter((b) => {
      if (!query) return true;
      return (
        b.id.toLowerCase().includes(query) ||
        b.customer_name.toLowerCase().includes(query) ||
        b.customer_email.toLowerCase().includes(query)
      );
    });

    tbody.replaceChildren();
    if (!filtered.length) {
      const tr = el("tr");
      const td = el("td", "muted", "Бронирования не найдены.");
      td.colSpan = 7;
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }

    filtered.forEach((b) => {
      const payment = state.paymentsByBooking.get(b.id);
      const ticket = state.ticketsByBooking.get(b.id);

      const tr = el("tr");

      const tdId = el("td", "mono", shortId(b.id));
      tdId.title = b.id;
      tr.appendChild(tdId);

      const tdCust = el("td");
      tdCust.appendChild(el("div", "", b.customer_name));
      tdCust.appendChild(el("div", "muted", b.customer_email));
      tr.appendChild(tdCust);

      tr.appendChild(el("td", "tabular", formatMoney(b.amount)));

      const tdStatus = el("td");
      tdStatus.appendChild(statusBadge(b.status));
      if (b.cancel_reason) {
        tdStatus.appendChild(el("div", "muted", b.cancel_reason));
      }
      tr.appendChild(tdStatus);

      const tdPay = el("td");
      tdPay.appendChild(payment ? statusBadge(payment.status) : el("span", "muted", "—"));
      tr.appendChild(tdPay);

      const tdTicket = el("td");
      if (ticket) {
        const btn = el("button", "btn btn-sm mono", ticket.ticket_code);
        btn.type = "button";
        btn.addEventListener("click", () => openTicketDialog(b, ticket, payment));
        tdTicket.appendChild(btn);
      } else {
        tdTicket.appendChild(el("span", "muted", "—"));
      }
      tr.appendChild(tdTicket);

      const tdAct = el("td");
      if (b.status === "CONFIRMED" || b.status === "PENDING") {
        const cancelBtn = el("button", "btn btn-sm btn-danger", "Отменить");
        cancelBtn.type = "button";
        cancelBtn.addEventListener("click", () => openCancelDialog(b));
        tdAct.appendChild(cancelBtn);
      } else {
        tdAct.appendChild(el("span", "muted", "—"));
      }
      tr.appendChild(tdAct);

      tbody.appendChild(tr);
    });
  }

  function renderNotificationsTable() {
    const tbody = $("notifications-tbody");
    $("notifications-count").textContent = String(state.notifications.length);
    tbody.replaceChildren();

    if (!state.notifications.length) {
      const tr = el("tr");
      const td = el("td", "muted", "Уведомлений пока нет.");
      td.colSpan = 4;
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }

    state.notifications.forEach((n) => {
      const tr = el("tr");
      tr.appendChild(el("td", "muted tabular", formatDate(n.created_at)));
      tr.appendChild(el("td", "", n.recipient_email));
      tr.appendChild(el("td", "mono", n.event_type));
      tr.appendChild(el("td", "", n.message));
      tbody.appendChild(tr);
    });
  }

  function openTicketDialog(booking, ticket, payment) {
    const details = $("ticket-details");
    details.replaceChildren();
    details.appendChild(el("div", "mono", `Код билета: ${ticket.ticket_code}`));
    details.appendChild(el("div", "", `Статус билета: ${ticket.status}`));
    details.appendChild(el("div", "", `Покупатель: ${booking.customer_name} (${booking.customer_email})`));
    details.appendChild(el("div", "tabular", `Сумма: ${formatMoney(booking.amount)}`));
    if (payment) {
      details.appendChild(el("div", "", `Статус платежа: ${payment.status}`));
    }
    details.appendChild(el("div", "mono", `Booking ID: ${booking.id}`));
    $("ticket-dialog").showModal();
  }

  function openCancelDialog(booking) {
    state.cancelBookingId = booking.id;
    $("cancel-booking-summary").textContent = `Бронь ${booking.id} (${booking.customer_name}, статус: ${booking.status})`;
    $("cancel-dialog").showModal();
  }

  // --- Events Binding ---
  function bindEvents() {
    document.querySelectorAll(".tab-btn").forEach((btn) => {
      btn.addEventListener("click", () => switchTab(btn.dataset.tab));
    });

    $("refresh-all-btn").addEventListener("click", async () => {
      await Promise.all([loadHealth(), loadVenues(), loadEvents()]);
      if (!$("panel-bookings").hidden) await loadBookingsAndRelated();
      showToast("Данные обновлены", "ok");
    });

    $("reload-seats-btn").addEventListener("click", () => {
      if (state.selectedEventId) loadSeats(state.selectedEventId);
    });

    $("regen-idem-btn").addEventListener("click", () => {
      $("booking-idem-key").value = newIdempotencyKey();
    });

    $("booking-form").addEventListener("submit", async (e) => {
      e.preventDefault();
      if (!state.selectedSeat || !state.selectedEventId) return;

      const submitBtn = $("booking-submit-btn");
      submitBtn.disabled = true;
      try {
        const formData = new FormData(e.target);
        const idemKey = String(formData.get("idempotency_key") || "").trim();
        const headers = { "Content-Type": "application/json" };
        if (idemKey) headers["Idempotency-Key"] = idemKey;

        const payload = {
          event_id: state.selectedEventId,
          seat_id: state.selectedSeat.id,
          customer_name: String(formData.get("customer_name") || "").trim(),
          customer_email: String(formData.get("customer_email") || "").trim(),
          simulate_payment_fail: Boolean($("booking-fail-sim").checked),
        };

        const booking = await apiRequest("/api/bookings", {
          method: "POST",
          headers,
          body: JSON.stringify(payload),
        });

        state.selectedSeat = null;
        showToast(`Бронь ${shortId(booking.id)} создана (${booking.status})`, "ok");
        pollSagaStatus(booking);
      } catch (err) {
        showToast(`Ошибка бронирования: ${err.message}`, "err");
      } finally {
        submitBtn.disabled = false;
      }
    });

    $("cancel-dialog-close").addEventListener("click", () => $("cancel-dialog").close());
    $("cancel-keep-btn").addEventListener("click", () => $("cancel-dialog").close());
    $("cancel-form").addEventListener("submit", async (e) => {
      e.preventDefault();
      if (!state.cancelBookingId) return;

      const btn = $("cancel-confirm-btn");
      btn.disabled = true;
      try {
        const reason = String($("cancel-reason").value || "").trim() || "Отменено пользователем";
        await apiRequest(`/api/bookings/${state.cancelBookingId}/cancel`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ reason }),
        });
        $("cancel-dialog").close();
        showToast("Бронь отменена, запущен возврат", "ok");
        await loadBookingsAndRelated();
        if (state.selectedEventId) await loadSeats(state.selectedEventId);
      } catch (err) {
        showToast(`Не удалось отменить бронь: ${err.message}`, "err");
      } finally {
        btn.disabled = false;
      }
    });

    $("ticket-dialog-close").addEventListener("click", () => $("ticket-dialog").close());
    $("ticket-ok-btn").addEventListener("click", () => $("ticket-dialog").close());

    $("bookings-filter-form").addEventListener("submit", (e) => {
      e.preventDefault();
      renderBookingsTable();
    });
    $("bookings-search").addEventListener("input", renderBookingsTable);

    $("create-venue-form").addEventListener("submit", async (e) => {
      e.preventDefault();
      const btn = e.target.querySelector('button[type="submit"]');
      btn.disabled = true;
      try {
        const fd = new FormData(e.target);
        const venue = await apiRequest("/api/venues", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            name: String(fd.get("name") || "").trim(),
            address: String(fd.get("address") || "").trim(),
            capacity: Number(fd.get("capacity")),
          }),
        });
        e.target.reset();
        showToast(`Площадка «${venue.name}» создана`, "ok");
        await loadVenues();
      } catch (err) {
        showToast(`Ошибка создания площадки: ${err.message}`, "err");
      } finally {
        btn.disabled = false;
      }
    });

    $("create-event-form").addEventListener("submit", async (e) => {
      e.preventDefault();
      const btn = e.target.querySelector('button[type="submit"]');
      btn.disabled = true;
      try {
        const fd = new FormData(e.target);
        const localDate = String(fd.get("starts_at") || "");
        const isoDate = new Date(localDate).toISOString();
        const event = await apiRequest("/api/events", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            venue_id: String(fd.get("venue_id")),
            title: String(fd.get("title") || "").trim(),
            description: String(fd.get("description") || "").trim(),
            starts_at: isoDate,
            base_price: String(fd.get("base_price")),
            rows: Number(fd.get("rows")),
            seats_per_row: Number(fd.get("seats_per_row")),
          }),
        });
        e.target.reset();
        showToast(`Мероприятие «${event.title}» создано`, "ok");
        await loadEvents();
        switchTab("events");
        await selectEvent(event.id);
      } catch (err) {
        showToast(`Ошибка создания мероприятия: ${err.message}`, "err");
      } finally {
        btn.disabled = false;
      }
    });
  }

  async function init() {
    setupDialogs();
    setupFormValidation();
    bindEvents();

    const dt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
    dt.setMinutes(dt.getMinutes() - dt.getTimezoneOffset());
    $("event-starts-at").value = dt.toISOString().slice(0, 16);

    await Promise.all([loadHealth(), loadVenues(), loadEvents()]);
  }

  init();
})();
