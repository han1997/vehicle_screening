import { Scope } from "../core/lifecycle.mjs";
import { esc } from "../core/format.mjs";
import {
  parseDateParts,
  parseTimeParts,
  canonicalDateTime,
  dateString,
  monthCells,
  addDays,
  pad,
} from "../domain/datetime.mjs";

export function mountDateTime(input, { overlays, referenceDate = "" }) {
  const scope = new Scope();
  const kind = input.dataset.dateKind;
  const field = input.closest(".field");
  const wrapper = document.createElement("div");
  wrapper.className = "date-control";
  input.before(wrapper);
  wrapper.append(input);
  const trigger = document.createElement("button");
  trigger.type = "button";
  trigger.className = "date-trigger";
  trigger.dataset.dateOpen = "";
  trigger.setAttribute(
    "aria-label",
    `选择${kind === "date" ? "日期" : kind === "time" ? "时间" : "日期和时间"}`
  );
  trigger.setAttribute("aria-haspopup", "dialog");
  trigger.setAttribute("aria-expanded", "false");
  trigger.innerHTML =
    kind === "time"
      ? '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="M12 7v5l3 2"/></svg>'
      : '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="4" y="5" width="16" height="16" rx="2"/><path d="M8 3v4m8-4v4M4 11h16"/></svg>';
  wrapper.append(trigger);
  input.placeholder =
    kind === "date" ? "YYYY-MM-DD" : kind === "time" ? "HH:mm" : "YYYY-MM-DD HH:mm";
  input.setAttribute("autocomplete", "off");
  const normalize = () => {
    const value = canonicalDateTime(input.value, kind);
    field?.querySelector("[data-date-error]")?.remove();
    if (value === null) {
      const message = document.createElement("p");
      message.className = "field-error";
      message.dataset.dateError = "";
      message.id = `date-error-${input.id}`;
      input.setAttribute("aria-errormessage", message.id);
      message.textContent =
        kind === "date"
          ? "请输入有效日期，例如 2026-04-29。"
          : kind === "time"
            ? "请输入 24 小时制时间，例如 19:00。"
            : "请输入有效日期和时间，例如 2026-04-29 19:00。";
      field?.append(message);
      input.setAttribute("aria-invalid", "true");
    } else {
      input.value = kind === "datetime-local" ? value.replace("T", " ") : value;
      input.removeAttribute("aria-invalid");
      input.removeAttribute("aria-errormessage");
    }
  };
  normalize();
  scope.on(input, "change", normalize);
  scope.on(input, "blur", normalize);
  function open() {
    if (input.disabled || trigger.disabled) return;
    if (overlays.active?.anchor === trigger) {
      overlays.close("cancel");
      return;
    }
    const initial = canonicalDateTime(input.value, kind);
    let draftDate = kind === "time" ? "" : initial ? initial.split("T")[0] : "";
    let draftTime =
      kind === "date"
        ? ""
        : initial
          ? kind === "time"
            ? initial
            : initial.split("T")[1]
          : "00:00";
    const now = new Date();
    const fallback = parseDateParts(referenceDate) || {
      year: now.getFullYear(),
      month: now.getMonth() + 1,
      day: now.getDate(),
    };
    let displayed = parseDateParts(draftDate) || fallback;
    let focusedDay = dateString(displayed);
    let showMonths = false;
    const content = document.createElement("div");
    content.className = `date-picker ${kind === "datetime-local" ? "has-time" : ""}`;
    content.dataset.datePicker = kind;
    const label = field?.querySelector("label")?.textContent || "选择日期时间";
    const popup = overlays.open({ anchor: trigger, content, label, initialFocus: false });
    const render = (focus) => {
      const selectedTime = parseTimeParts(draftTime) || { hour: 0, minute: 0 };
      const calendar =
        kind === "time"
          ? ""
          : `<div class="calendar"><div class="calendar-toolbar">
        <button type="button" class="icon-button" data-month-step="-1" aria-label="上个月">‹</button>
        <label class="calendar-year"><span class="sr-only">年份</span><input type="number" min="1" max="9999" step="1" value="${displayed.year}" data-calendar-year aria-label="年份"></label><span>年</span>
        <button type="button" class="text-button" data-month-toggle aria-expanded="${showMonths}">${displayed.month}月</button>
        <button type="button" class="icon-button" data-month-step="1" aria-label="下个月">›</button></div>
        ${
          showMonths
            ? `<div class="month-grid">${Array.from({ length: 12 }, (_, i) => `<button type="button" data-pick-month="${i + 1}" aria-pressed="${displayed.month === i + 1}">${i + 1}月</button>`).join("")}</div>`
            : `<div class="calendar-week" aria-hidden="true">${["一", "二", "三", "四", "五", "六", "日"].map((day) => `<span>${day}</span>`).join("")}</div><div class="calendar-days" role="grid" aria-label="${displayed.year}年${displayed.month}月">${monthCells(
                displayed.year,
                displayed.month
              )
                .map((parts) => {
                  const value = dateString(parts);
                  const allowed = parts.year >= 1 && parts.year <= 9999;
                  return /* HTML */ `<button
                    type="button"
                    role="gridcell"
                    data-date-day="${value}"
                    tabindex="${value === focusedDay ? 0 : -1}"
                    aria-label="${parts.year}年${parts.month}月${parts.day}日"
                    aria-selected="${draftDate === value}"
                    ${allowed ? "" : "disabled"}
                    class="${parts.month !== displayed.month ? "other-month" : ""}"
                  >
                    ${parts.day}
                  </button>`;
                })
                .join("")}</div>`
        }</div>`;
      const time =
        kind === "date"
          ? ""
          : `<div class="time-picker"><p class="time-caption">24 小时制</p><div class="time-columns">${[
              ["hour", 24, selectedTime.hour, "小时"],
              ["minute", 60, selectedTime.minute, "分钟"],
            ]
              .map(
                ([type, count, selected, text]) =>
                  `<div><span>${text}</span><div class="time-options" role="listbox" aria-label="${text}">${Array.from({ length: count }, (_, index) => `<button type="button" role="option" data-time-part="${type}" data-time-value="${index}" tabindex="${index === selected ? 0 : -1}" aria-selected="${index === selected}">${pad(index)}</button>`).join("")}</div></div>`
              )
              .join("")}</div></div>`;
      content.innerHTML = /* HTML */ `<div class="popup-title">
          <strong>${esc(label)}</strong
          ><button type="button" class="icon-button" data-date-cancel aria-label="取消选择">
            ×
          </button>
        </div>
        <div class="date-picker-body">${calendar}${time}</div>
        <p class="picker-preview" aria-live="polite">
          ${esc(
            [draftDate, kind === "date" ? "" : draftTime].filter(Boolean).join(" ") || "未选择"
          )}
        </p>
        <div class="popup-actions">
          <div>
            ${kind !== "time"
              ? '<button class="text-button" type="button" data-date-today>今天</button>'
              : ""}<button class="text-button" type="button" data-date-clear>清空</button>
          </div>
          <button class="button secondary small" type="button" data-date-cancel>取消</button
          ><button class="button primary small" type="button" data-date-apply>确定</button>
        </div>`;
      // Initial empty date is not committed by merely opening the panel.
      if (kind === "datetime-local" && !draftDate && draftTime)
        content.querySelector("[data-date-apply]").disabled = true;
      popup.position();
      content.querySelectorAll('.time-options [aria-selected="true"]').forEach((node) => {
        node.parentElement.scrollTop = Math.max(
          0,
          node.offsetTop - node.parentElement.offsetTop - 55
        );
      });
      if (focus) content.querySelector(focus)?.focus({ preventScroll: true });
    };
    const moveMonth = (amount) => {
      let year = displayed.year;
      let month = displayed.month + amount;
      if (month < 1) {
        year -= 1;
        month = 12;
      }
      if (month > 12) {
        year += 1;
        month = 1;
      }
      if (year < 1 || year > 9999) return;
      displayed = { year, month, day: 1 };
      focusedDay = dateString(displayed);
    };
    popup.scope.on(content, "click", (event) => {
      const button = event.target.closest("button");
      if (!button || button.disabled) return;
      if (button.hasAttribute("data-date-cancel")) {
        popup.close("cancel");
        return;
      }
      if (button.hasAttribute("data-date-apply")) {
        const value =
          kind === "date"
            ? draftDate
            : kind === "time"
              ? draftTime
              : draftDate
                ? `${draftDate}T${draftTime || "00:00"}`
                : "";
        if (canonicalDateTime(value, kind) === null) return;
        popup.close("apply");
        input.value = value;
        normalize();
        input.dispatchEvent(new Event("input", { bubbles: true }));
        input.dispatchEvent(new Event("change", { bubbles: true }));
        return;
      }
      if (button.hasAttribute("data-date-clear")) {
        draftDate = "";
        draftTime = "";
        render("[data-date-apply]");
        return;
      }
      if (button.hasAttribute("data-date-today")) {
        displayed = { year: now.getFullYear(), month: now.getMonth() + 1, day: now.getDate() };
        draftDate = dateString(displayed);
        focusedDay = draftDate;
        render(`[data-date-day="${focusedDay}"]`);
        return;
      }
      if (button.dataset.monthStep) {
        moveMonth(Number(button.dataset.monthStep));
        render(`[data-month-step="${button.dataset.monthStep}"]`);
        return;
      }
      if (button.hasAttribute("data-month-toggle")) {
        showMonths = !showMonths;
        render(showMonths ? `[data-pick-month="${displayed.month}"]` : "[data-month-toggle]");
        return;
      }
      if (button.dataset.pickMonth) {
        displayed.month = Number(button.dataset.pickMonth);
        displayed.day = 1;
        focusedDay = dateString(displayed);
        showMonths = false;
        render(`[data-date-day="${focusedDay}"]`);
        return;
      }
      if (button.dataset.dateDay) {
        draftDate = button.dataset.dateDay;
        focusedDay = draftDate;
        displayed = parseDateParts(draftDate);
        if (kind === "datetime-local" && !draftTime) draftTime = "00:00";
        render(`[data-date-day="${focusedDay}"]`);
        return;
      }
      if (button.dataset.timePart) {
        const parts = parseTimeParts(draftTime) || { hour: 0, minute: 0 };
        parts[button.dataset.timePart] = Number(button.dataset.timeValue);
        draftTime = `${pad(parts.hour)}:${pad(parts.minute)}`;
        render(
          `[data-time-part="${button.dataset.timePart}"][data-time-value="${button.dataset.timeValue}"]`
        );
      }
    });
    popup.scope.on(content, "change", (event) => {
      if (!event.target.hasAttribute("data-calendar-year")) return;
      const year = Number(event.target.value);
      if (!Number.isInteger(year) || year < 1 || year > 9999) {
        event.target.setAttribute("aria-invalid", "true");
        return;
      }
      displayed.year = year;
      displayed.day = 1;
      focusedDay = dateString(displayed);
      render("[data-calendar-year]");
    });
    popup.scope.on(content, "keydown", (event) => {
      const button = event.target.closest("button");
      if (event.target.hasAttribute("data-calendar-year") && event.key === "Enter") {
        event.preventDefault();
        event.target.dispatchEvent(new Event("change", { bubbles: true }));
        return;
      }
      if (!button) return;
      if (
        ["Enter", " "].includes(event.key) &&
        (button.dataset.dateDay || button.dataset.timePart)
      ) {
        event.preventDefault();
        button.click();
        return;
      }
      if (button.dataset.dateDay) {
        const offsets = { ArrowLeft: -1, ArrowRight: 1, ArrowUp: -7, ArrowDown: 7 };
        let next = button.dataset.dateDay;
        if (event.key in offsets) next = addDays(next, offsets[event.key]);
        else if (event.key === "Home" || event.key === "End") {
          const date = new Date(`${next}T12:00:00`);
          const weekday = (date.getDay() + 6) % 7;
          next = addDays(next, event.key === "Home" ? -weekday : 6 - weekday);
        } else if (event.key === "PageUp" || event.key === "PageDown") {
          moveMonth(event.key === "PageUp" ? -1 : 1);
          next = focusedDay;
        } else return;
        event.preventDefault();
        const parts = parseDateParts(next);
        if (!parts) return;
        displayed = parts;
        focusedDay = next;
        render(`[data-date-day="${next}"]`);
      } else if (
        button.dataset.timePart &&
        ["ArrowUp", "ArrowDown", "Home", "End"].includes(event.key)
      ) {
        event.preventDefault();
        const maximum = button.dataset.timePart === "hour" ? 23 : 59;
        const value = Number(button.dataset.timeValue);
        const next =
          event.key === "Home"
            ? 0
            : event.key === "End"
              ? maximum
              : Math.max(0, Math.min(maximum, value + (event.key === "ArrowDown" ? 1 : -1)));
        const node = content.querySelector(
          `[data-time-part="${button.dataset.timePart}"][data-time-value="${next}"]`
        );
        button.tabIndex = -1;
        node.tabIndex = 0;
        node.focus({ preventScroll: true });
        node.scrollIntoView({ block: "nearest" });
      }
    });
    render(
      kind === "time"
        ? '[data-time-part="hour"][aria-selected="true"]'
        : `[data-date-day="${focusedDay}"]`
    );
  }
  scope.on(trigger, "click", open);
  scope.on(input, "keydown", (event) => {
    if (event.key === "ArrowDown" && event.altKey) {
      event.preventDefault();
      open();
    }
  });
  return {
    update: () => {
      trigger.disabled = input.disabled;
    },
    destroy: () => {
      if (overlays.active?.anchor === trigger) overlays.close("unmount", false);
      scope.destroy();
      trigger.remove();
      wrapper.before(input);
      wrapper.remove();
    },
  };
}
