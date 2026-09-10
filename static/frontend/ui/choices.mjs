import { Scope } from "../core/lifecycle.mjs";
import { esc } from "../core/format.mjs";

let nextId = 0;
export function mountCombobox(control, { overlays }) {
  const scope = new Scope();
  const isSelect = control.tagName === "SELECT";
  const list = isSelect ? control : document.getElementById(control.getAttribute("list"));
  const options = [...(list?.querySelectorAll("option") || [])]
    .filter((option) => option.value)
    .map((option) => ({ value: option.value, label: option.textContent || option.value }));
  const wrapper = document.createElement("div");
  wrapper.className = "combobox-control";
  control.before(wrapper);
  wrapper.append(control);
  const input = isSelect ? document.createElement("input") : control;
  const nativeList = control.getAttribute("list");
  control.removeAttribute("list");
  let selectLabel;
  if (isSelect) {
    control.hidden = true;
    input.type = "text";
    input.id = `${control.id}-display`;
    input.value = options.find((item) => item.value === control.value)?.label || "";
    selectLabel = control.closest("form")?.querySelector(`label[for="${control.id}"]`);
    if (selectLabel) selectLabel.htmlFor = input.id;
    wrapper.append(input);
  }
  input.setAttribute("role", "combobox");
  input.setAttribute("aria-autocomplete", "list");
  input.setAttribute("aria-expanded", "false");
  input.autocomplete = "off";
  input.placeholder ||= "输入名称查找或点击选择";
  const toggle = document.createElement("button");
  toggle.type = "button";
  toggle.className = "combo-toggle";
  toggle.dataset.comboOpen = "";
  toggle.setAttribute("aria-label", "展开选项");
  toggle.innerHTML = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m7 10 5 5 5-5"/></svg>';
  wrapper.append(toggle);
  let popup;
  let choosing = false;
  let editingSearch = false;
  function open(search = false) {
    if (control.disabled) return;
    if (popup && overlays.active?.anchor === input) {
      render(search);
      return;
    }
    const content = document.createElement("div");
    content.className = "combo-panel";
    content.setAttribute("role", "listbox");
    content.id = `combo-options-${++nextId}`;
    popup = overlays.open({
      anchor: input,
      boundary: wrapper,
      content,
      label: input.getAttribute("aria-label") || "选择选项",
      role: "presentation",
      initialFocus: false,
      trap: false,
      onClose: () => {
        input.setAttribute("aria-expanded", "false");
        input.removeAttribute("aria-activedescendant");
        popup = null;
      },
    });
    popup.content = content;
    popup.index = -1;
    popup.search = search;
    popup.panel.classList.add("combo-popup");
    popup.panel.style.width = `${Math.max(280, Math.min(wrapper.offsetWidth, innerWidth - 24))}px`;
    input.setAttribute("aria-controls", content.id);
    input.setAttribute("aria-expanded", "true");
    popup.scope.on(content, "pointerdown", (event) => event.preventDefault());
    popup.scope.on(content, "click", (event) => {
      const option = event.target.closest("[data-option-index]");
      if (option) choose(Number(option.dataset.optionIndex));
    });
    render(search);
  }
  function render(search) {
    if (!popup) return;
    popup.search = search;
    const query = search ? input.value.trim().toLocaleLowerCase() : "";
    popup.matches = options.filter((option) => option.label.toLocaleLowerCase().includes(query));
    popup.index = -1;
    popup.content.innerHTML = popup.matches.length
      ? popup.matches
          .map(
            (option, index) =>
              `<button type="button" role="option" tabindex="-1" id="combo-item-${nextId}-${index}" data-option-index="${index}" aria-selected="${control.value === option.value}"><span>${esc(option.label)}</span>${control.value === option.value ? '<span aria-hidden="true">✓</span>' : ""}</button>`
          )
          .join("")
      : '<p class="empty-inline">没有匹配项，请调整搜索内容。</p>';
    input.removeAttribute("aria-activedescendant");
    popup.position();
  }
  function choose(index) {
    const option = popup?.matches[index];
    if (!option) return;
    control.value = option.value;
    input.value = option.label;
    if (!isSelect) input.value = option.value;
    overlays.close("choose", false);
    input.focus({ preventScroll: true });
    choosing = true;
    try {
      control.dispatchEvent(new Event("input", { bubbles: true }));
      control.dispatchEvent(new Event("change", { bubbles: true }));
    } finally {
      choosing = false;
    }
  }
  scope.on(input, "input", () => {
    if (choosing) return;
    if (isSelect) {
      control.value = options.find((option) => option.label === input.value)?.value || "";
      editingSearch = true;
      try {
        control.dispatchEvent(new Event("change", { bubbles: true }));
      } finally {
        editingSearch = false;
      }
    }
    open(true);
  });
  scope.on(toggle, "pointerdown", (event) => event.preventDefault());
  scope.on(toggle, "click", () => {
    input.focus();
    if (popup) overlays.close("cancel");
    else open(false);
  });
  scope.on(input, "keydown", (event) => {
    if (["ArrowDown", "ArrowUp"].includes(event.key)) {
      event.preventDefault();
      if (!popup) open(false);
      if (!popup?.matches.length) return;
      const direction = event.key === "ArrowDown" ? 1 : -1;
      popup.index = Math.max(
        0,
        Math.min(
          popup.matches.length - 1,
          popup.index < 0 ? (direction > 0 ? 0 : popup.matches.length - 1) : popup.index + direction
        )
      );
      popup.content
        .querySelectorAll("[data-option-index]")
        .forEach((node, index) => node.classList.toggle("is-active", index === popup.index));
      const current = popup.content.querySelector(`[data-option-index="${popup.index}"]`);
      input.setAttribute("aria-activedescendant", current.id);
      current.scrollIntoView({ block: "nearest" });
    } else if (event.key === "Enter" && popup) {
      event.preventDefault();
      if (popup.index >= 0) choose(popup.index);
    }
  });
  scope.on(control, "change", () => {
    if (isSelect && !editingSearch)
      input.value = options.find((option) => option.value === control.value)?.label || "";
  });
  return {
    update: () => {
      toggle.disabled = input.disabled = control.disabled;
    },
    destroy: () => {
      if (overlays.active?.anchor === input) overlays.close("unmount", false);
      scope.destroy();
      if (isSelect) {
        if (selectLabel) selectLabel.htmlFor = control.id;
        input.remove();
        control.hidden = false;
      } else if (nativeList) control.setAttribute("list", nativeList);
      toggle.remove();
      wrapper.before(control);
      wrapper.remove();
    },
  };
}

export function mountChoiceList(picker, onChange) {
  const scope = new Scope();
  const rows = [...picker.querySelectorAll("[data-choice-row]")];
  const input = picker.querySelector("[data-choice-search]");
  const update = () => {
    const query = input?.value.trim().toLocaleLowerCase() || "";
    rows.forEach((row) => {
      row.hidden = !row.dataset.text.includes(query);
    });
    const selected = rows.filter((row) => row.querySelector("input").checked).length;
    picker.querySelector("[data-selected-count]").textContent = selected
      ? `已选择 ${selected} 项`
      : "点击选择，可多选";
    const count = picker.querySelector("[data-visible-count]");
    if (count)
      count.textContent = `显示 ${rows.filter((row) => !row.hidden).length} / ${rows.length} 项 · 已选 ${selected} 项`;
  };
  scope.on(input, "input", update);
  scope.on(input, "keydown", (event) => {
    if (event.key === "Enter") event.preventDefault();
  });
  scope.on(picker, "change", () => {
    update();
    onChange?.();
  });
  for (const [selector, checked] of [
    ["[data-select-visible]", true],
    ["[data-clear-visible]", false],
  ])
    scope.on(picker.querySelector(selector), "click", () => {
      rows
        .filter((row) => !row.hidden)
        .forEach((row) => {
          const box = row.querySelector("input");
          if (!box.disabled) box.checked = checked;
        });
      picker.dispatchEvent(new Event("change", { bubbles: true }));
    });
  update();
  return { update, destroy: () => scope.destroy() };
}
