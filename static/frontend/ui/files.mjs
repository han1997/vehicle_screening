import { Scope } from "../core/lifecycle.mjs";

export function mountFileInput(input) {
  const scope = new Scope();
  const wrapper = document.createElement("div");
  wrapper.className = "file-control";
  input.before(wrapper);
  wrapper.append(input);
  input.hidden = true;
  const button = document.createElement("button");
  button.type = "button";
  button.className = "button secondary";
  button.textContent = "选择 Excel 文件";
  button.dataset.fileChoose = "";
  const label = document.createElement("span");
  label.className = "file-control-name";
  const clear = document.createElement("button");
  clear.type = "button";
  clear.className = "text-button";
  clear.textContent = "移除";
  clear.setAttribute("aria-label", "移除所选文件");
  wrapper.append(button, label, clear);
  const update = () => {
    const files = [...input.files];
    label.textContent = files.length ? files.map((file) => file.name).join("、") : "未选择文件";
    label.title = label.textContent;
    clear.hidden = !files.length;
    button.disabled = clear.disabled = input.disabled;
  };
  scope.on(button, "click", () => input.click());
  scope.on(clear, "click", () => {
    input.value = "";
    input.dispatchEvent(new Event("change", { bubbles: true }));
  });
  scope.on(input, "change", update);
  update();
  return {
    update,
    destroy: () => {
      scope.destroy();
      wrapper.before(input);
      input.hidden = false;
      wrapper.remove();
    },
  };
}
