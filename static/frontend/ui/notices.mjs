export function createNotices({ notices, actions }) {
  function notice(message, type = "info", retry) {
    if (!message) return;
    const node = document.createElement("div");
    node.className = `notice ${type}`;
    node.setAttribute("role", type === "error" ? "alert" : "status");
    const text = document.createElement("span");
    text.textContent = message;
    node.appendChild(text);
    if (retry) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "button secondary small";
      button.textContent = "重试";
      button.addEventListener("click", () => {
        node.remove();
        retry();
      });
      node.appendChild(button);
    }
    const close = document.createElement("button");
    close.type = "button";
    close.className = "icon-button";
    close.textContent = "×";
    close.setAttribute("aria-label", "关闭提示");
    close.addEventListener("click", () => node.remove());
    node.appendChild(close);
    notices.appendChild(node);
    if (type === "success") setTimeout(() => node.remove(), 5500);
  }
  return { notice };
}
