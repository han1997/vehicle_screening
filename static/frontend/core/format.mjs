export const esc = (value) =>
  String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
export const num = (value) => Number(value || 0).toLocaleString("zh-CN");
export const timeText = (value) => String(value || "").replace("T", " ");
export const bytes = (value) =>
  value < 1024 * 1024
    ? `${(value / 1024).toFixed(1)} KB`
    : `${(value / 1024 / 1024).toFixed(1)} MB`;
export const clone = (value) => JSON.parse(JSON.stringify(value));
export const featureHash = (mode) => `#/function/${mode}`;
export const fileKey = (file) =>
  [file.webkitRelativePath || file.name, file.size, file.lastModified].join("::");
