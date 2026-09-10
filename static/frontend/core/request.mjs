export class ApiError extends Error {
  constructor(message, status = 0, code = "") {
    super(message);
    this.status = status;
    this.code = code;
  }
}
export async function api(path, options = {}) {
  let response;
  try {
    response = await fetch(path, options);
  } catch (error) {
    if (error.name === "AbortError") throw error;
    throw new ApiError("暂时无法连接本地服务，数据和填写内容已保留。请重试。");
  }
  let payload;
  try {
    payload = await response.json();
  } catch (error) {
    throw new ApiError("服务响应不完整，请重试。", response.status);
  }
  if (!payload || typeof payload !== "object" || !response.ok || payload.ok === false)
    throw new ApiError(
      payload?.message || "请求失败，请重试。",
      response.status,
      payload?.code || ""
    );
  return payload;
}
export const post = (path, payload) =>
  api(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

export function uploadTraffic(body, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", "/api/upload");
    xhr.responseType = "json";
    xhr.upload.addEventListener("progress", (event) =>
      onProgress(
        event.lengthComputable
          ? `传输到本地服务：${Math.round((event.loaded / event.total) * 100)}%`
          : "正在传输到本地服务…"
      )
    );
    xhr.upload.addEventListener("load", () =>
      onProgress("文件已收到，正在读取 Excel、识别车牌和地点…")
    );
    xhr.addEventListener("load", () => {
      const payload = xhr.response;
      if (xhr.status < 200 || xhr.status >= 300 || !payload || payload.ok === false)
        reject(
          new ApiError(
            payload?.message || "读取失败，请检查文件后重试。",
            xhr.status,
            payload?.code
          )
        );
      else resolve(payload);
    });
    xhr.addEventListener("error", () => reject(new ApiError("无法连接本地服务，文件列表已保留。")));
    xhr.addEventListener("abort", () => reject(new ApiError("读取已取消，文件列表已保留。")));
    onProgress("正在准备读取…");
    xhr.send(body);
  });
}
