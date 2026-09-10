"use strict";

// 渲染进程运行在 contextIsolation 下，无需暴露 Node 能力；
// 保留 preload 以备后续需要（如原生通知）。
