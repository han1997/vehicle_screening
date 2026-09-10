"use strict";

const { app, BrowserWindow, dialog, shell, Menu } = require("electron");
const path = require("path");
const fs = require("fs");
const os = require("os");
const { createApp } = require("../server/index");

// Win7 老显卡兜底：禁用 GPU 加速，走软件渲染（本 UI 无重绘压力）
app.disableHardwareAcceleration();
// Win7 上 Chromium 多进程沙箱依赖 Win10 API，禁用以避免启动崩溃
app.commandLine.appendSwitch("no-sandbox");
app.commandLine.appendSwitch("disable-features", "HardwareMediaKeyHandling");

const isDev = !app.isPackaged;

// 数据目录：%APPDATA%\VehicleScreening（Win7/Win10 通用，免管理员可写）
const dataDir = path.join(app.getPath("appData"), "VehicleScreening");
fs.mkdirSync(dataDir, { recursive: true });

let mainWindow = null;
let server = null;

function startServer() {
  const frontendDir = isDev
    ? path.join(__dirname, "..", "..", "static", "frontend")
    : path.join(process.resourcesPath, "frontend");
  const created = createApp({ dataDir, frontendDir });
  return new Promise((resolve, reject) => {
    // 端口从 11000 起顺延，最多尝试 20 个
    const basePort = 11000;
    let attempt = 0;
    const tryListen = () => {
      const port = basePort + attempt;
      const listener = created.app.listen(port, "127.0.0.1");
      listener.once("listening", () => resolve({ listener, port }));
      listener.once("error", (err) => {
        if (err.code === "EADDRINUSE" && attempt < 20) {
          attempt += 1;
          tryListen();
        } else {
          reject(err);
        }
      });
    };
    tryListen();
  });
}

function createWindow(port) {
  mainWindow = new BrowserWindow({
    width: 1280,
    height: 840,
    minWidth: 980,
    minHeight: 640,
    title: "车辆进出筛选工具",
    icon: path.join(__dirname, "..", "build", "icon.ico"),
    autoHideMenuBar: true,
    show: false,
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
      spellcheck: false,
    },
  });

  Menu.setApplicationMenu(null);
  mainWindow.loadURL(`http://127.0.0.1:${port}/app`);

  mainWindow.once("ready-to-show", () => {
    mainWindow.show();
  });

  // 外部链接走系统浏览器，下载走原生保存对话框
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    if (url.startsWith(`http://127.0.0.1:${port}`)) {
      return { action: "allow" };
    }
    shell.openExternal(url);
    return { action: "deny" };
  });

  mainWindow.webContents.session.on("will-download", (event, item, webContents) => {
    const defaultName = item.getFilename();
    const result = dialog.showSaveDialogSync(mainWindow, {
      title: "导出筛选结果",
      defaultPath: defaultName,
      filters: [{ name: "Excel 工作簿", extensions: ["xlsx"] }],
    });
    if (!result) {
      event.preventDefault();
      return;
    }
    item.setSavePath(result);
    item.once("done", (e, state) => {
      if (state === "completed") {
        dialog.showMessageBox(mainWindow, {
          type: "info",
          title: "导出完成",
          message: `已导出：${result}`,
          buttons: ["打开所在文件夹", "确定"],
        }).then(({ response }) => {
          if (response === 0) shell.showItemInFolder(result);
        });
      } else if (state === "interrupted") {
        dialog.showErrorBox("导出失败", "文件保存被中断，请重试。");
      }
    });
  });

  mainWindow.on("closed", () => {
    mainWindow = null;
  });
}

// 单实例锁：重复启动时聚焦已有窗口
const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
} else {
  app.on("second-instance", () => {
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore();
      mainWindow.focus();
    }
  });

  app.whenReady().then(async () => {
    try {
      server = await startServer();
      createWindow(server.port);
    } catch (err) {
      dialog.showErrorBox(
        "启动失败",
        `本地服务启动失败：${err.message}\n\n请关闭已打开的车辆筛选工具后重试。`
      );
      app.quit();
    }
  });

  app.on("window-all-closed", () => {
    if (server && server.listener) {
      try {
        server.listener.close();
      } catch (err) {
        /* ignore */
      }
    }
    app.quit();
  });

  process.on("uncaughtException", (err) => {
    try {
      dialog.showErrorBox("运行错误", `程序遇到未处理的错误：${err.message}`);
    } catch (e) {
      /* ignore */
    }
  });
}
