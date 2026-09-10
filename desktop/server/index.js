"use strict";

const path = require("path");
const os = require("os");
const express = require("express");
const multer = require("multer");
const { MAX_UPLOAD_BYTES, MAX_UPLOAD_FILES } = require("./core/constants");
const { LibraryStore } = require("./core/libraries");
const { SessionStore } = require("./core/session");
const { apiError } = require("./http/response");
const { parseClockWindow, splitClockValue, composeClockValue } = require("./services/parameters");
const { createSessionService } = require("./services/sessions");
const { createDataService } = require("./services/data");
const { createFilterService } = require("./services/filter");
const { createResultService } = require("./services/results");
const { createLibraryService } = require("./services/library");
const { createExportService } = require("./services/export");

function createApp(options = {}) {
  const dataDir =
    options.dataDir || path.join(os.homedir(), "AppData", "Roaming", "VehicleScreening");
  const frontendDir = options.frontendDir || path.join(__dirname, "..", "..", "static", "frontend");
  const app = express();
  app.use(express.json({ limit: "1mb" }));
  app.use(express.urlencoded({ extended: true, limit: "1mb" }));
  const upload = multer({
    storage: multer.memoryStorage(),
    limits: { fileSize: MAX_UPLOAD_BYTES, files: MAX_UPLOAD_FILES },
  });
  const libraries = new LibraryStore(dataDir);
  const sessions = new SessionStore(dataDir);
  sessions.pruneExpired();
  const sessionService = createSessionService({ sessions });
  const dataService = createDataService({ sessions, libraries, sessionService });
  const filterService = createFilterService({ sessions, libraries, sessionService });
  const resultService = createResultService({ sessions, sessionService });
  const libraryService = createLibraryService({ sessions, libraries });
  const exportService = createExportService({ sessionService });
  require("./routes/data")(app, { dataService, upload });
  require("./routes/query")(app, { filterService, resultService });
  require("./routes/library")(app, { libraryService, upload });
  require("./routes/export")(app, { exportService, sessions });
  // ---- SPA 静态资源 ----
  app.use("/app", express.static(frontendDir, { maxAge: 0 }));
  app.get("/app/", (req, res) => res.sendFile(path.join(frontendDir, "index.html")));
  app.get("/app", (req, res) => res.sendFile(path.join(frontendDir, "index.html")));
  app.get("/", (req, res) => res.sendFile(path.join(frontendDir, "index.html")));

  // 兜底错误处理
  app.use((err, req, res, next) => {
    if (res.headersSent) return;
    if (err instanceof multer.MulterError) {
      const message =
        err.code === "LIMIT_FILE_SIZE"
          ? "单个文件不能超过 500 MB，请拆分文件后重试。"
          : `一次最多上传 ${MAX_UPLOAD_FILES} 个文件，请调整文件列表。`;
      return apiError(res, message, err.code === "LIMIT_FILE_SIZE" ? 413 : 400);
    }
    console.error("[server]", err);
    res.status(500).json({ ok: false, message: "服务器内部错误。" });
  });

  return { app, libraries, sessions };
}

// 独立运行（npm run server，用于开发/对拍）
if (require.main === module) {
  const dataDir =
    process.env.VS_DATA_DIR || path.join(os.homedir(), "AppData", "Roaming", "VehicleScreening");
  const { app } = createApp({ dataDir });
  const port = Number(process.env.VS_PORT) || 11000;
  app.listen(port, "127.0.0.1", () => {
    console.log(`vehicle_screening server listening at http://127.0.0.1:${port}`);
  });
}

module.exports = { createApp, parseClockWindow, splitClockValue, composeClockValue };
