"use strict";

const { MAX_UPLOAD_FILES } = require("../core/constants");
const { ApiError, routeHandler } = require("../http/response");

function registerDataRoutes(app, { dataService, upload }) {
  const { buildHomePayload, createTrafficSession, buildReviewPayload } = dataService;
  app.get(
    "/api/home",
    routeHandler((req) => buildHomePayload(req.query.data_id || ""), "home", "服务器内部错误。")
  );
  app.post(
    "/api/upload",
    upload.array("files", MAX_UPLOAD_FILES),
    routeHandler(
      async (req) => {
        if (!(req.files || []).length) throw new ApiError("未找到上传文件。");
        return createTrafficSession(req.files);
      },
      "upload",
      "上传处理失败，请重试。"
    )
  );
  app.get(
    "/api/review/:dataId",
    routeHandler(
      (req) => buildReviewPayload(req.params.dataId, req.query.mode),
      "review",
      "读取会话失败。"
    )
  );
}

module.exports = registerDataRoutes;
