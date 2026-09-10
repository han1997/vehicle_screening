"use strict";

const { routeHandler } = require("../http/response");

function registerExportRoutes(app, { exportService, sessions }) {
  app.get(
    "/download/:dataId",
    routeHandler(
      async (req, res) => {
        const { buffer, filename } = await exportService.prepareDownload(
          req.params.dataId,
          req.query.mode
        );
        const encoded = encodeURIComponent(filename).replace(/'/g, "%27");
        res.setHeader(
          "Content-Type",
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        );
        res.setHeader("Content-Disposition", `attachment; filename*=UTF-8''${encoded}`);
        sessions.touch(req.params.dataId);
        res.send(buffer);
      },
      "download",
      "导出失败。"
    )
  );
}

module.exports = registerExportRoutes;
