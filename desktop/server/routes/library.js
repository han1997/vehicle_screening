"use strict";

const { formGetAll } = require("../services/parameters");
const { apiError, routeHandler } = require("../http/response");

function registerLibraryRoutes(app, { libraryService, upload }) {
  const {
    buildCheckpointLibraryPayload,
    buildKeypersonLibraryPayload,
    importCheckpoints,
    deleteCheckpoints,
    importKeypersons,
    deleteKeypersons,
  } = libraryService;
  app.get(
    "/api/libraries/checkpoints",
    routeHandler(
      (req) => buildCheckpointLibraryPayload(req.query.data_id || ""),
      "checkpoints",
      "服务器内部错误。"
    )
  );
  app.get(
    "/api/libraries/keypersons",
    routeHandler(
      (req) => buildKeypersonLibraryPayload(req.query.data_id || ""),
      "keypersons",
      "服务器内部错误。"
    )
  );
  app.post(
    "/api/checkpoints/import/:dataId",
    routeHandler(
      (req) => importCheckpoints(req.params.dataId, req.body && req.body.checkpoint_source_column),
      "checkpoints",
      "服务器内部错误。"
    )
  );
  const keypersonUpload = upload.single("keyperson_file");
  for (const suffix of ["", "/:dataId"]) {
    app.post(
      `/api/checkpoints/delete${suffix}`,
      routeHandler(
        (req) =>
          deleteCheckpoints(
            req.params.dataId || "",
            formGetAll(req.body || {}, "delete_checkpoints")
          ),
        "checkpoints",
        "服务器内部错误。"
      )
    );
    app.post(
      `/api/keypersons/delete${suffix}`,
      routeHandler(
        (req) =>
          deleteKeypersons(
            req.params.dataId || "",
            formGetAll(req.body || {}, "delete_keypersons")
          ),
        "keypersons",
        "服务器内部错误。"
      )
    );
    app.post(`/api/keypersons/import${suffix}`, (req, res) => {
      keypersonUpload(req, res, (error) => {
        if (error) return apiError(res, "重点人文件上传失败。", 400);
        return routeHandler(
          (req) => importKeypersons(req.params.dataId || "", req.file),
          "keypersons",
          "服务器内部错误。"
        )(req, res);
      });
    });
  }
}

module.exports = registerLibraryRoutes;
