"use strict";

const { routeHandler } = require("../http/response");

function registerQueryRoutes(app, { filterService, resultService }) {
  const { executeFilter } = filterService;
  const { buildResultsPayload, buildVehiclePayload } = resultService;
  app.post(
    "/api/filter/:dataId",
    routeHandler(
      (req) => {
        const filterMode = executeFilter(req.params.dataId, req.body || {});
        return {
          filter_mode: filterMode,
          results_payload: buildResultsPayload(req.params.dataId, 1, false, filterMode),
        };
      },
      "filter",
      "筛选执行失败。"
    )
  );
  app.get(
    "/api/results/:dataId",
    routeHandler(
      (req) =>
        buildResultsPayload(req.params.dataId, Number(req.query.page) || 1, true, req.query.mode),
      "results",
      "读取结果失败。"
    )
  );
  for (const [route, detail] of [
    ["vehicles", false],
    ["vehicle", true],
  ]) {
    app.get(
      `/api/results/:dataId/${route}`,
      routeHandler(
        (req) => buildVehiclePayload(req.params.dataId, req.query, detail),
        "vehicle-results",
        "读取车辆结果失败，请重试。"
      )
    );
  }
}

module.exports = registerQueryRoutes;
