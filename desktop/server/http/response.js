"use strict";

class ApiError extends Error {
  constructor(message, statusCode = 400, code = "") {
    super(message);
    this.statusCode = statusCode;
    this.code = code;
  }
}

function apiSuccess(res, payload = {}) {
  res.json(Object.assign({ ok: true }, payload));
}

function apiError(res, message, statusCode = 400, code = "") {
  res.status(statusCode).json({ ok: false, message, status_code: statusCode, code });
}

// Shared response boundary; routes translate HTTP inputs, services return data or throw.
function routeHandler(handler, label, failureMessage) {
  return async (req, res) => {
    try {
      const payload = await handler(req, res);
      if (!res.headersSent && payload !== undefined) apiSuccess(res, payload);
    } catch (error) {
      if (res.headersSent) return;
      if (error instanceof ApiError)
        return apiError(res, error.message, error.statusCode, error.code);
      console.error(`[${label}]`, error);
      apiError(res, failureMessage, 500);
    }
  };
}

module.exports = { ApiError, apiSuccess, apiError, routeHandler };
