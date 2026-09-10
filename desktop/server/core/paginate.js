"use strict";

const { PAGE_SIZE } = require("./constants");

function paginateGroupedResultsByRows(results, page, maxRowsPerPage) {
  if (!results || !results.length) return { rows: [], page: 1, totalPages: 1, hasPrev: false, hasNext: false };

  const totalRows = results.length;
  const groupStarts = [];
  results.forEach((row, i) => {
    if (row.group_first) groupStarts.push(i);
  });
  if (!groupStarts.length) return { rows: [], page: 1, totalPages: 1, hasPrev: false, hasNext: false };

  const groupRanges = [];
  for (let idx = 0; idx < groupStarts.length; idx++) {
    const start = groupStarts[idx];
    const end = idx + 1 < groupStarts.length ? groupStarts[idx + 1] : totalRows;
    groupRanges.push([start, end]);
  }

  const rowLimit = Math.max(1, Number(maxRowsPerPage) || 1);
  const pages = [];
  let pageStart = null;
  let pageEnd = null;
  let pageRows = 0;

  for (const [start, end] of groupRanges) {
    const groupRows = end - start;
    if (pageStart === null) {
      pageStart = start;
      pageEnd = end;
      pageRows = groupRows;
      continue;
    }
    if (pageRows + groupRows > rowLimit) {
      pages.push([pageStart, pageEnd]);
      pageStart = start;
      pageEnd = end;
      pageRows = groupRows;
    } else {
      pageEnd = end;
      pageRows += groupRows;
    }
  }
  if (pageStart !== null) pages.push([pageStart, pageEnd]);

  const totalPages = Math.max(1, pages.length);
  const safePage = Math.max(1, Math.min(page, totalPages));
  const [startRow, endRow] = pages[safePage - 1];
  return {
    rows: results.slice(startRow, endRow),
    page: safePage,
    totalPages,
    hasPrev: safePage > 1,
    hasNext: safePage < totalPages,
  };
}

function paginateResults(results, page, filterMode) {
  if (!results || !results.length) return { rows: [], page: 1, totalPages: 1, hasPrev: false, hasNext: false };

  const total = results.length;

  if (filterMode === "pair" || filterMode === "timed_cross") {
    const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
    const safePage = Math.max(1, Math.min(page, totalPages));
    const start = (safePage - 1) * PAGE_SIZE;
    const end = Math.min(start + PAGE_SIZE, total);
    return { rows: results.slice(start, end), page: safePage, totalPages, hasPrev: safePage > 1, hasNext: safePage < totalPages };
  }

  if (filterMode === "keyperson") {
    return paginateGroupedResultsByRows(results, page, PAGE_SIZE);
  }

  // 频繁模式：按车辆组边界分页（按车辆组数计页）
  const groupStarts = [];
  results.forEach((r, i) => {
    if (r.group_first) groupStarts.push(i);
  });
  const totalGroups = groupStarts.length;
  if (totalGroups === 0) return { rows: [], page: 1, totalPages: 1, hasPrev: false, hasNext: false };
  const totalPages = Math.max(1, Math.ceil(totalGroups / PAGE_SIZE));
  const safePage = Math.max(1, Math.min(page, totalPages));
  const startGroupIdx = (safePage - 1) * PAGE_SIZE;
  const endGroupIdx = Math.min(startGroupIdx + PAGE_SIZE, totalGroups);
  const startRow = groupStarts[startGroupIdx];
  const endRow = endGroupIdx < totalGroups ? groupStarts[endGroupIdx] : total;
  return { rows: results.slice(startRow, endRow), page: safePage, totalPages, hasPrev: safePage > 1, hasNext: safePage < totalPages };
}

module.exports = { paginateResults, paginateGroupedResultsByRows };
