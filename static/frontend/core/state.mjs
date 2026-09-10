export function createState(store) {
  return {
    route: { page: "home", hash: "#/home" },
    dataId: store.activeId(),
    home: null,
    summary: null,
    workspace: null,
    reviews: {},
    resultCache: {},
    detailCache: {},
    library: null,
    files: new Map(),
    changingData: false,
    busy: false,
    loading: false,
    ticket: 0,
    controller: null,
    libraryReturn: "#/home",
    downloadSequence: 0,
    warnedStorage: false,
  };
}
