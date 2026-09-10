export const FEATURES = {
  pair: {
    title: "查两处之间的车辆",
    legacy: "卡口配对",
    description: "找出先经过一处、再经过另一处的车辆。",
    prepare: "添加文件后，选择先后经过的两个地点即可查找。",
    icon: "route",
  },
  frequent: {
    title: "查经常出现的车辆",
    legacy: "频繁出现",
    description: "看看哪些车辆在指定地点反复出现。",
    prepare: "添加文件后，选择查询地点和至少出现几次。",
    icon: "repeat",
  },
  timed_cross: {
    title: "查指定时间前后的车辆",
    legacy: "绝对时间卡口",
    description: "查某时刻前经过一处、另一时刻后经过另一处的车辆。",
    prepare: "添加文件后，分别选择两个地点和对应的时间。",
    icon: "clock",
  },
  keyperson: {
    title: "查名单中的车辆",
    legacy: "重点人车辆",
    description: "将名单中的车辆与通行记录进行比对。",
    prepare: "添加通行记录和名单，即可查出名单车辆的通行情况。",
    icon: "list",
  },
  night_stay: {
    title: "查夜间停留的车辆",
    legacy: "夜间停留",
    description: "找出夜间进入后，停留较长时间再驶出的车辆。",
    prepare: "添加文件后，选择进入、驶出的地点和停留时长。",
    icon: "moon",
  },
};
export const CATEGORY_LABELS = {
  matches: "符合停留条件",
  entries: "只有进入记录",
  exits: "只有驶出记录",
};
