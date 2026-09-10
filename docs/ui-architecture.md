# UI 与模块维护指南

## 运行与边界

仓库只维护 Electron 桌面版；`static/frontend/` 和内置 Express 服务都属于桌面运行时，不应随旧 Web 实现一起删除。旧 Flask/Jinja 与 PyInstaller 源码已移除，历史记录仅供追溯，不再是构建或对拍依赖。Python 只用于 pytest/Trellis 开发工具。

前端由本地 Express 服务直接提供原生 `.mjs` 文件，`app.js` 只调用 `startApp()`；不要重新引入页面全局变量、运行时 CDN 或打包步骤。Electron 22 / Chromium 108 是兼容基线。`workflow.mjs` 为 Node 测试提供同一份纯逻辑，不维护第二份实现。

`createApp({ dataDir, frontendDir })` 的参数、返回 `{ app, libraries, sessions }` 及原有时间辅助导出保留。路由只转换 HTTP 输入与输出；服务通过显式注入的会话库、资料库和会话访问器工作，`core` 筛选/评分以及 Excel 工作簿生成仍是业务事实来源。重构接口层不改变默认筛选规则。

## 前端职责

- `core`：应用状态、草稿存储、网络传输、导航和生命周期。过期与临时错误继续严格区分，异步响应先校验批次/路由/请求序号。
- `domain`：功能与字段定义、纯表单逻辑、本地日期工具。保留 v2 草稿键、明确的空数组与 false，不以 truthy 默认替换用户的选择。
- `pages`：组合公共控件与页面内容，通过注入的 actions 调用跨页面行为。纯视图只返回标记，不发请求。
- `ui`：控件只接收元素、选项及回调，不读取业务全局状态。`ControlHost` 统一挂载并销毁控件；`Scope` 管理监听、定时器和清理函数。

替换页面 DOM 前先保存表单草稿，再销毁控件与页面作用域；导航前关闭弹层并取消旧读取。不能在滚动、搜索或选中状态变化时全量重建页面，以免丢失焦点与输入。

## 控件契约

日期时间显示在 text input 上，用 `data-date-kind` 标记类型。组件在确认时发出原生 `input`/`change` 事件，表单捕获将显示格式转换为既有 API 值：日期 `YYYY-MM-DD`、时间 `HH:mm`、日期时间 `YYYY-MM-DDTHH:mm`。转换失败保留原始草稿并提示，不能自动滚到其他日期；不经 UTC 转换。打开弹层只初始化候选值，直到用户确认才提交。

选择器也通过 input/change 通知表单。单选提交后关闭，多选即时更新并保留明确空选择；搜索中 Enter 不应触发外层表单提交。原生 select 可保留为隐藏值载体，但展示、搜索与键盘操作由同一公共选择器实现。

`OverlayManager` 统一定位到 document body，管理层级、外部点击、Esc、确认框焦点限制和退出时的焦点恢复。切页或控件销毁必须关闭对应弹层，不能遗留遮罩或 inert 状态。

## 视觉规范

样式按 tokens → base → layout → components → pages → pickers 顺序载入。新控件优先复用变量和组件类，不追加与已有规则冲突的页面补丁。标准控件高 44px，紧凑按钮 36px；辅助文字不小于 12px。复用中文系统字体、浅色内容、绿色主操作和低对比边框，警告/错误使用语义色而非装饰色。

表单为可滚动内容与独立底部操作区，`mountFormPanel` 依据视口及上方反馈区域调整可用高度。表格横向滚动限于容器，弹层不能被该容器裁切。尊重 reduced-motion，所有必要信息不能只用颜色表达。

## 打包准备

`desktop/package.json` 中的 `build:icon` 从源码生成图标；`prestart`、`predist`、`predist:x64` 和 `predist:ia32` 统一调用它。`electron-builder.yml` 将生成的 `build/icon.ico` 收入应用，内置界面通过 `extraResources` 复制。不要提交生成图标、安装包或 `node_modules`，也不要以本机已有资产代替新检出代码的构建验证。

## 测试与验收

`pytest tests -q` 同时运行原筛选回归、公共控件测试及桌面仓库/图标生成回归；`npm --prefix desktop run test:api` 单独运行合成数据 API 场景。测试服务才开放 `/__test__/controls`，正式应用不存在这个路由。控件页使用合成文字/数据，截图和导出写入 pytest 临时目录，绝不写入用户交通数据目录。

变更组件时补充取消/清空/非法输入、焦点、禁用、销毁与缩放测试；变更接口服务时验证五模式结果、分功能快照、旧接口和导出与之前一致。所有用例必须主动清理自己的连接与窗口，不能用增加超时替代正确的生命周期管理。
