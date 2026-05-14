# 车辆筛查工具（vehicle_screening）

基于 Flask 的本地 Web 工具，用于批量分析车辆通行 Excel 数据，支持卡口配对筛查、频繁车辆筛查、重点人员车辆筛查，并导出预警结果。

## 1. 主要功能

- 上传 `.xls/.xlsx` 通行数据并自动解析关键字段
- 卡口配对筛查（两个卡口 + 最大时间间隔）
- 绝对时间卡口筛查（前置时刻前经过 A 卡口 + 后置时刻后离开 B 卡口）
- 频繁出现车辆筛查（多卡口 + 时间窗 + 最低出现次数）
- 重点人员车辆筛查（重点车辆库 + 卡口范围 + 时间窗）
- 结果导出为 Excel（含风险分级和汇总）
- 本地卡口库与重点人员库维护

## 2. 项目结构

- `app.py`：主程序（路由、解析、筛查、导出）
- `templates/`：页面模板（上传、参数确认、结果展示、卡口库/重点库管理）
- `static/`：静态资源目录
- `uploads/`：运行时上传缓存和会话数据（临时目录，不要提交）
- `checkpoint_library.json`：卡口库
- `keyperson_library.json`：重点人员库
- `build_exe.ps1`：Windows 打包脚本

## 3. 环境要求

- Python `3.8 - 3.12`
- Windows PowerShell（用于执行打包脚本）

依赖：

```powershell
pip install flask pandas xlrd openpyxl
```

## 4. 本地运行（源码）

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install flask pandas xlrd openpyxl
python app.py
```

打开浏览器访问：`http://127.0.0.1:11000`

## 5. 数据格式要求

- 支持文件类型：`.xls`、`.xlsx`
- 必需字段（可自动匹配常见别名）：`车牌号`、`抓拍时间`、`抓拍地点`
- 可选字段：`号牌种类/号牌类型`
- 上传大小限制：`500 MB`（`MAX_CONTENT_LENGTH`）

## 6. 打包 EXE（Windows）

### 6.1 常规打包

```powershell
.\build_exe.ps1
```

已安装 PyInstaller 时可跳过安装步骤：

```powershell
.\build_exe.ps1 -SkipDepsInstall
```

输出文件：`dist\vehicle_screening.exe`

### 6.2 Win7 兼容打包（重要）

如果目标机器是 Windows 7，必须使用 Python `3.8.x` 打包：

```powershell
py -3.8 -m venv .venv38
.\.venv38\Scripts\python.exe -m pip install -U pip
.\.venv38\Scripts\python.exe -m pip install flask pandas xlrd openpyxl
.\build_exe.ps1 -Win7Compatible -PythonExe .\.venv38\Scripts\python.exe
```

说明：

- `-Win7Compatible` 模式会固定 `pyinstaller==5.13.2`
- 若不是 Python 3.8，会被脚本直接拦截并提示
- 不要通过手工下载 DLL 方式修复 `api-ms-win-core-path-l1-1-0.dll`

### 6.3 一键同时生成 Win7 32/64 位安装包

前提：

- 已准备 Python 3.8 x64 与 x86 两套环境（例如 `.venv38` 与 `.venv38_x86`）
- 已安装 Inno Setup 6（默认 `ISCC.exe` 路径可自动识别）

执行：

```powershell
.\build_win7_dual_installers.ps1 -SkipDepsInstall
```

若 Python 环境路径不同，可显式指定：

```powershell
.\build_win7_dual_installers.ps1 `
  -Python64Exe .\.venv38\Scripts\python.exe `
  -Python32Exe .\.venv38_x86\Scripts\python.exe `
  -SkipDepsInstall
```

若 Inno Setup 不在默认路径，可指定：

```powershell
.\build_win7_dual_installers.ps1 `
  -InnoSetupCompiler "C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
```

若只需要生成 32/64 位 Win7 兼容 EXE，不需要安装包，可跳过 Inno Setup：

```powershell
.\build_win7_dual_installers.ps1 -SkipDepsInstall -SkipInstaller
```

输出目录：

- `dist\win7_dual\<时间戳>\installer\vehicle_screening_win7_x64_setup.exe`
- `dist\win7_dual\<时间戳>\installer\vehicle_screening_win7_x86_setup.exe`
- `dist\win7_dual\<时间戳>\bin\vehicle_screening_win7_x64.exe`
- `dist\win7_dual\<时间戳>\bin\vehicle_screening_win7_x86.exe`
- `dist\win7_dual\<时间戳>\SHA256SUMS.txt`

## 7. 使用流程

1. 首页上传 Excel
2. 在参数页选择筛查模式和条件
3. 查看结果并下载导出文件

## 8. 常见问题

- 打包后 EXE 无法覆盖：先关闭正在运行的 `vehicle_screening.exe`，再重新打包
- 浏览器未自动打开：手动访问 `http://127.0.0.1:11000`
- 解析失败：检查表头是否包含车牌、时间、地点对应列，且时间列可被识别为日期时间
- 打包报错 `PermissionError: [WinError 5] 拒绝访问 ... dist\vehicle_screening.exe`：
  这是旧版 EXE 仍在运行导致文件被占用。先执行：

```powershell
Get-Process vehicle_screening -ErrorAction SilentlyContinue | Stop-Process -Force
```

  然后重新打包：

```powershell
.\build_exe.ps1 -Win7Compatible -PythonExe .\.venv38\Scripts\python.exe -SkipDepsInstall
```

- 运行时报错“此文件的版本与正在运行的 Windows 版本不兼容（x86/x64）”：
  通常是目标机器系统位数与 EXE 位数不一致（例如 32 位 Win7 运行了 64 位 EXE）。
  可先在目标机确认系统位数：

```powershell
wmic os get osarchitecture
```

  如果目标机是 32 位系统，请用 Python 3.8 x86 重新打包：

```powershell
# 按实际安装路径替换 C:\Python38-32\python.exe
C:\Python38-32\python.exe -m venv .venv38_x86
.\.venv38_x86\Scripts\python.exe -m pip install flask pandas xlrd openpyxl pyinstaller==5.13.2
.\build_exe.ps1 -Win7Compatible -PythonExe .\.venv38_x86\Scripts\python.exe -SkipDepsInstall
```

## 9. 安全与数据

- `app.py` 中 `SECRET_KEY` 仅适合本地使用，部署前请修改
- 不要提交真实业务数据、导出结果和 `uploads/` 目录内容
- 建议将 EXE 放在有写权限目录运行（避免 `Program Files`）
