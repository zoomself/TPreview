# A 股辅助工具（Baostock + GitHub Actions + Pages）

使用 [Baostock](http://baostock.com/) 增量同步 A 股日 K 数据到仓库内的 CSV，并在每次同步后生成一份静态 HTML 分析报告，通过 **GitHub Pages** 在线浏览。定时任务由 **GitHub Actions** 在收盘后触发（可按需调整）。

## 功能概览

1. **增量下载**：按 `config.yaml` 的日期区间拉取日 K；本地已有 CSV 时比对最后一根 K 线的日期，从**下一交易日**继续拉取，跳过已完成股票。
2. **元数据与 ST 标记**：写入 `data/meta/stocks.csv`。`is_st` 通过对 `code_name` 的启发式规则识别（如 `*ST`、`ST`、`S*ST`、`退市` 等），**不是**交易所官方字段，可能存在误判或漏判。
3. **定时更新**：Workflow 默认每日在北京时间约 **17:30** 运行（UTC `30 9 * * *`），亦支持手动 **Run workflow**。若配置的结束日等于「上海当天」且 Baostock 判断**非交易日**，仍会刷新 `stocks.csv`，但跳过日 K 拉取（避免无效请求）。
4. **分析报告**：列出「截至本地最新一根 K」时，**连续下跌天数 ≥ 配置阈值**（默认 5）的股票；字段包括名称、代码、ST 标记、最新收盘价、连跌天数、连跌总跌幅、近 N 个交易日（默认 20）总跌幅；按连跌天数降序，同天数下按连跌总跌幅从大到小（跌幅更深优先）。每行可用 **「展开每日跌幅」** 查看 streak 内逐日涨跌幅。
5. **GitHub Pages**：将 `docs/index.html` 作为站点入口发布即可。

## 快速开始（本地）

需要 **Python 3.10+**（推荐 3.11，与 Actions 一致）。

```bash
cd TPreview
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

可选：复制配置（若不复制，则默认读取 `config.example.yaml`）。

```bash
cp config.example.yaml config.yaml
```

执行同步与报告：

```bash
python -m src.sync
python -m src.report
```

- 同步结果：`data/daily/*.csv`、`data/meta/stocks.csv`
- 报告输出：`docs/index.html`

### 可选环境变量（调试 / 冒烟）

| 变量 | 含义 |
|------|------|
| `CONFIG_PATH` | 指定其它 YAML 配置文件路径 |
| `SYNC_START_DATE` | 覆盖配置中的 `start_date`（`YYYY-MM-DD`） |
| `SYNC_END_DATE` | 覆盖配置中的 `end_date`（`YYYY-MM-DD` 或 `today`） |
| `SYNC_MAX_STOCKS` | 仅同步前 N 只股票（用于本地快速验证） |

## `config.yaml` 说明

示例见 [config.example.yaml](config.example.yaml)。

| 字段 | 说明 |
|------|------|
| `start_date` | 首次下载起始日（含），`YYYY-MM-DD` |
| `end_date` | 结束日（含）；填 `today` 表示按 **Asia/Shanghai** 当天日期 |
| `adjustflag` | Baostock 复权：`3` 不复权，`2` 前复权，`1` 后复权 |
| `min_streak_days` | 报告筛选：最新一根 K 往回统计的**连续下跌天数**阈值（收盘价严格 `close[t] < close[t-1]`） |
| `lookback_trading_days` | 「近一月」口径：向前多少个**交易日**计算累计涨跌幅 |
| `request_sleep_sec` | 每只股票请求后的休眠秒数，略降频避免被限流 |
| `data_dir` / `daily_subdir` / `meta_subdir` | 数据目录布局 |
| `docs_dir` | 报告输出目录（默认 `docs`） |
| `template_path` | Jinja2 模板路径 |

## 指标定义（与页面一致）

- **连续下跌**：相邻交易日收盘价满足 `close[t] < close[t-1]`；平盘或上涨则 streak 断开。
- **连跌天数**：以**最新交易日**为 streak 末端，向前连续满足上述条件的**天数（步数）**。
- **连跌总跌幅**：相对 **streak 起点的前一交易日收盘价** 到 streak 末收盘的涨跌幅：\((C_{\text{end}}/C_{\text{ref}} - 1)\times 100\%\)。
- **近 N 日总跌幅**：\(C_{\text{last}}/C_{\text{last}-N}-1\)（需至少 N+1 根 K）；不足历史则页面显示为 `—`。
- **排序**：连跌天数降序；若相同，连跌总跌幅更小（跌得更多）优先。

## 推送到 GitHub 后的设置

### 1. 启用 Actions

仓库 **Settings → Actions → General**：允许运行 workflows（默认公开库一般已开启）。

### 2. 允许写入内容（用于 bot 提交 `data/` 与 `docs/`）

本仓库 workflow 已声明：

```yaml
permissions:
  contents: write
```

若组织策略限制默认 `GITHUB_TOKEN` 写权限，请在 **Settings → Actions → General → Workflow permissions** 中选择 **Read and write permissions**（或按组织策略为 bot 配置 PAT，属进阶场景）。

### 3. 开启 GitHub Pages

**Settings → Pages → Build and deployment**

- **Source**：`Deploy from a branch`
- **Branch**：`main`（或你的默认分支）**/`docs`** 文件夹  
- 保存后站点地址一般为：`https://<owner>.github.io/<repo>/`

首次若 `docs/index.html` 已存在（本仓库已带占位页），启用后即可访问；完整数据需等待 **Sync Baostock and publish report** workflow 成功运行。

### 4. 手动运行一次

**Actions** 选项卡 → **Sync Baostock and publish report** → **Run workflow**。

日志中可查看同步进度与错误。提交信息含 `[skip ci]`，避免由 bot 推送再次触发无限循环。

## Baostock 与版本

服务端会校验客户端版本。若登录报版本不匹配，请升级：

```bash
pip install -U baostock
```

本仓库在 [requirements.txt](requirements.txt) 中钉了已验证版本；若官方再次升级要求，请按报错提示同步修改 `requirements.txt`。

## 体积、性能与时限

- 全市场、长历史 CSV **体积可能很大**，克隆与 Pages 无关但会影响仓库体验；建议先用较短 `start_date` 验证，再逐步拉长。
- **GitHub Actions** 单 job 最长 360 分钟（本 workflow 已设 `timeout-minutes: 360`）；全量首次同步仍可能较慢，可先在本地跑完首批再推送，使 CI 以**增量**为主。
- 若未来接近 GitHub 单文件建议上限，可考虑 [Git LFS](https://git-lfs.github.com/) 或将冷数据外置（超出本模板范围）。

## 免责声明

本项目仅供学习与技术研究使用。证券行情来自第三方（Baostock），**不保证**实时性、完整性与准确性；报告中的 ST 标记为名称规则推断。**不构成投资建议**。使用与传播数据时请遵守 Baostock 及数据源的相关条款。

## 目录结构

```
.github/workflows/sync-and-report.yml   # 定时同步 + 生成报告 + 提交
config.example.yaml                     # 默认配置模板
data/daily/                             # 各股日 K CSV
data/meta/stocks.csv                    # 证券基础信息 + is_st（同步生成）
docs/index.html                         # Pages 入口（由脚本生成）
templates/report.html.j2                # 报告模板
src/config.py                           # 配置加载
src/sync.py                             # 增量同步
src/report.py                           # 报告生成
requirements.txt
README.md
```

## 许可证

未默认附带许可证文件；如需开源发布请自行添加 `LICENSE`。
