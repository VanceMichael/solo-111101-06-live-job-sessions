# 直播带岗活动的会话与回放采集

离线采集工程使用 Python 标准库和 SQLite。adapter 读取本项目的真实来源格式，sources 提供可重复响应序列，capture 原子归档解析后的来源记录。已有 --capture 只记录原始观察，不作业务投影；--run 工作流在同一状态库上重建活动、场次版本与回放关系，并输出可解释的当前投影与变更脉络。

## 环境与准备

推荐 Python 3.12.12，支持 Python 3.11+；requirements.lock 为空表示没有第三方包，运行、测试和验收均不下载数据或依赖。Dockerfile 提供同一 Linux 环境。需要容器时先在正式运行之外准备：

```sh
docker build -t q006-live-job-sessions:initial .
docker run --rm q006-live-job-sessions:initial
```

## 基线测试与原始采集

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q collector_06 tests acceptance
scratch=$(mktemp -d)
python3 -m collector_06.cli --database "$scratch/raw.sqlite3" --capture fixtures --route /schedule
python3 -m collector_06.cli --database "$scratch/raw.sqlite3" --capture fixtures --route /schedule
python3 -m collector_06.cli --run fixtures/rescheduled.json --database collector_06.sqlite3 --output rescheduled.json
```

第一次 decoded=1、inserted=1，重抓同一观测 inserted=0。临时库不提交。tests 是已实现采集基础能力的回归测试，不代表扩展功能完成。

## 来源协议

ICS UID 标识场次，SEQUENCE 标识修订，RECURRENCE-ID 若出现则与 UID 组成场次实例身份。回放由 X-TYPE:REPLAY 标识，X-SESSION、X-SESSION-SEQUENCE 明确指向拍摄时的场次版本，不能改挂到当前改期版本。X-ACTIVITY 仅表示所属活动，不足以推断场次；只有该字段的回放进入 unresolved/missing_session_revision。

较低 SEQUENCE 迟到仅补历史，不倒退当前日程；相同序号不同日程隔离为 conflict。取消只改变场次状态，不能删除已归档回放。对每条关联输出用于决策的字段证据。matching=explicit_only 必须遵守；可另外支持配置匹配规则，但时间邻近不能默认合并。按场次版本保存日程，再按当前投影查询。

FixtureTransport 每次进程创建从 routes.json 首条响应开始，同一路由多次 request 依次推进，最后一条可重复；calls 保存请求证据。源数据相对 fixture_root 读取，不能越出该目录。所有响应是本地合成数据，不发出公网请求。单场景 JSON 的 fixture_root 相对当前工作目录解析；在仓库根目录执行即可。run_id 是采集批次标识，name 仅作展示标签。

## 扩展接口与可观察输出

`python3 -m collector_06.cli --run 场景文件 --database 状态库 --output 结果文件`

场景文件含 name、run_id、sources（有序根来源）、options、fixture_root；name 只作日志标签。expected 是公开最小预期，产品不应读取它。所有来源和选项应独立支持；四个场景不能替代全部边界测试。业务结果投影：sessions（uid、sequence、start、state）、replays（uid、session、session_sequence、关联证据）、unresolved、history。数组顺序不影响检查，公开预期数组是子集，但空数组要求确实为空。内部 SQLite 表、架构和调度实现由开发者选择。每个场景在同一状态库上用两个独立进程执行，以检查重启和重复输入；业务投影应保持一致，notifications 为数量时表示此次新建通知数，第二次应为0，为数组时表示持久化待处理通知清单。

### 场景文件格式

```json
{
  "name": "rescheduled",
  "run_id": "run-rescheduled",
  "sources": ["/schedule", "/move", "/replay"],
  "options": {"matching": "explicit_only"},
  "fixture_root": "fixtures"
}
```

`run_id` 标识采集批次；`sources` 按数组顺序经 FixtureTransport 拉取，同一路由在进程内按 routes.json 依次推进。状态库中所有版本、回放和通知都带 run_id 与来源路由，重复执行同一场景时按来源指纹幂等跳过。`portal_database` 等额外字段会被忽略，产品不依赖任何场景名称分支。

### 身份、版本与投影规则

* 场次身份 = `UID`（出现 `RECURRENCE-ID` 时为 UID + RECURRENCE-ID）；每个 `SEQUENCE` 作为独立版本写入 `session_versions`，再按身份取最高序号版本投影当前日程。
* 迟到的较低 SEQUENCE 只补入历史（history 标记 `late_archive`），不覆盖也不倒退当前投影；相同序号但日程内容不同（UID/RECURRENCE-ID/SEQUENCE 之外字段哈希不同）不会互相覆盖，隔离进 `session_conflicts`，history 与通知记为 `conflict`，当前投影保持先建立的版本。
* `STATUS:CANCELLED` 版本只把当前状态改为 `cancelled`；取消事件不带 DTSTART 时，投影 start 沿该身份历史版本回填。取消不删除任何已归档回放，回放表只追加。
* 回放由 `X-TYPE:REPLAY` 识别。同时带 `X-SESSION` 与 `X-SESSION-SEQUENCE` 时按显式锚点关联到拍摄时的场次版本（即使当前日程已改期也不改挂），证据含 `explicit_session_revision` 决策、锚点字段和 `target_version_observed`；指向尚未观测到的版本时仍保留显式关联并在证据中标注。缺少任一字段（含只有 `X-ACTIVITY`）的回放进入 `unresolved`，reason 固定为 `missing_session_revision`，证据列出缺失字段与全部相关来源字段。
* 每条关联的 `evidence` 至少包含来源路由、参与决策的原始字段、匹配模式与决策名；history 条目携带 ICS 字段与内容哈希，conflict 同时携带已建立版本与冲突版本的哈希。

### 可配置匹配

`options.matching` 支持：

* `explicit_only`（默认且四个内置场景均使用）：只承认 X-SESSION/X-SESSION-SEQUENCE 显式锚点，时间邻近永远不参与合并；该模式下携带 `match_rules` 会直接报错。
* `configurable`：必须显式给出非空 `options.match_rules`，可选规则为 `explicit_session_revision`（始终最先执行）、`activity_single_session`（仅当 X-ACTIVITY 在当前日程中唯一命中一个场次身份时才补关联，多候选保持 unresolved）、`time_proximity`（需回放带 X-RECORDED-AT，选最近且无并列的场次，并列或无候选保持 unresolved）。后两条规则只有在被明确列入 match_rules 时才生效，证据中以 `configured_rule:<规则名>` 标注，绝不默认开启。

### 输出结构

`sessions` 为当前投影（uid、可选 recurrence_id、sequence、start、state=scheduled/cancelled）；`replays` 为已归属回放（uid、session、session_sequence、evidence，可选 recurrence_id/url）；`unresolved` 为无法确定归属的回放（uid、reason、evidence）；`history` 按身份与序号列出全部版本脉络（kind 为 scheduled/rescheduled/cancelled/late_archive/revised/conflict）；`notifications` 为本进程新建通知数（改期、取消、冲突、无法归属回放各有稳定通知键，重跑为 0，通知清单持久化在状态库）。

### 边界回归

`tests/test_workflow.py` 不按四个样本名称分支，而是用临时 ICS 夹具数据驱动，额外覆盖：相同 SEQUENCE 不同日程的 conflict 隔离、重复取消幂等、RECURRENCE-ID 实例身份与改期后回放仍锚定旧版本、低序号迟到不倒退当前日程、活动页重新拉取不丢已归档回放、显式锚点指向未知版本、configurable 规则的启用/多候选/并列，以及独立子进程重启后投影一致且通知归零。

## 验收与正式展示

```sh
python3 acceptance/check.py --contracts-only
python3 acceptance/check.py
```

contracts-only 仅检查材料结构，不能当成功能验收。第二条在初始版本会因尚无 --run 而失败，这是待开发功能的验收入口，不属于基线测试。完成开发后应四个场景均 PASS；每个场景独立临时目录，执行两次、每次进程超时5秒，八次总预算40秒，无固定 sleep。正式展示从干净状态执行基线测试和第二条命令，预算小于90秒；依赖和镜像预先准备，不能用旧日志代替运行。请另加参数变化及边界回归，不修改预期来迁就实现。
