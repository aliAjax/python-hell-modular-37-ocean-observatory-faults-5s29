# 海底观测网设备故障管理

这是一个只使用Python标准库和SQLite的模块化原型项目，默认端口为`8337`。领域对象包括站点、资产、链路、遥测、故障事件、恢复动作、出海任务和数据缺口。`app.py`只负责参数解析、依赖组装和服务生命周期，业务状态机与约束集中在`src/rules.py`。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、领域异常、身份解析和实体数据结构。
- `src/rules.py`：状态机、角色权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务、乐观锁、审计和幂等键。
- `src/service.py`：用例编排、离线记录合并、版本控制和审计写入。
- `src/http_api.py`：HTTP路由、JSON解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8337
```

服务启动时自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。初始化服务不需要单独命令，首次启动即可访问：

```bash
curl http://127.0.0.1:8337/health
```

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。

身份通过`X-User-Id`和`X-Role`请求头传入，角色和动作权限由规则引擎校验。

## 链路降级与遥测积压

链路有`capacity`（排队容量）和可选的`degraded_capacity`（降级带宽，缺省为0，不能大于capacity）。遥测通过链路入库入口推送，系统按链路当前状态决定去向：

- `up` / `backup_active`：全部直接送达（telemetry=current）。
- `degraded`：先按降级带宽送达，其余进入该链路的队列（telemetry=queued）；队列达到capacity后，剩余样本不丢弃事实，而是合并写入一个`source=link_overflow`的数据缺口（同一链路/资产/指标的连续溢出只扩展同一个缺口窗口和`missing_samples`计数）。
- `down`：所有样本都记为溢出缺口。
- 链路`restore`或`activate_backup`后，队列按观测时间顺序自动冲刷为current；缺口仍然保留，需要后续插值或补传处理。

```
POST /api/links/<link_id>/telemetry
{"samples":[{"asset_id":"...","metric":"pressure","value":1.2,"observed_at":"..."}]}
```

## 插值依据与修订号级联

缺口`fill`必须提供`source`（interpolation/backfill/field_record）和`basis_revision`，两者与`fill_history`一起永久记录在缺口上；`refill`要求新依据修订号严格大于旧依据。恢复动作和出海任务可以在创建或重算时带`basis_gap_id`，系统会快照缺口的依据（`basis_revision`/`basis_source`），故障诊断`diagnose`也可以带`basis_gap_id`记录结论依据。

当更高修订号的真实数据（迟到遥测`revise`或补传记录）落入已插值缺口的时间窗，系统自动级联：

- 缺口回到`open`并标记`stale_reason`；
- 依据该缺口、仍处于proposed/approved/running的恢复动作进入`invalidated`，需在缺口重新填补后`recompute`；
- planned/approved的出海任务进入`invalidated`，需`replan`；已经出海（underway/completed）的任务不能撤回，只标记`basis_stale`提醒现场；
- 诊断结论回到`diagnosing`并标记`conclusion_stale=true`；
- 缺口依据失效、恢复动作未完成或出海任务未取消/重排时，故障事件不能`resolve`。

## 补传断点重试与去重

补传会话（kind=`backfill`，初始active）记录`source_id`、`asset_id`、`metric`和已提交的`cursor_seq`：

```
POST /api/backfills
{"source_id":"vessel-upload","asset_id":"...","metric":"pressure"}

POST /api/entities/<backfill_id>/actions
{"action":"push_batch","data":{"records":[
  {"seq":1,"record_id":"r1","value":1.1,"observed_at":"..."}, ...]}}
```

每条记录必须带连续的`seq`和稳定的`record_id`。记录按(source_id, record_id)生成稳定ID，重复推送只入库一次；seq必须紧接当前checkpoint，遇到断流时已提交记录保持落库，接口返回502并给出`resume_from_seq`，客户端从该点重发即可，已入库记录在`duplicate`中返回、不会重复。会话完成后可`complete`或`abort`。

## 并发编辑

缺口（gap）和出海任务（mission）的所有动作都必须带`expected_version`（读取实体时的version）。两人同时基于同一版本修改时，先提交者成功（version+1），后提交者在任何规则校验之前收到409 `version conflict`，必须重新读取再决定是否继续。

## 核心流程

建立站点、资产和链路后，遥测通过`/api/links/<id>/telemetry`入库；链路降级时超出降级带宽的遥测排队，队列满后保留溢出缺口。缺口经插值（记录来源和修订号）或补传填补，恢复动作、出海任务和故障诊断结论基于缺口依据。补传产生更高修订号真实数据后，过期依据级联失效，需要重算动作、重排任务后才能关闭事件。遥测`revise`动作只接受更高修订号，用于处理迟到数据。

## 规则重点

- 同一资产和故障类型不能同时有多个活动事件。
- 恢复动作按`dedupe_key`防止重复执行（已invalidated的动作不占dedupe键，允许重算后新建）。
- 缺口插值必须记录来源和`basis_revision`，依据被更高修订号数据推翻时级联失效。
- gap和mission的动作强制乐观锁（`expected_version`），并发修改只有一方成功。
- 事件解决前恢复动作、数据缺口、出海任务和受影响资产必须达到可关闭状态，且不能存在stale依据。
- 补传记录按稳定身份去重，checkpoint保证失败后从断点续传。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

项目使用请求头模拟身份、SQLite单机持久化和简化状态机，适合原型演示和流程验证，不替代行业正式系统、设备控制系统或现场安全规程。
