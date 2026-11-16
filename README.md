# 化工批次工况分群

KMeans 聚类的真实业务分析小型项目。完整模拟客户、痛点、上下游、算法公式与一天工作剧本见 [BUSINESS.md](BUSINESS.md)。

## 模块与业务流程
数据接入（JSON/CSV、字段验证、批次指纹）→ 参数化分析（异步 Python worker）→ 结果复核（指标、图形、明细、解释、批准）→ 定时分发（本地收件箱、CSV/JSON、SHA256）；另有业务总览、方案编排和审计轨迹。
四个贯穿全栈的实体：Dataset 数据批次、Job 分析任务、AnalysisPlan 分析方案与 PlanRun 方案运行、Report 分发计划；辅助 User、Audit、Delivery。

### 方案编排（分叉汇合）
- 方案是由 `cluster`（分群）与 `merge`（汇合）步骤组成的 DAG，每个方案绑定一个数据批次。根步骤读取整批数据，下游 `cluster` 步骤用 `select` 按上游族群编号取该族群的**原始批次**继续分析；`merge` 步骤把各支线选取的原始批次合并，**同一批次号只纳入一次**。
- 保存与运行前做 DAG 校验：重复/非法步骤键、悬空上游、按族群选取汇合步骤、根步骤有多个上游、汇合步骤少于两个上游、循环依赖均返回 422 拒绝执行。
- 每次运行固定方案版本（`plan_version`）、方案定义快照与数据指纹（`dataset_checksum`），并预先建立全部步骤记录。各步只在上游全部成功后才运行（其他步骤 `pending`），任一步失败即把其下游标记 `blocked`，失败与阻断都会让运行以 `failed` 结束。
- 输入指纹沿依赖链计算：方案参数、上游步骤、数据批次任一变化都会使该步及其下游指纹改变。`POST /plans/{id}/impact` 可预演改动影响：指纹未变的步骤沿用（`reuse`），变化的步骤及下游重算（`recompute`）。
- 新运行建立时旧运行标记 `superseded`：旧结果不再可批准、不再进入简报；若旧计划已挂简报，worker 会在下一次到期时暂停该计划。旧运行的已完成步骤仍保留作为可复用结果。
- worker 用条件更新原子领取步骤（`attempt` 递增），只有仍持有该次领取的执行才能提交结果；被取代、重置或重复消费导致的迟到结果写入 `plan.step.late_rejected` 审计并丢弃。
- 中断恢复：worker 启动时把 `running` 步骤立即重置为排队，周期扫描对超过 90 秒未完成的步骤同样重排；重排沿用步骤已固定的输入批次与参数，不新建步骤、不重复纳入批次，因此派发半途或汇总前中断都能继续。并发重试通过唯一约束 `(run_id, step_key)` 与原子领取避免重复创建。
- 逐步溯源：`GET /runs/{id}` 返回每步的输入批次数、去重后批次数、输入批次号、每个上游族群提供了多少批/新纳入多少批/去重跳过多少批、是否沿用了哪次运行，以及失败原因与影响汇总。
- 方案运行全部步骤成功后可获批准；`Report` 可绑定 `run_id` 生成固定快照简报，来源失效时计划自动暂停而非输出旧结论。原有的单次 Job 分析、批准与分发链路保持不变。

页面：`#/plans` 编排与运行记录，`#/plans/{planId}/{runId}` 查看逐步来源与影响，`#/results/run{runId}` 在结果复核页查看方案结论。

## 快速启动
```bash
cp .env.example .env
docker compose up -d --build --wait
docker compose exec -T backend python -m scripts.seed_demo
```
访问 http://localhost:18209；后端文档 http://localhost:19209/docs；数据库宿主端口 17709。
`SEED_DEMO=true` 自动幂等 seed；不覆盖用户修改。手动 seed 命令可重复执行。

## 演示账号与权限
|账号|密码|真实权限|
|---|---|---|
|admin@demo.local|Demo123!|接入、执行、批准、分发、暂停、读取与下载|
|analyst@demo.local|Demo123!|接入、执行、编排方案与运行、读取与下载；不能批准或安排分发|
|viewer@demo.local|Demo123!|只读与下载|

## 技术与本地开发
Python 3.12 / FastAPI / SQLAlchemy / NumPy / SciPy / scikit-learn；MySQL 8.0；Redis 7；Nginx；原生 HTML CSS JavaScript ES modules（用户指定）。后端所有校验、持久化和算法在服务端。无前端打包步骤。
后端：安装 backend/requirements.txt，设置 DATABASE_URL、REDIS_URL、JWT_SECRET、ARTIFACT_DIR 后在 backend/ 执行 `python -m migrations.initial`、`python -m scripts.seed_demo`、`uvicorn src.main:app`，另开 `python -m src.worker`。前端使用带 /api 反向代理的 Nginx，直接打开 index.html 不支持 API。

## 目录与工程约定
backend/src/{models,schemas,services,routes,controllers,middlewares,constants,config,utils}；backend/scripts/seed_demo.py；backend/migrations/initial.py。
frontend/src/{api,stores,types,components/common,hooks,pages,router,utils,constants}。
Role 枚举：admin/analyst/viewer；JobStatus：queued/running/completed/failed；PlanRunStatus 另含 superseded；StepStatus：pending/queued/running/completed/failed/blocked/invalidated。后端 backend/src/constants/enums.py，前端 frontend/src/constants/enums.js；路由权限在 middlewares/auth.py；页面按相同角色控制操作入口。

## 环境变量与 Docker
COMPOSE_PROJECT_NAME=batch-clusters；FRONTEND_PORT=18209；BACKEND_PORT=19209；DB_PORT=17709；DB_PASSWORD、JWT_SECRET 见 .env.example；SEED_DEMO 默认 true。共享命名产物卷供后端下载与 worker 写入，数据库/Redis 各持久卷。
恰好五服务均有健康检查。Redis 队列加 SQL 排队状态兜底；worker 按 next_run 生成本地收件箱文件。周期 60 秒至 30 天。分发结果为批准分析的固定快照；若需新数据重新计算并建立新计划。当前不连接邮件或企业聊天。
错误响应、请求编号、Redis 每 IP 每分钟 240 次限流、JWT 和数据库操作审计。演示密钥与密码仅供本机模拟；生产部署前配置独立密钥与账号。

## 数据与验收
所有数据为固定随机种子生成的模拟业务数据，不能作为实际生产决策证据。项目独立算法、输入边界及业务局限详见 BUSINESS.md。测试与截图统一存放 ../../test-artifacts/gb-1010/，源码不含日志/截图/测试报告。
清理本项目演示数据：`docker compose down -v --remove-orphans`（会删除本项目命名卷）。

## License
MIT，见 LICENSE。
