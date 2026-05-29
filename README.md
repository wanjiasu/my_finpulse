# FastAPI Celery Flower Templator

这是一个基于 **FastAPI**、**Celery**、**Redis**、**PostgreSQL** 和 **Flower** 的异步任务处理模板项目。

## 🚀 项目简介

本项目提供了一个完整的异步任务架构模板，适用于需要处理耗时任务（如数据抓取、大规模计算、邮件发送等）的 Web 应用。它集成了以下功能：

- **FastAPI**: 高性能的异步 Web 框架，负责 API 接口。
- **Celery**: 强大的分布式任务队列，处理后台异步任务。
- **Redis**: 作为 Celery 的消息代理（Broker）和后端（Backend）。
- **PostgreSQL**: 用于持久化存储任务结果和其他业务数据（如股票数据）。
- **Flower**: Celery 的实时监控工具，提供 Web 界面查看任务状态。
- **Docker**: 支持容器化部署，方便环境迁移。

## 🛠️ 技术栈

- **Language**: Python 3.10+
- **Web Framework**: FastAPI
- **Task Queue**: Celery
- **Message Broker/Backend**: Redis
- **Database**: PostgreSQL (psycopg2)
- **Monitoring**: Flower
- **Package Manager**: [uv](https://github.com/astral-sh/uv)
- **Deployment**: Docker & Docker Compose

## 📂 项目结构

```text
.
├── app/
│   ├── data_fetcher/        # Tushare 数据抓取核心模块 (基类 + 子类)
│   │   ├── base_fetcher.py   # 数据抓取基类 (含自动重试机制)
│   │   ├── index_fetcher.py  # 指数数据抓取
│   │   ├── sector_fetcher.py # 板块/概念数据抓取
│   │   └── stock_fetcher.py  # 股票基础/行情/复权因子抓取
│   ├── celery_app.py        # Celery 实例配置
│   ├── db.py                # 数据库初始化 (PostgreSQL + SQLAlchemy)
│   ├── main.py              # FastAPI 主入口 (API 路由定义)
│   ├── settings.py          # 配置文件 (Pydantic Settings)
│   └── tasks.py             # Celery 异步任务定义
├── Dockerfile               # Docker 镜像构建文件
├── docker-compose.yml       # 多容器启动配置
├── env.example              # 环境变量模板
├── pyproject.toml           # 项目依赖配置
└── uv.lock                  # uv 锁定文件
```

## ⚙️ 快速开始

### 1. 克隆项目并配置环境

```bash
cp env.example .env
```

默认配置已优化为使用 Docker Compose 启动的本地服务。

### 2. 使用 Docker Compose 启动

```bash
docker-compose up --build
```

启动后，所有依赖项（包括 Redis 和 PostgreSQL）都会自动在容器中运行。
- **API 服务**: [http://localhost:8000](http://localhost:8000)
- **Flower 监控**: [http://localhost:5555](http://localhost:5555)
- **API 文档 (Swagger)**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Redis**: `localhost:6379`
- **PostgreSQL**: `localhost:5432`

### 3. 本地运行 (开发环境)

如果你想在本地开发而不使用 Docker，可以使用 `uv`:

```bash
# 安装依赖
uv sync

# 启动 FastAPI
uv run uvicorn app.main:app --reload

# 启动 Celery Worker
uv run celery -A app.celery_app.celery worker -l info

# 启动 Flower
uv run celery -A app.celery_app.celery flower --port=5555
```

## 🛣️ API 接口说明

### 基础任务
- **GET `/health`**: 健康检查。
- **POST `/tasks/add`**: 触发一个简单的加法异步任务。
- **GET `/tasks/{task_id}`**: 获取指定异步任务的执行状态和结果。

### 股票/行情同步
- **POST `/stocks/sync`**: 同步股票基础信息（代码、名称、行业等）。
- **POST `/stocks/sync_daily`**: 同步全市场指定日期的行情和复权因子。
    - 参数: `trade_date` (YYYYMMDD)
- **POST `/stocks/sync_history`**: 批量同步历史行情数据。
    - 参数: `start_date` (默认 20180101)

### 板块/指数同步
- **POST `/sectors/ths_index/sync`**: 同步同花顺板块指数列表。
- **POST `/sectors/ths_member/sync`**: 同步指定板块的成分股列表。
    - 参数: `ts_code` (板块代码)

## 📝 核心特性

1.  **面向对象设计**: 数据抓取逻辑采用基类+子类模式，结构清晰，易于扩展。
2.  **自动重试机制**: 基类内置 API 限流重试逻辑，触发 Tushare 频率限制时自动休息 60 秒后重试。
3.  **异步持久化**: 结合 Celery 和 SQLAlchemy，支持大规模数据（如数年历史行情）的异步批量入库。
4.  **鲁棒性**: 任务级别支持异常自动重试，确保长周期同步任务的稳定性。


## 当前问题
finacial数据待进一步研究