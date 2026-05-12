from celery import Celery
from celery.schedules import crontab
from .settings import settings

celery = Celery(
    "worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["app.tasks"],
)

celery.conf.update(
    task_track_started=True,
    broker_connection_retry_on_startup=True,
    timezone='Asia/Shanghai',
    enable_utc=False,
)

# 定时任务配置
celery.conf.beat_schedule = {
    "daily_sync_at_18pm": {
        "task": "tasks.sync_daily_automatic",
        "schedule": crontab(hour=18, minute=0),
    },
}
