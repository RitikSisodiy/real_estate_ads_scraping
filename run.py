import os
if os.name == 'nt':
    celeryworker = "celery -A tasks.celery_app worker -l info -P eventlet"
else:
    celeryworker = "celery -A tasks.celery_app worker --loglevel=info"
commands = (
    "uvicorn main:app --host 0.0.0.0 --port 8000",
    celeryworker,
    "celery -A tasks.celery_app beat -l info",
)
import subprocess
tasks = []
for command in commands:
    tasks.append(subprocess.Popen(command.split(' ')))

input("press Enter to exit")