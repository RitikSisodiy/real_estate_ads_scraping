redis-server --daemonize yes &
uvicorn main:app --host 0.0.0.0 --port 8000 &
celery -A tasks.celery_app flower --host=0.0.0.0 --port=38000 &
celery -A tasks.celery_app worker --loglevel=info &
celery -A tasks.celery_app beat -l info &
java -jar real_estate_advert/seloger/selger.jar
