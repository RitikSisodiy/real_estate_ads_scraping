# Use an official Python runtime as a parent image
FROM python:3.8.13-slim-buster

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Install Java runtime and Redis server
RUN apt-get update && apt-get install -y default-jre redis-server

# Make port 80 and 6379 available to the world outside this container
EXPOSE 80
EXPOSE 6379

# Define environment variable
ENV NAME World

# Run app.py and the Java program when the container launches
CMD ["sh", "-c", "redis-server --daemonize yes && (uvicorn main:app --host 0.0.0.0 --port 8000 & celery -A tasks.celery_app flower --host=0.0.0.0 --port=38000 & celery -A tasks.celery_app worker --loglevel=info & celery -A tasks.celery_app beat -l info & java -jar real_estate_advert/seloger/selger.jar)"]