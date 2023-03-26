## Web Scraping Service

This web scraping server is responsible for scraping data from different resources. It's built on a REST API server that is built on FAST API.

### Getting Started

A powerful and flexible toolkit for building web scraping.

### Requirements

- Python 3.7+
- pip
- git

### Installation

1. First ensure you have python globally installed on your computer. If not, you can download python [here](https://www.python.org).

2. Confirm that you have installed virtualenv globally as well. If not, run this:

    ```
    pip install virtualenv
    ```

3. Git clone this repo to your PC:

    ```
    git clone https://git.kifwat.net/ki/services/real-estate-ads-scraping.git
    ```

4. Install the dependencies:

    ```
    virtualenv venv -p python3
    source venv/bin/activate
    cd real-estate-ads-scraping
    pip install -r requirements.txt
    ```

5. To install RabbitMQ Server in Ubuntu 22.04|20.04|18.04, update apt list first:

    ```
    sudo apt update
    sudo apt install rabbitmq-server
    systemctl status rabbitmq-server.service
    ```

6. Install the Celery Package in Ubuntu:

    ```
    sudo apt install python-celery-common
    ```

### Running the Server

1. Start the server using this one simple command and bind machine IP to API Gateway:

    ```
    uvicorn main:app --host 0.0.0.0 --port 8000
    ```

2. Run the Celery worker:

   **Linux**

    ```
    celery -A tasks.celery_app worker --loglevel=info
    ```

   **Windows**

    ```
    celery -A tasks.celery_app worker -l info -P eventlet
    ```

3. To run the scheduler:

    ```
    celery -A tasks.celery_app beat -l info
    ```

4. To run the script:

    ```
    cd /apps/KI/real-estate-ads-scraping/ && source env/bin/activate && (uvicorn main:app --host 0.0.0.0 --port 8000 & celery -A tasks.celery_app flower --port=38000 & celery -A tasks.celery_app worker --loglevel=info & celery -A tasks.celery_app beat -l info & java -jar real_estate_advert/seloger/selger.jar)
    ```

5. To kill the script:

    ```
    ps aux | grep -i "/apps/KI/real-estate-ads-scraping/env/bin/python3.8 /apps/KI/real-estate-ads-scraping/env/bin/uvicorn\|/apps/KI/real-estate-ads-scraping/env/bin/python3.8 /apps/KI/real-estate-ads-scraping/env/bin/celery" | grep -v grep | awk {'print $2'} | xargs kill -9
    ```

Note: Make sure to create .env file or set environment variables.

## Environment variables

* KAFKA:
  * BROKER_IPS: `<host>:<port>,<host>:<port>`
* S3 Client:
  * REGION_NAME
  * BUCKET_NAME
* ES Client:
  * ES_HOSTS: `<host>:<port>,<host>:<port>`
  * ES_USER: `<username>`
  * ES_PASSWORD: `<password>`
* Proxy:
  * Proxy: `<proxyurl>`
