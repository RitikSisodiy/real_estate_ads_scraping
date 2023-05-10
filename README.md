# Real Estate Ads Scraper

This is a Python-based web scraper that extracts real estate ad data from multiple websites. The project is organized in a modular and scalable manner, allowing developers to easily add new portals to the scraper and extend its functionality.

## Project Structure

The folder structure of Real Estate Ads Scraper is as follows:

```
real-estate-ads-scraper/
├── HttpRequest/
│   ├── AioProxy.py
│   ├── AsyncProxy.py
│   ├── requestsModules.py
│   ├── s3Client.py
│   └── uploader.py
├── real_estate_advert/
│   ├── avendrealouer/
│   │   ├── scraper.py
│   │   └── parser.py
│   ├── bienci/
│   │   ├── scraper.py
│   │   └── parser.py
│   ├── gensdeconfiance/
│   │   ├── scraper.py
│   │   └── parser.py
│   ├── leboncoin/
│   │   ├── scraperv4.py
│   │   └── parser.py
│   ├── lefigaro/
│   │   ├── scraper.py
│   │   └── parser.py
│   ├── logicImmo/
│   │   ├── logicImmo.py
│   │   └── parser.py
│   ├── ouestfrance/
│   │   ├── scraper.py
│   │   └── parser.py
│   ├── pap/
│   │   ├── scraper2.py
│   │   └── parser.py
│   ├── paruvendu/
│   │   ├── scraperv2.py
│   │   └── parser.py
│   └── seloger/
│       ├── scraperv3.py
│       └── parser.py
├── api.py
├── celery_commands.txt
├── Dockerfile
├── kafka_publisher.py
├── main.py
├── readme.md
├── requirements.txt
├── run.py
├── saveLastCheck.py
├── settings.py
├── status.py
└── tasks.py

```

### HttpRequest/

The `HttpRequest` directory contains modules used for making HTTP requests, handling proxies, uploading images to S3, and sending scraped data to Kafka topics.

### real_estate_advert/

The `real_estate_advert` directory contains subdirectories for each real estate portal that the scraper can crawl. Each portal subdirectory contains `scraper.py` for scraping ads from the portal and `parser.py` for parsing the data into a common structure.

### api.py

`api.py` contains endpoints that can be used to start individual scraper tasks.

### celery_commands.txt

`celery_commands.txt` contains commands to start the project using Celery.

### Dockerfile

`Dockerfile` can be used to containerize the project.

### kafka_publisher.py

`kafka_publisher.py` sends data to Kafka topics, but it has been replaced with `HttpRequest/uploader.py`.

### main.py

`main.py` is the main file for FastAPI.

### readme.md

`readme.md` contains project setup instructions.

### requirements.txt

`requirements.txt` lists Python dependencies.

### run.py

`run.py` runs all required commands at once.

### saveLastCheck.py

`saveLastCheck.py` changes the status of deleted ads in Elasticsearch by the last check time.

### settings.py

`settings.py` contains project configuration and settings.

### status.py 
`status.py` This file is used to manually check if an ad exists or not (Deprecated).

### tasks.py 
`tasks.py`  This file contains the Celery app configuration and all tasks, including task configuration to scrape, update, and check deleted ads, as well as cronjobs to run scheduled tasks at specified intervals.



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

5. ## Installing and Configuring Redis 
    ```
    sudo apt update
    ```

    Then, install Redis by typing:

    ```
    sudo apt install redis-server
    ```

    restart redis server:

    ```
    sudo systemctl restart redis.service
    ```

    ## Testing Redis

    Start by checking that the Redis service is running:

    ```
    sudo systemctl status redis
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

4. To start the Celery-flower monitering
    ```
    celery -A tasks.celery_app flower --port=38000
    ```
5. To start the seloger token genrater
    ```
    java -jar real_estate_advert/seloger/selger.jar
    ```

5. To run the script:

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




# Method 2 
Real Estate Ads Scraping Docker Image
This Docker image is used to scrape real estate ads using Python, Celery, and Redis. It includes a Java program for web scraping real estate websites.

### Building the Image

To build the image, run the following command in the directory where the Dockerfile is located:

```
docker build -t realscraper .
```

This will create a Docker image with the tag "realscraper".

### Running the Container

To run the container, use the following command:

```
docker run -it --name realscraper -v /apps/KI/real-estate-ads-scraping:/app -p 38000:38000 -p 8000:8000 --add-host node-2.kifwat.net:10.8.0.44 --add-host node-3.kifwat.net:10.8.0.45 --add-host node-4.kifwat.net:10.8.0.46 --add-host node-1.kifwat.net:10.8.0.43 realscraper
```

This will run the container with the name "realscraper", map the ports 38000 and 8000 to the host machine, and add the specified hosts. It will also mount the local directory `/apps/KI/real-estate-ads-scraping` to the `/app` directory inside the container.

### Detaching from the Container

To detach from the running container without stopping the process, press `Ctrl+p`, followed by `Ctrl+q`.

### Accessing the Application

Once the container is running, you can access the application at http://localhost:8000. You can also access the Celery Flower monitoring tool at http://localhost:38000.

### Environment Variables

The image defines an environment variable `NAME` which is set to `World` by default. You can change the value of this variable by setting it when running the container, like this:

```
docker run -it --name realscraper -v /apps/KI/real-estate-ads-scraping:/app -p 38000:38000 -p 8000:8000 --add-host node-2.kifwat.net:10.8.0.44 --add-host node-3.kifwat.net:10.8.0.45 --add-host node-4.kifwat.net:10.8.0.46 --add-host node-1.kifwat.net:10.8.0.43 -e NAME=John realscraper
``` 