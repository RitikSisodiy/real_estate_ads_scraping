# Web Scrapping Services
web scraping server is responsible to scrape data from different resources. REST API server that is build on FAST API


## Getting Started
A powerful and flexible toolkit for building Web Scraping.
## Requirements
Python 3.7+, pip and git

## Installation
* first ensure you have python globally installed in your computer. If not, you can get python [here](https://www.python.org").
* After doing this, confirm that you have installed virtualenv globally as well. If not, run this:
    ```bash
     pip install virtualenv
    ```
* Then, Git clone this repo to your PC
    ```bash
  [optional]
       cd /apps/ 
       cd KI
  
  git clone https://git.kifwat.net/ki/services/real-estate-ads-scraping.git
    ```
  
* #### Dependencies

    1. Create and fire up your virtual environment:
        ```bash
        virtualenv  venv -p python3
        source venv/bin/activate
        ```
    2. Cd into real-estate-ads-scraping directory:
        ```bash
        cd real-estate-ads-scraping
        ```
    3. Install the dependencies needed to run the app:
        ```bash
        pip install -r requirements.txt
        ```
    4. To install RabbitMQ Server Ubuntu 22.04|20.04|18.04, update apt list first::
        ```bash
       sudo apt update
       sudo apt install rabbitmq-server
       
       [Verify the rabbitmq-server is woring by using below command]
       systemctl status rabbitmq-server.service
        ```
    5. Install the Celery Package in Ubuntu:
        ```bash
        sudo apt install python-celery-common
        ```

* #### Run It
    Fire up the server using this one simple command and
    bind machine IP to API Gateway 
    ```bash
    uvicorn main:app  --host 0.0.0.0 --port 8000
    ```
  Run celery worker
    ```
    celery -A tasks.celery_app worker -l info
    ```
  You can now access the file api service on your browser by using
    ```
    [machineIP]:8000/docs
  Example
  10.8.0.27:8000/docs
    ```
note make sure to create .env file of set enviorment variable
````# KAFKA
BROKER_IPS=<host>:<port>,<host>:<port>
# s3 Client 
REGION_NAME= 
BUCKET_NAME=
# ES Client
ES_HOSTS=<host>:<port>,<host>:<port>
ES_USER=<username>
ES_PASSWORD=<passworkd>
# Proxy
Proxy=<proxyurl>````