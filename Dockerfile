FROM python:3.5
ADD . /scraper
WORKDIR /scraper
RUN pip install -r requirements.txt
