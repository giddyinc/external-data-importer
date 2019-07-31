FROM python:3.6

ENV APP_NAME external-data-importer
ENV APP_HOME /srv/app/src
WORKDIR $APP_HOME
COPY requirements.txt $APP_HOME/requirements.txt
RUN pip install -r requirements.txt

COPY . $APP_HOME