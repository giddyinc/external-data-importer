FROM python:3.6

ENV APP_NAME external-data-importer
ENV APP_HOME /srv/app/
WORKDIR $APP_HOME
COPY src/requirements.txt $APP_HOME/requirements.txt
RUN pip install -r requirements.txt
COPY . $APP_HOME
ENV SECRET_PATH $APP_HOME/src/secrets/
RUN mkdir /srv/app/temp
RUN mkdir /srv/app/temp/braintree
RUN mkdir /srv/app/temp/braintree/prod/
RUN mkdir /srv/app/temp/ups
RUN mkdir /srv/app/temp/ups/prod/
