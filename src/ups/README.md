# Braintree to Redshift
This is based off jira ticket [DATA-216](https://giddyinc.atlassian.net/browse/DATA-216)

The code will run as an airflow job - details here
### TODO
* Encryption on files
* Table Structure
* Data Transformation

### Setting up environment
* Install dependencies using `pip requirements.txt`
* Update secrets and config files with appropriate values
* Create table in database if does not exist
* Start app using python3 `python3 ups/ups_report.py`

### Running as docker Containter
* Add following at the end of Dockerfile
  * `ENV APP_ENV staging`
  * `CMD [ "python3" , "/srv/app/src/ups/ups_report.py" ]`
* create secrets.staging.json and config.staging.json based on template
*  cd to `external-data-importer`
* run `docker build -t ups .`
* run ` docker run ups`
