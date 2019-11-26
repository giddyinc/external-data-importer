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


### Create table
```
create table externalsources.weekly_table(
payer_account  VARCHAR (128),
invoice_date VARCHAR (128),
invoice_num VARCHAR (128),
invoice_amt VARCHAR (128),
transaction_date VARCHAR (128),
ship_ref_num1 VARCHAR (128),
ship_ref_num2 VARCHAR (128),
bill_option_code VARCHAR (128),
pkg_quantity VARCHAR (128),
tracking_num VARCHAR (128),
entered_weight VARCHAR (128),
billed_weight VARCHAR (128),
container_type VARCHAR (128),
pkg_dimension VARCHAR (128),
zone VARCHAR (128),
charge_category_code VARCHAR (128),
charge_category_detail_code VARCHAR (128),
charge_class_code VARCHAR (128),
charge_desc_code VARCHAR (128),
charge_desc VARCHAR (128),
incentive_amt VARCHAR (128),
net_amt VARCHAR (128),
sender_name VARCHAR (128),
sender_company VARCHAR (128),
sender_add1 VARCHAR (128),
sender_add2 VARCHAR (128),
sender_city VARCHAR (128),
sender_state VARCHAR (128),
sender_postal VARCHAR (128),
sender_country_or_territory VARCHAR (128),
receiver_name VARCHAR (128),
receiver_company VARCHAR (128),
receiver_add1 VARCHAR (128),
receiver_add2 VARCHAR (128),
receiver_city VARCHAR (128),
receiver_state VARCHAR (128),
receiver_postal VARCHAR (128),
receiver_country_or_territory VARCHAR (128),
type VARCHAR (128),
return_type VARCHAR (128),
date_uploaded_at timestamp
);
```
