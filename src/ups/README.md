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
create table externalsources.weekly_table_backfill(
payer_account                 varchar(128),
invoice_date                  date,
invoice_num                   varchar(128),
invoice_amt                   numeric(20,6),
transaction_date              date,
ship_ref_num1                 varchar(128),
ship_ref_num2                 varchar(128),
bill_option_code              varchar(128),
pkg_quantity                  integer     ,
tracking_num                  varchar(128),
entered_weight                numeric(20,6),
billed_weight                 numeric(20,6),
container_type                varchar(128),
pkg_dimension                 varchar(128),
zone                          double precision,
charge_category_code          varchar(128),
charge_category_detail_code   varchar(128),
charge_class_code             varchar(128),
charge_desc_code              varchar(128),
charge_desc                   varchar(128),
incentive_amt                 numeric(20,6),
net_amt                       numeric(20,6),
sender_name                   varchar(128),
sender_company                varchar(128),
sender_add1                   varchar(128),
sender_add2                   varchar(128),
sender_city                   varchar(128),
sender_state                  varchar(128),
sender_postal                 varchar(5),
sender_country_or_territory   varchar(128),
receiver_name                 varchar(128),
receiver_company              varchar(128),
receiver_add1                 varchar(128),
receiver_add2                 varchar(128),
receiver_city                 varchar(128),
receiver_state                varchar(128),
receiver_postal               varchar(5),
receiver_country_or_territory varchar(128),
type                          varchar(128),
return_type                   varchar(128),
date_uploaded_at              timestamp
);
```
