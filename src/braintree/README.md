# Braintree to Redshift
This is based off jira ticket [DATA-180](https://giddyinc.atlassian.net/browse/DATA-180)

The code will run as an airflow job - details here
### TODO
API call limits

### Setting up environment
* Install dependencies using `pip requirements.txt`
* Update secrets and config files with appropriate values
* Create table in database if does not exist
* Start app using python3 `python3 braintree/braintree_report.py`

### Running as docker Containter
* Add following at the end of Dockerfile
  * `ENV APP_ENV staging`
  * `CMD [ "python3" , "/srv/app/src/braintree/braintree_report.py" ]`
* create secrets.staging.json and config.staging.json based on template
*  cd to `external-data-importer`
* run `docker build -t braintree .`
* run ` docker run braintree`


### Create table
```
create table externalsources.test_braintree_settlements
(
transaction_id VARCHAR (32),
transaction_type VARCHAR (32),
transaction_status VARCHAR (32),
created_datetime_utc timestamp,
submitted_for_settlement_date_utc timestamp,
settlement_date_utc timestamp,
disbursement_date_utc timestamp,
merchant_account VARCHAR (64),
amount_authorized VARCHAR (32),
amount_submitted_for_settlement VARCHAR (32),
service_fee VARCHAR (32),
tax_amount VARCHAR (32),
tax_exempt VARCHAR (32),
purchase_order_number VARCHAR (64),
order_id VARCHAR (64),
refunded_transaction_id VARCHAR (32),
payment_instrument_type VARCHAR (64),
card_type VARCHAR (32),
customer_id VARCHAR (64),
payment_method_token VARCHAR (32),
customer_company VARCHAR (128),
processor VARCHAR (64),
settlement_batch_id VARCHAR (64),
settlement_batch_date VARCHAR(32),
"user" VARCHAR (64),
shipping_country_name VARCHAR (64),
shipping_postal_code VARCHAR(32),
shipping_region VARCHAR(32),
billing_region  VARCHAR(32),
source  VARCHAR(32),
date_uploaded_at timestamp
)
distkey(settlement_date_utc)
;
```

```
ALTER TABLE externalsources.test_braintree_settlements ADD COLUMN billing_region  VARCHAR(32);
ALTER TABLE externalsources.test_braintree_settlements ADD COLUMN source  VARCHAR(32);
```
