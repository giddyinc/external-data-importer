# Braintree to Redshift
This is based off jira ticket [DATA-180](https://giddyinc.atlassian.net/browse/DATA-180)

The code will run as an airflow job - details here
### TODO
1. Insert data using S3 uploads
2. Data duplicates
3. API call limits

### Setting up environment
* Install dependencies using `pip requirements.txt`
* Update secrets and config files with appropriate values
* Create table in database if does not exist
* Start app using python3 `python3 braintree/braintree_report.py`


### Create table
```
create table externalsources.test_braintree_settlements
(
transaction_id VARCHAR (255),
transaction_type VARCHAR (255),
transaction_status VARCHAR (255),
created_datetime_utc timestamp,
submitted_for_settlement_date_utc timestamp,
settlement_date_utc timestamp,
disbursement_date_utc timestamp,
merchant_account VARCHAR (255),
amount_authorized VARCHAR (255),
amount_submitted_for_settlement VARCHAR (255),
service_fee VARCHAR (255),
tax_amount VARCHAR (255),
tax_exempt VARCHAR (255),
purchase_order_number VARCHAR (255),
order_id VARCHAR (255),
refunded_transaction_id VARCHAR (255),
payment_instrument_type VARCHAR (255),
card_type VARCHAR (255),
customer_id VARCHAR (255),
payment_method_token VARCHAR (255),
customer_company VARCHAR (255),
processor VARCHAR (255),
date_uploaded_at timestamp
);
```