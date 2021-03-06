import braintree
import psycopg2
from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil.rrule import rrule, DAILY
import os
import sys
import logging
import utils
import gzip
import csv
import requests


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, \
    format='%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s: %(message)s', \
    datefmt="%Y-%m-%d %H:%M:%S", \
    stream=sys.stdout)

class SessionHttp(braintree.util.http.Http):
    session = requests.Session()

    def __init__(self, config, environment=None):
        super(SessionHttp, self).__init__(config, environment)

    def _Http__request_function(self, method):
        if method == "GET":
            return SessionHttp.session.get
        elif method == "POST":
            return SessionHttp.session.post
        elif method == "PUT":
            return SessionHttp.session.put
        elif method == "DELETE":
            return SessionHttp.session.delete


def check_dupes(config):
    db_config = config['redshift']
    with psycopg2.connect(dbname=db_config["database"], user=db_config["user"], host=db_config["host"],port=db_config["port"],password=db_config["password"] ) as rs_conn:
        with rs_conn.cursor() as rs_cur:
            query = "select transaction_id,count(*) as count  from %s.%s group by transaction_id having count > 1;" % (config['braintree']['schema']['schema_name'],config['braintree']['schema']['table_name'])
            try:
                rs_cur.execute( query)
                rs_conn.commit()
            except Exception as e:
                rs_conn.rollback()
                LOG.error("Error checking for dupes in redshift table %s.%s with error: %s" % (config['braintree']['schema']['schema_name'],config['braintree']['schema']['table_name'],str(e) ))
            if(rs_cur.rowcount > 0):
                raise Exception("Found dupes in braintree import")
    rs_conn.close()

def get_last_updated(config):
    max_date_from_db = None
    db_config = config['redshift']
    with psycopg2.connect(dbname=db_config["database"], user=db_config["user"], host=db_config["host"],port=db_config["port"],password=db_config["password"] ) as rs_conn:
        LOG.info("redshift connected to host %s with user %s" % (db_config["host"],db_config["user"]))
        with rs_conn.cursor() as rs_cur:
            query = "select to_char(max(settlement_date_utc), 'YYYY-mm-dd HH24:MI:SS.US') from %s.%s" % (config['braintree']['schema']['schema_name'],config['braintree']['schema']['table_name'])
            try:
                rs_cur.execute( query)
                rs_conn.commit()
                max_date_from_db = rs_cur.fetchall()[0][0]
            except Exception as e:
                rs_conn.rollback()
                LOG.error("Error copying s3 file for %s to redshift with error %s and query:\n%s\n" % (str(e), config['braintree']['schema']['table_name'], query))
                raise e
    rs_conn.close()
    return max_date_from_db

def get_braintree_connection(braintree_config):
    braintree_connection = braintree.BraintreeGateway(
        braintree.Configuration(
            http_strategy=SessionHttp,
            environment=braintree.Environment.Production,#braintree_config['environment'],
            merchant_id=braintree_config['merchant_id'],
            public_key=braintree_config['public_key'],
            private_key=braintree_config['private_key']
        )
    )
    return braintree_connection

def get_search_results(braintree_connection,start,end):
    search_results = braintree_connection.transaction.search(
      braintree.TransactionSearch.settled_at.between(
        start,
        end
      ),
      (braintree.TransactionSearch.status == braintree.Transaction.Status.Settled)
    )
    return search_results

def process_transaction(transaction):
    t = {}
    t['transaction_id']                      = transaction.id
    t['transaction_type']                    = transaction.type
    t['transaction_status']                  = transaction.status
    t['transaction_created_at']              = transaction.created_at
    t['submitted_for_settlement_date']       = None
    t['settlement_date']                     = None
    t['transaction_disbursement_date']       = transaction.disbursement_details.disbursement_date
    t['transaction_merchant_account_id']     = transaction.merchant_account_id
    t['amount_authorized']                   = None
    t['amount_submitted_for_settlement']     = None
    t['transaction_service_fee_amount']      = transaction.service_fee_amount
    t['transaction_tax_amount']              = transaction.tax_amount
    t['transaction_tax_exempt']              = transaction.tax_exempt
    t['transaction_purchase_order_number']   = transaction.purchase_order_number
    t['transaction_order_id']                = transaction.order_id
    t['transaction_refunded_transaction_id'] = transaction.refunded_transaction_id
    t['transaction_payment_instrument_type'] = transaction.payment_instrument_type
    t['transaction_customer_id']             = None
    t['transaction_token']                   = None
    t['transaction_customer_company']        = None
    t['transaction_processor']               = None
    t['settlement_batch_id']                 = transaction.settlement_batch_id
    t['settlement_batch_date']               = transaction.settlement_batch_id[:10]
    t['shipping_country_name']               = transaction.shipping_details.country_name
    t['shipping_postal_code']                = transaction.shipping_details.postal_code
    t['shipping_region']                     = transaction.shipping_details.region
    t['billing_region']                      = transaction.billing_details.region
    t['source']                              = None
    t['user']                                = None
    t['ach_accountholder_name']              = None
    t['ach_bank_name']                       = None
    t['ach_business_name']                   = None
    t['ach_last4']                           = None
    t['ach_routing_number']                  = None
    t['ach_account_type']                    = None
    t['ach_ownership_type']                  = None


    #status timestamps here
    for status_event in transaction.status_history:
        if status_event.status == "submitted_for_settlement":
            t['submitted_for_settlement_date'] = status_event.timestamp
            t['amount_submitted_for_settlement'] = status_event.amount
            if hasattr('status_event', 'user'):
                t['user'] = status_event.user
        elif status_event.status == "settled":
            t['settlement_date'] = status_event.timestamp
        elif status_event.status == "authorized":
            t['amount_authorized'] = status_event.amount

    #card type here
    if (transaction.payment_instrument_type == 'amex_express_checkout_card'):
        t['transaction_card_type'] = transaction.amex_express_checkout_card_details.card_type
    elif (transaction.payment_instrument_type == "android_pay_card"):
        t['transaction_card_type'] = transaction.android_pay_card_details.source_card_type
        t['source'] = transaction.android_pay_card_details.source_description
    elif (transaction.payment_instrument_type ==  "apple_pay_card"):
        t['transaction_card_type'] = transaction.apple_pay_details.card_type
        t['source'] = transaction.apple_pay_details.source_description
    elif (transaction.payment_instrument_type == "credit_card"):
        t['transaction_card_type'] = transaction.credit_card_details.card_type
        t['transaction_token']     = transaction.credit_card_details.token
    elif (transaction.payment_instrument_type == "masterpass_card"):
        t['transaction_card_type'] = transaction.masterpass_card_details.card_type
    elif (transaction.payment_instrument_type == "paypal_account"):
        t['transaction_card_type'] = None
    elif (transaction.payment_instrument_type == "paypal_here"):
        t['transaction_card_type'] = transaction.paypal_here_details.payment_type
    elif (transaction.payment_instrument_type == "samsung_pay_card"):
        t['transaction_card_type'] = transaction.samsung_pay_card_details.card_type
    elif (transaction.payment_instrument_type == "us_bank_account"):
        t['transaction_card_type'] = None
        bank_account_info = transaction.us_bank_account
        t['ach_accountholder_name'] = bank_account_info.account_holder_name
        t['ach_bank_name']          = bank_account_info.bank_name
        t['ach_business_name']      = bank_account_info.business_name
        t['ach_last4']              = bank_account_info.last_4
        t['ach_routing_number']     = bank_account_info.routing_number
        t['ach_account_type']       = bank_account_info.account_type
        t['ach_ownership_type']     = bank_account_info.ownership_type
        t['transaction_token']      = bank_account_info.token
    elif (transaction.payment_instrument_type == "venmo_account"):
        t['transaction_card_type'] = None
        t['source'] = transaction.venmo_account['source_description']
    elif (transaction.payment_instrument_type == "visa_checkout_card"):
        t['transaction_card_type'] = transaction.visa_checkout_card_details.card_type
    else:
        t['transaction_card_type'] = "Error - Unknown Payment Instrument Type"

    #processor logic here
    if (t['transaction_payment_instrument_type'] == "paypal_account"):
         t['transaction_processor'] = "Paypal"
    elif (t['transaction_payment_instrument_type'] == "venmo_account"):
         t['transaction_processor'] = "Braintree"
    elif (t['transaction_payment_instrument_type'] == "us_bank_account"):
         t['transaction_processor'] = "USBankAccount"
    elif (t['transaction_card_type'] is not None):
        if (t['transaction_card_type'] == "Amex Express" or t['transaction_card_type'] == "American Express" or t['transaction_card_type'] == "Apple Pay - American Express"):
            t['transaction_processor'] = "American Express Merchant Account"
        elif (t['transaction_card_type'] == "Visa" or t['transaction_card_type'].lower() == "Mastercard".lower() or t['transaction_card_type'] == "Discover"):
            t['transaction_processor'] = "Braintree"
        elif (t['transaction_card_type'] == "UnionPay" or t['transaction_card_type'] == "Elo" or t['transaction_card_type'] == "JCB"):
            t['transaction_processor'] = "Braintree"
        elif (t['transaction_card_type'] == "Apple Pay - Discover" or t['transaction_card_type'] == "Apple Pay - Visa" or t['transaction_card_type'] == "Apple Pay - MasterCard"):
            t['transaction_processor'] = "Braintree"
    else:
         t['transaction_processor'] = "ERROR - UPDATE LOGIC"

    customer = transaction.vault_customer
    if (customer is not None):
        t['transaction_customer_id'] =  customer.id
        t['transaction_customer_company'] = customer.company

    return t

def process_search_results(search_results,braintree_connection):
    search_results_processed = []
    count = 0
    for transaction in search_results.items:
        try:
           search_results_processed.append(process_transaction(transaction))
        except Exception as e:
            try:
                search_results_processed.append(process_transaction(transaction))
            except Exception as e:
                try:
                    search_results_processed.append(process_transaction(transaction))
                except Exception as e:
                    LOG.error(str(e))
                    LOG.error("Error Processing transaction %s" % transaction)
                    raise(e)

        count = count + 1
        # if (count > 5):
        #    break
    return search_results_processed

def write_to_file(config,file_path,batchTimestamp,search_results_processed,fieldnames):
    with gzip.open( file_path , 'wt') as tempfile:
        csv_writer = csv.writer(tempfile, dialect=format)#, fieldnames=fieldnames )
        csv_writer.writerow(",".join(fieldnames))
        for r in search_results_processed:
            csv_writer.writerow([r['transaction_id'],r['transaction_type'],r['transaction_status'],r['transaction_created_at'],
                r['submitted_for_settlement_date'],r['settlement_date'],r['transaction_disbursement_date'],r['transaction_merchant_account_id'],
                r['amount_authorized'],r['amount_submitted_for_settlement'],r['transaction_service_fee_amount'],r['transaction_tax_amount'],
                r['transaction_tax_exempt'],r['transaction_purchase_order_number'],r['transaction_order_id'],r['transaction_refunded_transaction_id'],
                r['transaction_payment_instrument_type'],r['transaction_card_type'],r['transaction_customer_id'],r['transaction_token'],
                r['transaction_customer_company'],r['transaction_processor'],r['settlement_batch_id'],r['settlement_batch_date'],r['user'],
                r['shipping_country_name'],r['shipping_postal_code'],r['shipping_region'],r['billing_region'],r['source'],batchTimestamp,
                r['ach_accountholder_name'],r['ach_bank_name'],r['ach_business_name'],r['ach_last4'],r['ach_routing_number'],
                r['ach_account_type'],r['ach_ownership_type']])
    LOG.info("Wrote to  temp file %s" % file_path)

def write_file_and_upload_to_s3(config,search_results_processed,fieldnames):
    batchTimestamp = datetime.now()
    file_path = os.path.join(config['braintree']['FILE_DUMP_TEMP_DIR'], str(batchTimestamp.strftime('%Y-%m-%d_%H_%M_%S.%f'))) + '.gz'
    s3_key_name = os.path.join(config['braintree']['S3']['UPLOAD_DIR'],batchTimestamp.strftime('%Y-%m-%d/%H_%M_%S.%f')) + '.gz'
    write_to_file(config,file_path,batchTimestamp,search_results_processed,fieldnames)
    s3conn = utils.s3_connect(config['AWS']['ACCESSKEY'], config['AWS']['SECRETKEY'])
    utils.s3_upload(s3conn, config['braintree']['S3']['BUCKET'], s3_key_name, file_path)
    os.remove(file_path)
    return s3_key_name

def copy_into_redshift(config, s3_key, fieldnames):
    db_config = config['redshift']
    with psycopg2.connect(dbname=db_config["database"], user=db_config["user"], host=db_config["host"],port=db_config["port"],password=db_config["password"] ) as rs_conn:
        LOG.info("redshift connected to host %s with user %s" % (db_config["host"],db_config["user"]))
        LOG.info("copying to %s.%s" % (config['braintree']['schema']['schema_name'],config['braintree']['schema']['table_name']))
        with rs_conn.cursor() as rs_cur:
            query_template = "COPY %s.%s (%s)    FROM 's3://%s/%s' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' CSV DELIMITER ',' TIMEFORMAT 'YYYY-MM-DD HH:MI:SS' IGNOREBLANKLINES IGNOREHEADER 1 TRUNCATECOLUMNS GZIP"
            query = query_template % (
                    config['braintree']['schema']['schema_name'],
                    config['braintree']['schema']['table_name'],
                    ",".join(fieldnames),
                    config['braintree']['S3']['BUCKET'],
                    s3_key,
                    config['AWS']['ACCESSKEY'],
                    config['AWS']['SECRETKEY'])
            try:
                rs_cur.execute( query )
                rs_conn.commit()
            except Exception as e:
                rs_conn.rollback()
                LOG.error("Error copying s3 file for %s to redshift with error %s and query:\n%s\n" % (str(e), config['braintree']['schema']['table_name'], query))
                raise e
    rs_conn.close()

def get_data():
    LOG.info('App started')
    APP_HOME = os.environ['APP_HOME']
    APP_ENV = os.environ['APP_ENV']
    SECRET_PATH = os.environ['SECRET_PATH']
    secret_file_path = SECRET_PATH+"secrets."+APP_ENV+".json"
    config_file_path = APP_HOME+"src/config/config."+APP_ENV+".json"

    config = utils.load_config( secret_file_path,config_file_path )
    LOG.info("loaded config with database host:%s and user:%s" % (config['redshift']['host'],config['redshift']['user']))

    fieldnames = ['transaction_id' ,'transaction_type','transaction_status','created_datetime_utc','submitted_for_settlement_date_utc',
    'settlement_date_utc','disbursement_date_utc','merchant_account','amount_authorized','amount_submitted_for_settlement','service_fee',
    'tax_amount','tax_exempt','purchase_order_number','order_id','refunded_transaction_id','payment_instrument_type','card_type',
    'customer_id','payment_method_token','customer_company','processor','settlement_batch_id','settlement_batch_date','"user"',
    'shipping_country_name','shipping_postal_code','shipping_region','billing_region','source','date_uploaded_at',
    'ach_accountholder_name','ach_bank_name','ach_business_name','ach_last4','ach_routing_number','ach_account_type',
    'ach_ownership_type']


    last_updated_date = get_last_updated(config)
    start_date = datetime.strptime(last_updated_date, '%Y-%m-%d %H:%M:%S.%f') + timedelta(days=1)
    today = datetime.now()
    # the until date is not inclusive. results calculated upto yesterday
    for dt in rrule(DAILY, dtstart=start_date, until=today):
        start = datetime(dt.year, dt.month, dt.day, 0,0,0,0)
        end = datetime(dt.year, dt.month, dt.day, 23, 59, 59,999999)
        LOG.info("searching for results from %s to %s" % (start.strftime('%Y-%m-%d %H:%M:%S.%f'),end.strftime('%Y-%m-%d %H:%M:%S.%f')))

        # make braintree connection
        braintree_connection = get_braintree_connection(config['braintree']['braintree'])
        LOG.info("braintree_connection %s" % braintree_connection)

        # get search results
        search_results = get_search_results(braintree_connection,start,end)
        LOG.info("search_results count : %s " % search_results.maximum_size)

        # process results
        search_results_processed = process_search_results(search_results,braintree_connection)
        LOG.info("count of search_results processed %s " % len(search_results_processed))

        # write to file and upload to s3
        s3_key_name = write_file_and_upload_to_s3(config,search_results_processed,fieldnames)
        LOG.info("uploaded to s3 %s" % s3_key_name)

        #copy to redshift
        copy_into_redshift(config, s3_key_name,fieldnames)
        LOG.info("copied to redshift")

        #check for dupes
        check_dupes(config)
        LOG.info("check for dupes passed")

        LOG.info("-------------  processed data for date = %s ----------------------------" % start.strftime('%Y-%m-%d %H:%M:%S.%f'))


if __name__=="__main__":
    get_data()
