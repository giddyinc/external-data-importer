import braintree
import psycopg2
from datetime import datetime
from datetime import timedelta
import os
import sys
import logging
import utils


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, \
    format='%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s: %(message)s', \
    datefmt="%Y-%m-%d %H:%M:%S", \
    stream=sys.stdout)

def get_braintree_connection(braintree_config):
    LOG.info("config %s" % braintree_config)
    braintree_connection = braintree.BraintreeGateway(
        braintree.Configuration(
            environment=braintree.Environment.Production,#braintree_config['environment'],
            merchant_id=braintree_config['merchant_id'],
            public_key=braintree_config['public_key'],
            private_key=braintree_config['private_key']
        )
    )
    return braintree_connection

def get_search_results(braintree_connection):
    N = 1
    date_N_days_ago = datetime.now() - timedelta(days=N)

    #fieldnames = ['transaction_id', "transaction_type", "transaction_status", "settlement_date",
    #"merchant_account","amount_submitted_for_settlement", "order_id", "payment_instrument_type",
    #"credit_card_type", "customer_location", "customer_id", "payment_method_token", "processor_authorization_code",
    #"paypal_payer_email", "paypal_payment_id", "paypal_authorization_id", "paypal_payer_id" ]

    search_results = braintree_connection.transaction.search(
      braintree.TransactionSearch.settled_at.between(
        date_N_days_ago,
        datetime.now()
      ),
      (braintree.TransactionSearch.status == braintree.Transaction.Status.Settled)
    )
    return search_results

def save_to_database(search_results,db_config,schema_name,table_name):
    count_saved = 0
    try:
        with psycopg2.connect(dbname=db_config["database"], user=db_config["user"], host=db_config["host"],port=db_config["port"],password=db_config["password"] ) as conn:
            with conn.cursor() as cursor:
                for transaction in search_results.items:
                    transaction_id = transaction.id or "transaction.id"
                    transaction_type = transaction.type or "transaction.type"
                    transaction_status = transaction.status or "transaction.status"
                    transaction_created_at = transaction.created_at or "transaction.created_at"
                    submitted_for_settlement_date = None
                    settlement_date = None
                    transaction_disbursement_date = datetime(1907,1,1)#""transaction.disbursement_date" #if transaction.disbursement_date is None else transaction.disbursement_date
                    transaction_merchant_account_id = transaction.merchant_account_id or "transaction.merchant_account_id"
                    amount_authorized = None
                    amount_submitted_for_settlement = None
                    transaction_service_fee_amount = transaction.service_fee_amount or "transaction.service_fee_amount"
                    transaction_tax_amount = transaction.tax_amount or "transaction.tax_amount"
                    transaction_tax_exempt = transaction.tax_exempt or "transaction.tax_exempt"
                    transaction_purchase_order_number = transaction.purchase_order_number or "transaction.purchase_order_number"
                    transaction_order_gid = "transaction.order_gid"# or "transaction.order_gid"
                    transaction_order_id = transaction.order_id or "transaction.order_id"
                    transaction_refunded_transaction_id = transaction.refunded_transaction_id or "transaction.refunded_transaction_id"
                    transaction_payment_instrument_type = transaction.payment_instrument_type or "transaction.payment_instrument_type"
                    transaction_card_type = transaction.credit_card_details.card_type or "transaction.credit_card_details.card_type"
                    transaction_customer_id = "transaction.customer_id"# or "transaction.customer_id"
                    transaction_token = transaction.credit_card_details.token or "transaction.credit_card_details.token"
                    transaction_customer_company = "transaction.customer_company"# or "transaction.customer_company"
                    transaction_channel = transaction.channel or "transaction.channel"
                    transaction_processor = "transaction.processor"

                    for status_event in transaction.status_history:
                        if status_event.status == "submitted_for_settlement":
                            submitted_for_settlement_date = status_event.timestamp
                            amount_submitted_for_settlement = status_event.amount
                        elif status_event.status == "settled":
                            settlement_date = status_event.timestamp
                        elif status_event.status == "authorized":
                            amount_authorized = status_event.amount

                    sql = "insert into "+schema_name+"."+table_name+" (transaction_id,transaction_type,transaction_status,created_datetime,      submitted_for_settlement_date,settlement_date,disbursement_date,            merchant_account,               amount_authorized,amount_submitted_for_settlement,service_fee,                   tax_amount,            tax_exempt,            purchase_order_number,            order_gid,            order_id,            refunded_transaction_id,            payment_instrument_type,            card_type,            customer_id,            payment_method_token,customer_company,            channel,            processor, raw_data) values(%s,            %s,              %s,                %s,                    %s,                           %s,             %s,                           %s,                             %s,               %s,                             %s,                            %s,                    %s,                    %s,                               %s,                   %s,                  %s,                                 %s,                                 %s,                   %s,                     %s,                  %s,                          %s,                 %s,                   %s)"
                    params = (                                     transaction_id,transaction_type,transaction_status,transaction_created_at,submitted_for_settlement_date,settlement_date,transaction_disbursement_date,transaction_merchant_account_id,amount_authorized,amount_submitted_for_settlement,transaction_service_fee_amount,transaction_tax_amount,transaction_tax_exempt,transaction_purchase_order_number,transaction_order_gid,transaction_order_id,transaction_refunded_transaction_id,transaction_payment_instrument_type,transaction_card_type,transaction_customer_id,transaction_token,   transaction_customer_company,transaction_channel,transaction_processor,str(transaction))
                    cursor.execute(sql,params)
                    conn.commit()
                    count_saved = count_saved + 1
                    if (count_saved == 20):
                        break
    except Exception as e:
        LOG.info(str(e))
        conn.rollback()

    return count_saved

def get_data():
    LOG.info('App started')
    APP_HOME = os.environ['APP_HOME']
    LOG.info("APP_HOME:"+APP_HOME)
    APP_ENV = os.environ['APP_ENV']
    LOG.info("APP_ENV:"+APP_ENV)
    SECRET_PATH = os.environ['SECRET_PATH']
    LOG.info("SECRET_PATH:"+SECRET_PATH)
    secret_file_path = SECRET_PATH+"secrets."+APP_ENV+".json"
    config_file_path = APP_HOME+"src/config/config-braintree."+APP_ENV+".json"

    files = os.listdir(APP_HOME)
    LOG.info("files in APP HOME %s" % files)
    files = os.listdir(APP_HOME+"/src")
    LOG.info("files in APP HOME/src %s" % files)
    files = os.listdir(APP_HOME+"/src/secrets")
    LOG.info("files in APP HOME/src/secrets %s" % files)

    # config = utils.load_config( secret_file_path,config_file_path )
    # LOG.info("loaded config with database host:%s and user:%s" % (config['braintree']['database']['host'],config['braintree']['database']['user']))

    # braintree_connection = get_braintree_connection(config['braintree']['braintree'])
    # LOG.info("braintree_connection %s" % braintree_connection)
    # search_results = get_search_results(braintree_connection)
    # LOG.info("search_results: %s " % search_results)

    # count_saved = save_to_database(search_results,config['braintree']['database'],config['schema']['schema_name'],config['schema']['table_name'])
    # LOG.info("Script Complete - saved %s rows to database" % count_saved)

if __name__=="__main__":
    get_data()
