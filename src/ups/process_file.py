import os
import sys
import logging
import pandas as pd
import re
import s3fs
from datetime import datetime

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, \
    format='%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s: %(message)s', \
    datefmt="%Y-%m-%d %H:%M:%S", \
    stream=sys.stdout)

def make_snake_case(x):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', x)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

from functools import reduce

def change_case(str): 
    return reduce(lambda x, y: x + ('_' if y.isupper() else '') + y, str).lower() 

required_cols = [
                'payer_account',
                'invoice_date',
                'invoice_num',
                'invoice_amt',
                'transaction_date',
                'ship_ref_num1',
                'ship_ref_num2',
                'bill_option_code',
                'pkg_quantity',
                'tracking_num',
                'entered_weight',
                'billed_weight',
                'container_type',
                'pkg_dimension',
                'zone',
                'charge_category_code',
                'charge_category_detail_code',
                'charge_class_code',
                'charge_desc_code',
                'charge_desc',
                'incentive_amt',
                'net_amt',
                'sender_name',
                'sender_company',
                'sender_add1',
                'sender_add2',
                'sender_city',
                'sender_state',
                'sender_postal',
                'sender_country_or_territory',
                'receiver_name',
                'receiver_company',
                'receiver_add1',
                'receiver_add2',
                'receiver_city',
                'receiver_state',
                'receiver_postal',
                'receiver_country_or_territory',
                ]


def process_file(local_path,file_raw,file_processed,s3_path):
    df = pd.read_csv(local_path+file_raw, low_memory=False)
    current_columns = df.columns.tolist()
    new_columns = [make_snake_case(i)  for i in current_columns]
    df.columns = new_columns

    #drop extra header row
    df = df.drop(0)
    df = df[required_cols]
    LOG.info("created data frame")
    #df['receiver_postal'] = df['receiver_postal'].astype(str)
    #df['receiver_postal-trunc'] = df['receiver_postal'].str.slice[:5]

    #df['sender_postal'] = df['sender_postal'].str[:5]

    desc_csv = pd.read_csv('s3://boxed-pensieve-s3-redshift/ups/ups_conversion/Charge_Descriptions_Key.csv')
    final_df = df.merge(desc_csv,on='charge_desc')
    LOG.info("merged new file")

    final_df['date_uploaded_at'] = datetime.now()
    final_df.to_csv(s3_path+file_processed, index=False)
    LOG.info("uploaded new file to S3")

