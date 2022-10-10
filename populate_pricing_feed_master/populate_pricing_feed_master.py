import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

import json
import boto3
import pandas as pd
from pyathena import connect
from datetime import date, datetime
from datetime import timedelta
import time
import io
from io import StringIO
import vaex as vx
import gc
import os

import numpy as np
import sys
import requests

sys.path.insert(0, '/glue/lib/installation')
keys = [k for k in sys.modules.keys() if 'boto' in k]
for k in keys:
    if 'boto' in k:
       del sys.modules[k]
import boto3


def send_message_to_slack(channel,username, message):
    url = "https://hooks.slack.com/services/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"
    payload={"channel": channel, "username": username, "text": message}
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    return response

send_message_to_slack('#de-etlops-alerts','populate_pricing_feed_master', 'PopulatePricingMasterData: Job Started')

secret_name = "arn:aws:secretsmanager:us-east-1:209656230012:secret:DEteam_Access_keys-Shm8fc"
region_name = "us-east-1"

session = boto3.session.Session()
client = session.client(service_name='secretsmanager', region_name=region_name)

aws_access = client.get_secret_value(SecretId=secret_name)['SecretString']

aws_access=json.loads(aws_access)

athena_conn = connect(aws_access_key_id=aws_access['aws_access_key_id'],
                          aws_secret_access_key=aws_access['aws_secret_access_key'],
                                   s3_staging_dir= 's3://testing-0255/temp/',
                                        region_name='us-east-1')
                          
                         
s3_client = boto3.client(
    "s3"
    )

s3 = boto3.resource(
    "s3"
)
bucket=s3.Bucket('retailscape-rearch')
bucket1=s3.Bucket('pricing-analytics')
customer_code='chewy'
#key_id_cols = ['tenant_code_id','tenant_source_id','tenant_sku_id','tenant_category_id','tenant_sub_subcategory_id','priority_id','comp_sku_id','comp_source_id','comp_store_id','comp_seller_id']
#dim_tables = ['pricing_tenant_code_dim', 'pricing_tenant_source_dim', 'pricing_tenant_sku_dim', 'pricing_tenant_category_dim', 'pricing_tenant_sub_subcategory_dim', 'pricing_priority_dim', 'pricing_tenant_brand_dim', ' pricing_comp_sku_dim', 'pricing_comp_source_dim', 'pricing_comp_store_dim', 'pricing_comp_seller_dim']


query="""
UNLOAD (
    select 
    cast(concat(D1.year, '-', D1.month, '-', D1.day) as timestamp) as capture_date, 
    cast(concat(D1.year,D1.month,D1.day) as int) as capture_date_int,
    D1.effective_price as tenant_effective_price, 
    D1.product_fullimage as tenant_image_url,
    D1.product_list_price as tenant_list_price,
    D1.product_name AS tenant_product_title,
	D1.product_url AS tenant_product_url,
    D1.offer_price as tenant_promo_price,
    D1.product_regular_price as tenant_reg_price,
    D1.product_size AS tenant_size,
    D1.product_weight_uom as tenant_uom, --to be confirmed
    D2.size as comp_size,
    D2.uom as comp_uom,
    D2.promo_price as comp_promo_price,
    round(try_cast(D1.effective_price as double)/D2.effective_price, 3) as cpi_value,
    D2.is_latest_feed,
    CASE
            WHEN (try_cast(D1.effective_price as double)/D2.effective_price) > 1.05 THEN False
            WHEN (try_cast(D1.effective_price as double)/D2.effective_price) < 0.95 THEN False 
            WHEN D2.effective_price is null then False
            ELSE True END
            AS price_competitiveness_flag,
	
	CASE
            WHEN (avg(try_cast(D1.effective_price as double)) over(partition by try_cast(D1.product_part_number as bigint)) / min(try_cast(D2.effective_price as double)) over(partition by try_cast(D1.product_part_number as bigint))) > 1.05 THEN False
            WHEN (avg(try_cast(D1.effective_price as double)) over(partition by try_cast(D1.product_part_number as bigint)) / min(try_cast(D2.effective_price as double)) over(partition by try_cast(D1.product_part_number as bigint))) < 0.95 THEN False
            WHEN (avg(try_cast(D1.effective_price as double)) over(partition by try_cast(D1.product_part_number as bigint)) / min(try_cast(D2.effective_price as double)) over(partition by try_cast(D1.product_part_number as bigint)))  is null then False
            ELSE True END
            AS price_opp_flag,
			
    D1.product_discontinued_flag as is_discontiued,
    D1.store_name_display as tenant_store_name_display,
    D1.tenant_source,
    b.tenant_source_id,
    D1.tenant_store,
    w.tenant_store_id,
    D1.tenant_source_store,
	e.tenant_source_store_id,
	D1.product_part_number as tenant_sku,
    c.tenant_sku_id,
	replace(D1.product_gtin,'"','') as tenant_upc,
	p.tenant_upc_id,
	D1.product_purchase_brand as tenant_brand,
    d.tenant_brand_id,
	D1.product_merch_classification1 as tenant_category,
    f.tenant_category_id,
	D1.product_merch_classification2  as tenant_subcategory,
    g.tenant_subcategory_id,
	D1.product_merch_classification3 as tenant_sub_subcategory,
    h.tenant_sub_subcategory_id,
    i.tenant_code_id,
	D2.sku as comp_sku,
    j.comp_sku_id,
	D2.upc as comp_upc,
	q.comp_upc_id,
	regexp_replace(D2.source_name, '(\w)(\w*)', x -> upper(x[1]) || lower(x[2])) as comp_source,
    k.comp_source_id,
    D2.store_name as comp_store,
    v.comp_store_id,
    D2.source_store as comp_source_store,
	r.comp_source_store_id,
	D2.store_name_display as comp_store_name_display,
	D2.brand as comp_brand,
    l.comp_brand_id,
	D2.category as comp_category,
	s.comp_category_id,
	D2.subcategory as comp_subcategory,
	t.comp_subcategory_id,
	D2.sub_subcategory as comp_sub_subcategory,
	u.comp_sub_subcategory_id,
	D2.seller as seller,
    m.comp_seller_id as seller_id,
	D1.performanceclassification as priority,
    n.priority_id,
	D2.effective_price as comp_effective_price,
	D2.image_url as comp_image_url,
	D2.list_price as comp_list_price,
	D2.product_title as comp_product_title,
	D2.product_url as comp_product_url,
	D2.is_price_changed,
	D2.hour
    from  
   (SELECT *, 'chewy' as tenant, 'chewy' as tenant_source, 'chewy' as tenant_store, 'chewy_chewy' as tenant_source_store, 'chewy' as store_name_display,
		LEAST(COALESCE(trim(offer_price), trim(product_regular_price),trim(product_map_price),trim(product_list_price))) as effective_price
        FROM customeringestion.pricingdata_prod_customer_chewy
        WHERE cast(concat(year, '-', month, '-', day) as date) >=current_date + interval '-30' day
                AND product_published_flag = 'true' and product_discontinued_flag = 'false'
                and product_type= 'Item') D1

    JOIN

    (select a.*, 'chewy' as tenant, a.base_sku as client_sku,
    case when hour = max_hour then True else False end as is_latest_feed,
    case when max(a.effective_price) over(partition by coalesce(a.sku, '')||coalesce(a.source, '')||coalesce(a.store_name, '')||coalesce(a.seller, '')||a.dt)  !=
    min(a.effective_price) over(partition by coalesce(a.sku, '')||coalesce(a.source, '')||coalesce(a.store_name, '')||coalesce(a.seller, '')||a.dt) then True else False end as is_price_changed
    from
    (
    select *, max(hour ) over(partition by coalesce(sku, '')||coalesce(source, '')||coalesce(store_name, '')||coalesce(seller, '')||dt) max_hour
        from chewy_dashboard.chewy_deltachewy_p 
        where cast(dt as date) >= current_date + interval '-30' day
        and scraping_status = 200
    ) a
    ) D2

    ON D1.product_part_number = D2.client_sku and cast(concat(D1.year, '-', D1.month, '-', D1.day) as date) = cast(dt as date) 
 
    join
    
    pricing_analytics.pricing_tenant_source_dim b
    on lower(trim(D1.tenant))=b.tenant_code
    and D1.tenant_source=b.tenant_source
    
    join 
    
    pricing_analytics.pricing_tenant_sku_dim c
    on lower(trim(D1.tenant))=c.tenant_code
    and lower(trim(D1.product_part_number))=c.tenant_sku
    
    join 
    
    pricing_analytics.pricing_tenant_brand_dim d
    on lower(trim(D1.tenant))=d.tenant_code
    and lower(trim(D1.product_purchase_brand))=d.tenant_brand
    
    join
    pricing_analytics.pricing_tenant_source_store_dim e
    on lower(trim(D1.tenant))=e.tenant_code
    and lower(trim(D1.tenant_source_store)) = e.tenant_source_store
    
    
    join
    pricing_analytics.pricing_tenant_store_dim w
    on lower(trim(D1.tenant))=w.tenant_code
    and lower(trim(D1.tenant_store)) = w.tenant_store
    
    join 
    
    pricing_analytics.pricing_tenant_category_dim f
    on lower(trim(D1.tenant))=f.tenant_code
    and lower(trim(D1.product_merch_classification1))=f.tenant_category
    
    join 
    
    pricing_analytics.pricing_tenant_subcategory_dim g
    on lower(trim(D1.tenant))=g.tenant_code
    and lower(trim(D1.product_merch_classification2))=g.tenant_subcategory
    
    join 
    
    pricing_analytics.pricing_tenant_sub_subcategory_dim h
    on lower(trim(D1.tenant))=h.tenant_code
    and lower(trim(D1.product_merch_classification3))=h.tenant_sub_subcategory
    
    join 
    
    pricing_analytics.pricing_tenant_code_dim i
    on lower(trim(D1.tenant))=i.tenant_code
    
    join 
    
    pricing_analytics.pricing_comp_sku_dim j
    on lower(trim(D2.tenant))=j.tenant_code
    and lower(trim(D2.sku))=j.comp_sku
    
    join 
    
    pricing_analytics.pricing_comp_source_dim k
    on lower(trim(D2.tenant))=k.tenant_code
    and lower(trim(D2.source_name))=k.comp_source
    
    join
	pricing_analytics.pricing_comp_store_dim v
	on lower(trim(D2.tenant))=v.tenant_code
	and lower(trim(D2.store_name)) =v.comp_store
    
    join 
    
    pricing_analytics.pricing_comp_brand_dim l
    on lower(trim(D2.tenant))=l.tenant_code
    and lower(trim(D2.brand))=l.comp_brand
    
    join 
    
    pricing_analytics.pricing_comp_seller_dim m
    on lower(trim(D2.tenant))=m.tenant_code
    and lower(trim(D2.seller))=m.comp_seller
    
    join 
    
    pricing_analytics.pricing_priority_dim n
    on lower(trim(D1.tenant))=n.tenant_code
    and lower(trim(D1.performanceclassification))=lower(trim(n.priority))
	
	join
    pricing_analytics.pricing_tenant_upc_dim p
	on lower(trim(D1.tenant))=p.tenant_code
	and lower(trim(D1.product_gtin))= p.tenant_upc
	
	join 
	pricing_analytics.pricing_comp_upc_dim q
	on lower(trim(D2.tenant))=q.tenant_code
	and lower(trim(D2.upc))=q.comp_upc
	
	join 
	pricing_analytics.pricing_comp_source_store_dim r
	on lower(trim(D2.tenant))=r.tenant_code
	and lower(trim(D2.source_store))=r.comp_source_store
	
	
	join 
	pricing_analytics.pricing_comp_category_dim s
	on lower(trim(D2.tenant))=s.tenant_code
	and lower(trim(D2.category))=s.comp_category
	
	join 
	pricing_analytics.pricing_comp_subcategory_dim t
	on lower(trim(D2.tenant))=t.tenant_code
	and lower(trim(D2.subcategory))=t.comp_subcategory
	
	
	join 
	pricing_analytics.pricing_comp_sub_subcategory_dim u 
	on lower(trim(D2.tenant))=u.tenant_code
	and lower(trim(D2.sub_subcategory))=u.comp_sub_subcategory
   
    )
     TO 's3://pricing-analytics/pricing-facts/customer_pricing_data_fact_master/customer_code={}/date={}/' 
    WITH (format = 'PARQUET', partitioned_by = ARRAY['hour'])
    """.format(customer_code,date.today().strftime("%Y-%m-%d"))
    
df1=pd.read_sql(query, athena_conn)
try:
    queryP = "msck repair table pricing_analytics.customer_pricing_data_fact_master"
    df2=pd.read_sql(queryP, athena_conn)
except:
    pass
hours = ['00','02','04','06','08','10','12','14','16','18','20','22']

for hour in hours:
    prefix_objs = bucket1.objects.filter(Prefix="pricing-facts/customer_pricing_data_fact_master/customer_code={}/date={}/hour={}/".format(customer_code,date.today().strftime("%Y-%m-%d"),hour))
    # prefix_objs = bucket1.objects.filter(Prefix="pricing-facts/customer_pricing_data_fact_master/customer_code=chewy/date=2022-09-14/hour={}/".format(hour))
    print("creating file for hour "+hour)
    df = pd.DataFrame()
    for i,obj in enumerate(prefix_objs):
        try:
            key = obj.key
            df1=pd.read_parquet('s3://pricing-analytics/'+key)
            # print(df1.columns)
            print(len(df1))
            if len(df) == 0:
                df= df1
            else:
                df = df.append(df1, ignore_index=True)
            del df1
        except Exception as error:
            print(error)
            print("There was an error reading files from S3 path!")
    if len(df) != 0:
        hdf5_filename = 'matched_product_pricing_feed_{}.hdf5'.format(hour)
        vaex_df = vx.from_pandas(df, copy_index=False)
        vaex_df.export_hdf5(hdf5_filename)
        bucket.upload_file(Filename=hdf5_filename, Key='current/{}/pricing/{}'.format(customer_code,hdf5_filename))
        bucket.upload_file(Filename=hdf5_filename, Key='archive/{}/pricing/{}/{}'.format(customer_code,date.today().strftime("%Y-%m-%d"),hdf5_filename))
        del df
        del vaex_df
        gc.collect()
        os.remove(hdf5_filename)
        
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
send_message_to_slack('#de-etlops-alerts','populate_pricing_feed_master', 'PopulatePricingMasterData: Job Completed')

    