# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 23:41:49 2020

@author: Murali Subramanian
"""

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adaccountuser import AdAccountUser
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign

import os 
import datetime

from airflow import models
from airflow.providers.google.cloud.transfers.facebook_ads_to_gcs import FacebookAdsReportToGcsOperator
from airflow.utils.dates import days_ago

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())


GCS_BUCKET = 'composertogcsfb'
GCS_OBJ_PATH = 'gs://composertogcsfb'
GCS_CONN_ID = 'gcp_default'

FIELDS=[
        AdsInsights.Field.account_id,
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.campaign_name,
        AdsInsights.Field.adset_name,
        AdsInsights.Field.adset_id,
        AdsInsights.Field.ad_name,
        AdsInsights.Field.ad_id,
        AdsInsights.Field.spend,
        AdsInsights.Field.impressions,
        AdsInsights.Field.clicks,
        AdsInsights.Field.actions,
        AdsInsights.Field.conversions
       ]

PARAMS = {
        'level': 'ad',
        'date_preset': 'yesterday'}

default_dag_args = {"start_date": days_ago(1)}

with models.DAG(
    'composer_sample_bq_notify',
    schedule_interval = datetime.timedelta(days = 1),
    default_args = default_dag_args) as dag:

    run_operator = FacebookAdsReportToGcsOperator(
        task_id = 'run_fetch_data',
        start_date = days_ago(1),
        owner = 'airflow',
        bucket_name = GCS_BUCKET,
        params = PARAMS,
        fields = FIELDS,
        gcp_conn_id = GCS_CONN_ID,
        object_name = GCS_OBJ_PATH,
        dag = dag
        )
    
    






