# Databricks notebook source
import requests
from datetime import datetime
import pytz
import json
from dotenv import dotenv_values
import os

# COMMAND ----------

def datetime_to_cron(dt: datetime) -> str:
    return dt.strftime(f'{dt.second} {dt.minute} {dt.hour} * * ?')

# COMMAND ----------

secrets = dotenv_values("/Workspace/Users/jordan@jordanmartinetti.com/.env")

# COMMAND ----------

url = 'https://api.openweathermap.org/data/2.5/weather?lat='+secrets['LAT']+'&lon='+secrets['LON']+'&appid='+secrets['API_KEY']
response = requests.get(url)
weather = response.json()


# COMMAND ----------

sunset_utc = weather["sys"]["sunset"]
local_timezone = pytz.timezone("US/Central")
sunset_utc_formatted = datetime.fromtimestamp(sunset_utc,local_timezone)

# COMMAND ----------

databricks_instance = secrets['DB_INSTANCE']
api_token = secrets['DB_API_TOKEN'] #apiToken
job_id = secrets['DB_JOB_ID'] #Your notification job id which is created separately.
cron_expression = datetime_to_cron(sunset_utc_formatted)

# COMMAND ----------

payload = {
    "job_id":job_id,
    "new_settings":{
        "schedule": {
        "quartz_cron_expression": cron_expression,
        "timezone_id": "US/Central",  # Set the timezone
        "pause_status": "UNPAUSED"
    }
    }
}

headers = {
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json"
}

# COMMAND ----------

url = f"{databricks_instance}/api/2.1/jobs/update"
response = requests.post(url, headers=headers, data=json.dumps(payload))

# COMMAND ----------

# Check the response
if response.status_code == 200:
    print(f"Job scheduled at {sunset_utc_formatted.strftime('%Y-%m-%d %H:%M:%S')}.")
else:
    print(f"Failed to update job schedule: {response.status_code}")
    print(response.json())
