from google.cloud import bigquery
from google.cloud import pubsub_v1
import json
import os
from datetime import date
from datetime import timedelta

client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()

topic_path = os.environ.get("topic_path")
table = os.environ.get("cost_table_name")
project = os.environ.get("project_id")
today = date.today()
costdate = (today - timedelta(days=1)).strftime("%Y-%m-%d")


def get_daily_cost(request):
    query_totalcost = f"""
      SELECT TIMESTAMP_TRUNC(usage_end_time, DAY) as cost_date,ROUND(sum(cost),2) as total_cost 
      FROM `{table}` 
      WHERE TIMESTAMP_TRUNC(usage_end_time, DAY) = TIMESTAMP("{costdate}") AND project.id = "{project}"
      GROUP BY cost_date
      LIMIT 1
  """
    results_totalcost = client.query(query_totalcost)

    output = {}
    for row in results_totalcost:
        output["project"] = project
        output["date"] = row["cost_date"].strftime("%m/%d/%Y")
        output["total_cost"] = row["total_cost"]

    query_costbyservice = f"""
    SELECT service.description as service,ROUND(sum(cost),2) as service_cost,TRUNC(sum(cost)/(SELECT sum(cost) FROM `{table}` WHERE TIMESTAMP_TRUNC(usage_end_time, DAY) = TIMESTAMP("{costdate}") AND project.id = "{project}"),4) as cost_ratio
    FROM `{table}`  
    WHERE TIMESTAMP_TRUNC(usage_end_time, DAY) = TIMESTAMP("{costdate}") AND cost > 0 AND project.id = "{project}"
    GROUP BY service
    ORDER BY service_cost DESC
    LIMIT 10
  """
    results_costbyservice = client.query(query_costbyservice)

    output2 = []
    for row in results_costbyservice:
        item = {}
        item["service"] = row["service"]
        item["cost"] = row["service_cost"]
        item["cost_ratio"] = row["cost_ratio"]
        output2.append(item)

    output["top10_service"] = output2
    print(output)

    # Data must be a bytestring
    data = json.dumps(output)
    data = data.encode("utf-8")
    future = publisher.publish(topic_path, data)

    print(f"Published messages to {topic_path}.")
    return output
