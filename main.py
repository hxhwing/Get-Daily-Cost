from google.cloud import bigquery
from google.cloud import pubsub_v1
import json
import os
from datetime import date
from datetime import timedelta

client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()

# topic_path = os.environ.get("topic_path")
table = os.environ.get("cost_table_name")
# project = os.environ.get("project_id")
today = date.today()
costdate = (today - timedelta(days=1)).strftime("%Y-%m-%d")


def query_cost(project):
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
    return output


def get_daily_cost(request):
    if "Project-IDs" not in request.headers:
        return "Please specify project ids in HTTP header Project-IDs, separate with commas"
    project_ids = request.headers.get("Project-IDs")
    project_list = project_ids.split(",")
    message = []

    for project in project_list:
        response = query_cost(project)
        message.append(response)

    if "Topic-Name" in request.headers:
        topic_path = request.headers.get("Topic-Name")
        # Data must be a bytestring
        data = json.dumps(message)
        data = data.encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(f"Published messages to {topic_path}.")
    else:
        print("No 'Topic-Name' header found, if you need to send response through email, please specify Pub/Sub topic name in HTTP header Topic-Name in this format: projects/project-id/topics/topic-name")

    return message
