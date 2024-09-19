import json
import msal
import requests
from lxml import etree
import re
import base64
import html
import pandas as pd
import sqlalchemy as sa
import pyodbc


def parse_ssis(filepath):
    tree = etree.parse(filepath)
    namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts', 'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'}
    merges = tree.xpath(f"//components/component[@componentClassID='Microsoft.MergeJoin']/@name")
    lookups = tree.xpath("//components/component[@componentClassID='Microsoft.Lookup']/@name")
    sql_task = tree.xpath("//DTS:Executable[@DTS:ExecutableType='Microsoft.ExecuteSQLTask']/@DTS:ObjectName", namespaces=namespaces)
    if merges:
        print(merges)
        return 'merge'
    elif lookups:
        print(lookups)
        return 'lookup'
    elif sql_task:
        print(sql_task)
        return 'copy-procedure'
    else:
        return 'copy'

def parse_procedure_dataflow(filepath):
    tree = etree.parse(filepath)
    namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts', 'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'}
    details ={}
    source_table = tree.xpath("//components/component[contains(@name,'Source')]/properties/property[@name='OpenRowset']/text()")
    destination_table = tree.xpath("//components/component[contains(@name,'Destination')]/properties/property[@name='OpenRowset']/text()")

    matches = re.findall(r'\[([^\]]+)\]', source_table[0])
    details["schema"] = matches[0]
    details["source_table"] = matches[1]
    matches = re.findall(r'\[([^\]]+)\]', destination_table[0])
    details["destination_table"] = matches[-1]

    # Extracting SQL Tasks
    sql_tasks = tree.xpath("//DTS:Executable[@DTS:ExecutableType='Microsoft.ExecuteSQLTask']/DTS:ObjectData/SQLTask:SqlTaskData", namespaces=namespaces)
    
    if sql_tasks is not None:
        for sql_task in sql_tasks:
            sql_statement = sql_task.xpath("@SQLTask:SqlStatementSource", namespaces=namespaces)
            if sql_statement:
                sql_text = html.unescape(sql_statement[0]).strip()
                if sql_text.startswith("EXEC"):
                    # Extract the procedure name without 'EXEC' and the trailing semicolon
                    procedure_name = sql_text[5:].rstrip(';').strip()
                    details["procedure_name"] = procedure_name
    else:
        details["procedure_name"] = ""
    return details

def parse_plain_dataflow(filepath):
    tree = etree.parse(filepath)
    details = {}    
    source_table = tree.xpath("//components/component[contains(@name,'Source')]/properties/property[@name='OpenRowset']/text()")
    destination_table = tree.xpath("//components/component[contains(@name,'Destination')]/properties/property[@name='OpenRowset']/text()")
    
    matches = re.findall(r'\[([^\]]+)\]', source_table[0])
    details["schema"] = matches[0]
    details["source_table"] = matches[1]
    matches = re.findall(r'\[([^\]]+)\]', destination_table[0])
    details["destination_table"] = matches[-1]
    return details

def parse_mergejoin_dataflow(filepath):
    tree = etree.parse(filepath)
    namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts'}
    details = {}
    details["sources"] = []
    source_tables = tree.xpath("//components/component[contains(@description,'Source')]/properties/property[@name='OpenRowset']/text()")
    destination_table = tree.xpath("//components/component[contains(@description,'Destination')]/properties/property[@name='OpenRowset']/text()")
    
    table =[]
    for source_table in source_tables:
        matches = re.findall(r'\[([^\]]+)\]', source_table)
        details["sources"].append({"schema":matches[0], "table": matches[1]})
        table.append([matches[0], matches[1]])
    
    matches = re.findall(r'\[([^\]]+)\]', destination_table[0])
    details["destination"] = {"schema":matches[0], "table":matches[1]}

    sort_columns = []
    sort_orders = []
    sort_component_names = tree.xpath("//components/component[contains(@componentClassID,'Microsoft.Sort')]/@name")
    for name in sort_component_names:
        column_texts = tree.xpath(f"//components/component[@name='{name}']/outputs/output/outputColumns/outputColumn/properties/property/text()")
        pattern = r'Columns\[(.*?)\]\}'
        columns = []
        for column_text in column_texts:
            match = re.search(pattern, column_text)
            columns.append(match.group(1))
        sort_columns.append(columns)
    set1 = set(sort_columns[0])
    set2 = set(sort_columns[1])

    left_order = {}
    left_sorts = tree.xpath(f"//components/component[@componentClassID='Microsoft.MergeJoin']/inputs/input[contains(@name,'Left Input')]/inputColumns/inputColumn/@cachedSortKeyPosition")
    left_columns = tree.xpath(f"//components/component[@componentClassID='Microsoft.MergeJoin']/inputs/input[contains(@name,'Left Input')]/inputColumns/inputColumn/@refId")
    for i in range(len(left_sorts)):
        left_order[int(left_sorts[i])] = left_columns[i].split(".Columns[")[-1][:-1]

    right_order = {}
    right_sorts = tree.xpath(f"//components/component[@componentClassID='Microsoft.MergeJoin']/inputs/input[contains(@name,'Right Input')]/inputColumns/inputColumn/@cachedSortKeyPosition")
    right_columns = tree.xpath(f"//components/component[@componentClassID='Microsoft.MergeJoin']/inputs/input[contains(@name,'Right Input')]/inputColumns/inputColumn/@refId")
    for i in range(len(right_sorts)):
        right_order[int(right_sorts[i])] = right_columns[i].split(".Columns[")[-1][:-1]

    outputs = tree.xpath(f"//components/component[@componentClassID='Microsoft.MergeJoin']/outputs/output/outputColumns/outputColumn/@refId")
    colnames = []
    for output in outputs:
        colnames.append(output.split(".Columns[")[-1][:-1])

    joins = {2:"INNER JOIN", 1:"LEFT JOIN", 0:"OUTER JOIN"}
    merge = tree.xpath(f"//components/component[@componentClassID='Microsoft.MergeJoin']/properties/property[@typeConverter='JoinType']/text()")
    join = joins[int(merge[0])]

    query = "SELECT "
    for column in colnames:
        if column in set1:
            query = query + f"t1.{column}, "
        else:
            query = query + f"t2.{column}, "
    query = query[:-2] + f" FROM Lakehouse.{table[1][0]}.{table[1][1]} AS t1 {join} Lakehouse.{table[0][0]}.{table[0][1]} AS t2 ON t1.{left_order[1]} = t2.{right_order[1]};"
    return query, details

def parse_lookup_dataflow(filepath):
    tree = etree.parse(filepath)
    namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts'}

    details = {}
    source_tables = tree.xpath("//components/component[contains(@description,'Source')]/properties/property[@name='OpenRowset']/text()")
    destination_table = tree.xpath("//components/component[contains(@description,'Destination')]/properties/property[@name='OpenRowset']/text()")
    
    matches = re.findall(r'\[([^\]]+)\]', source_tables[0])
    details["source"] = {"schema":matches[0], "table": matches[1]}
    
    source_schema = matches[0]
    source_table = matches[1]
    matches = re.findall(r'\[([^\]]+)\]', destination_table[0])
    details["destination"] = {"schema":matches[0], "table":matches[1]}

    columns =  tree.xpath("//components/component[contains(@description,'Source')]/outputs/output[contains(@name,'Source Output')]/outputColumns/outputColumn/@name")

    lookups = tree.xpath("//components/component[@componentClassID='Microsoft.Lookup']/properties/property[@name='SqlCommand']/text()")
    pattern = r"FROM\s+\[([^\]]+)\]\.\[([^\]]+)\]"
    match = re.search(pattern, lookups[0], re.IGNORECASE)
    lookup_schema = match.group(1)
    lookup_table = match.group(2)
    details["lookup"] = {"schema":lookup_schema, "table":lookup_table}
    
    join_col1 = tree.xpath("//components/component[@componentClassID='Microsoft.Lookup']/inputs/input/inputColumns/inputColumn/@cachedName")
    join_col2 = tree.xpath("//components/component[@componentClassID='Microsoft.Lookup']/inputs/input/inputColumns/inputColumn/properties/property[@name='JoinToReferenceColumn']/text()")

    ref_cols = tree.xpath("//components/component[@componentClassID='Microsoft.Lookup']/outputs/output/outputColumns/outputColumn/properties/property[@name='CopyFromReferenceColumn']/text()")

    query = "SELECT "
    for col in columns:
        query = query + f"t1.{col}, "
    for col in ref_cols:
        query = query + f"t2.{col}, "
    query = query[:-2] + f" FROM Lakehouse.{source_schema}.{source_table} AS t1 JOIN Lakehouse.{lookup_schema}.{lookup_table} AS t2 ON t1.{join_col1[0]} = t2.{join_col2[0]}"
    return query, details

def encode_json_to_base64(file_path):
    with open(file_path, "rb") as file:
        file_content = file.read()
        base64_encoded_data = base64.b64encode(file_content)
        base64_string = base64_encoded_data.decode("utf-8")
    return base64_string


def create_token(client_id, authority, scope):
    # Acquire a token for Fabric APIs
    app = msal.PublicClientApplication(client_id, authority=authority)
    username = "vivek.goli@kanerika.com"
    password = "Vivek@16"
    result = app.acquire_token_by_username_password(username=username, password=password, scopes=scope)

    if "access_token" in result:
        access_token = result["access_token"]
        print("Access token acquired")
        return access_token
    else:
        raise ValueError("Failed to acquire token. Err: %s" % result)

def create_pipeline(access_token, workspace_id, pipeline_json):
    base_url = "https://api.fabric.microsoft.com/v1/"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(f"{base_url}workspaces/{workspace_id}/items", headers=headers, data=pipeline_json)
    
    if response.status_code == 201:
        print("Pipeline created successfully")
        print(response.json())
    else:
        print(f"Failed to create pipeline. Status code: {response.status_code}")
        print(response.json())

def get_workspaceid(access_token, name):
    base_url = "https://api.fabric.microsoft.com/v1/"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    response = requests.get(f"{base_url}workspaces", headers=headers)
    response_body = response.json()
    ws_map = {}
    for value in response_body["value"]:
        ws_map[value["displayName"]] = value["id"]
        #print(value["id"], value["displayName"])
    
    print("Workspace", name, ws_map[name])
    return ws_map[name]

def get_warehouseid(access_token, workspace_id, name):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    wh_map = {}

    if response.status_code == 200:
        artifacts = response.json().get('value', [])
        warehouses = [artifact for artifact in artifacts if artifact['type'] == 'Warehouse']
        if warehouses:
            for warehouse in warehouses:
                wh_map[warehouse['displayName']] = warehouse['id']
        else:
            print(f"No warehouses found in workspace ID '{workspace_id}'.")
    else:
        print(f"Failed to retrieve warehouses. Status code: {response.status_code}")
        print("Response:", response.text)
    print("Warehouse:", name, wh_map.get(name, None))
    return wh_map.get(name, None)

def get_lakehouseid(access_token, workspace_id, name):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    lh_map = {}

    if response.status_code == 200:
        artifacts = response.json().get('value', [])
        lakehouses = [artifact for artifact in artifacts if artifact['type'] == 'Lakehouse']
        if lakehouses:
            for lakehouse in lakehouses:
                lh_map[lakehouse['displayName']] = lakehouse['id']
        else:
            print(f"No lakehouses found in workspace ID '{workspace_id}'.")
    else:
        print(f"Failed to retrieve lakehouses. Status code: {response.status_code}")
        print("Response:", response.text)
    
    print("Lakehouse: ", name, lh_map[name])
    return lh_map[name]

def create_json_copydata(pipeline_name, workspace_id, artifact_id, lakehouse_folder, destination_table):
    with open(f"json_templates/copydata_template.json", "r") as file:
        fabric_json = json.load(file)
    fabric_json["name"] = pipeline_name
        
    fabric_json["properties"]["activities"][0]["typeProperties"]["sink"]["datasetSettings"]["linkedService"]["properties"]["typeProperties"] = {
        "workspaceId": workspace_id,
        "artifactId": artifact_id,
        "rootFolder": lakehouse_folder
    }
    fabric_json["properties"]["activities"][0]["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]["table"] = destination_table
    with open(f"json_templates/copydata_template.json", "w") as json_file:
        json.dump(fabric_json, json_file, indent=4)
    print(f"Pipeline JSON file created successfully.")


def create_json_copydata_procedure(pipeline_name, workspace_id, artifact_id, warehouse, destination_table, procedure, endpoint):
    with open(f"json_templates/copydata_procedure_template.json", "r") as file:
        fabric_json = json.load(file)
    fabric_json["name"] = pipeline_name
    warehouse_details = {
        "endpoint": endpoint,
        "artifactId": artifact_id,
        "workspaceId": workspace_id
    }

    fabric_json["properties"]["activities"][0]["typeProperties"]["sink"]["datasetSettings"]["linkedService"]["name"] = warehouse
    fabric_json["properties"]["activities"][0]["typeProperties"]["sink"]["datasetSettings"]["linkedService"]["properties"]["typeProperties"] = warehouse_details        
    fabric_json["properties"]["activities"][0]["typeProperties"]["sink"]["datasetSettings"]["typeProperties"] = {
        "schema": "dbo",
        "table": destination_table
    }

    procedure_activity = fabric_json["properties"]["activities"][1]
    procedure_activity["typeProperties"]["storedProcedureName"] = procedure
    procedure_activity["linkedService"]["name"] = warehouse
    procedure_activity["linkedService"]["objectId"] = artifact_id
    procedure_activity["linkedService"]["properties"]["typeProperties"] = warehouse_details
    fabric_json["properties"]["activities"][1] = procedure_activity

    with open(f"json_templates/copydata_procedure_template.json", "w") as json_file:
        json.dump(fabric_json, json_file, indent=4)
    print(f"Pipeline JSON file created successfully.")

def create_json_mergejoin(pipeline_name, source, workspace_id, lakehouse_id, warehouse, warehouse_id, endpoint, procedure):
    with open(f"json_templates/merge_template.json", "r") as file:
        fabric_json = json.load(file)

    fabric_json["name"] = pipeline_name
    for i in range(2):
        fabric_json["properties"]["activities"][i]["typeProperties"]["sink"]["datasetSettings"]["linkedService"]["properties"]["typeProperties"] = {
            "workspaceId": workspace_id,
            "artifactId": lakehouse_id,
            "rootFolder": "Tables"
        }
        fabric_json["properties"]["activities"][i]["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]["table"] = source[i]

    procedure_activity = fabric_json["properties"]["activities"][3]
    procedure_activity["typeProperties"]["storedProcedureName"] = procedure
    procedure_activity["linkedService"]["name"] = warehouse
    procedure_activity["linkedService"]["objectId"] = warehouse_id
    procedure_activity["linkedService"]["properties"]["typeProperties"] = {
        "endpoint": endpoint,
        "artifactId": warehouse_id,
        "workspaceId": workspace_id
    }

    with open(f"json_templates/merge_template.json", "w") as json_file:
        json.dump(fabric_json, json_file, indent=4)
    print(f"Pipeline JSON file created successfully.")

def create_json_lookup(pipeline_name, source, workspace_id, lakehouse_id, warehouse, warehouse_id, endpoint, procedure):
    with open(f"json_templates/lookup_template.json", "r") as file:
        fabric_json = json.load(file)

    fabric_json["name"] = pipeline_name
    for i in range(2):
        fabric_json["properties"]["activities"][i]["typeProperties"]["sink"]["datasetSettings"]["linkedService"]["properties"]["typeProperties"] = {
            "workspaceId": workspace_id,
            "artifactId": lakehouse_id,
            "rootFolder": "Tables"
        }

        fabric_json["properties"]["activities"][i]["typeProperties"]["sink"]["datasetSettings"]["typeProperties"]["table"] = source[i]

    procedure_activity = fabric_json["properties"]["activities"][3]
    procedure_activity["typeProperties"]["storedProcedureName"] = procedure
    procedure_activity["linkedService"]["name"] = warehouse
    procedure_activity["linkedService"]["objectId"] = warehouse_id
    procedure_activity["linkedService"]["properties"]["typeProperties"] = {
        "endpoint": endpoint,
        "artifactId": warehouse_id,
        "workspaceId": workspace_id
    }

    with open(f"json_templates/lookup_template.json", "w") as json_file:
        json.dump(fabric_json, json_file, indent=4)
    print(f"Pipeline JSON file created successfully.")


def create_procedure_fabric(server, database, sql_query):
    user = "vivek.goli@kanerika.com"
    password = "Vivek@16"
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Authentication=ActiveDirectoryPassword;"
        f"UID={user};"
        f"PWD={password}"
    )

    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            conn.commit()
            print("Stored procedure created successfully.")

def create_join_procedure(procedure_name, destination_table, query):
    return f"""
                CREATE PROCEDURE {procedure_name}
                AS
                BEGIN
                    IF OBJECT_ID('{destination_table}', 'U') IS NOT NULL
                    BEGIN
                        DROP TABLE {destination_table};
                    END

                    CREATE TABLE {destination_table} AS
                    {query}
                END;

            """
