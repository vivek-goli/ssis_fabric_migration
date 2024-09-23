import json
import msal
import requests
from lxml import etree
import re
import base64
import pyodbc

class SSIS_Fabric:
    #class variables
    client_id = "c657b643-3d0b-42a2-8680-2d05f60ffef0"
    tenant_id = "ea8bd1fd-ac34-4ae2-b421-6cfa2fcff243"
    scope = ['https://api.fabric.microsoft.com/.default']
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    username = "vivek.goli@kanerika.com"
    password = "Vivek@16"

    #initialize the instance variables
    def __init__(self, workspace, lakehouse, warehouse, pipeline_name, warehouse_endpoint):
        self.workspace = workspace
        self.lakehouse = lakehouse
        self.warehouse = warehouse
        self.pipeline_name = pipeline_name
        self.endpoint = warehouse_endpoint
        self.workspace_id = ""
        self.lakehouse_id = ""
        self.warehouse_id = ""
        self.access_token = ""
        self.component_map = {}
        self.flows = {}
        self.dependency_map = {}
        self.executables = {}
        self.counts = {"copy":1, "procedure":1, "wait":1}
    
    def create_token(self):
        # Acquire a token for Fabric APIs
        app = msal.PublicClientApplication(self.client_id, authority=self.authority)
        username = self.username
        password = self.password
        result = app.acquire_token_by_username_password(username=username, password=password, scopes=self.scope)

        if "access_token" in result:
            self.access_token = result["access_token"]
            print("Access token acquired")
            return self.access_token
        else:
            raise ValueError("Failed to acquire token. Err: %s" % result)

    def get_workspace_id(self):
        base_url = "https://api.fabric.microsoft.com/v1/"
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }
        response = requests.get(f"{base_url}workspaces", headers=headers)
        response_body = response.json()
        ws_map = {}
        for value in response_body["value"]:
            ws_map[value["displayName"]] = value["id"]
        
        print("Workspace", self.workspace, ws_map[self.workspace])
        self.workspace_id = ws_map[self.workspace]
    
    def get_warehouse_id(self):
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{self.workspace_id}/warehouses"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
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
                print(f"No warehouses found in workspace ID '{self.workspace_id}'.")
        else:
            print(f"Failed to retrieve warehouses. Status code: {response.status_code}")
            print("Response:", response.text)
        print("Warehouse:", self.warehouse, wh_map.get(self.warehouse, None))
        self.warehouse_id = wh_map.get(self.warehouse, None)

    def get_lakehouse_id(self):
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{self.workspace_id}/lakehouses"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
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
                print(f"No lakehouses found in workspace ID '{self.workspace_id}'.")
        else:
            print(f"Failed to retrieve lakehouses. Status code: {response.status_code}")
            print("Response:", response.text)
        
        print("Lakehouse: ", self.lakehouse, lh_map[self.lakehouse])
        self.lakehouse_id = lh_map[self.lakehouse]

    @staticmethod 
    def get_input_columns_for_merge(inputs): #returns the input column names for one side input of merge join
        cols = []
        sort_map = {}
        for i in range(len(inputs)):
            col_name = inputs[i].xpath("@cachedName")[0]
            cols.append(col_name)
            sortid = inputs[i].xpath("@cachedSortKeyPosition")
            if len(sortid) > 0:
                sort_map[int(sortid[0])] = col_name
        return cols, sort_map

    @staticmethod
    def parse_source_component(dataflow, name): # returns the table name from the source, same name is used as destination in copy acitvity
        component = dataflow.xpath(f"//components/component[@name='{name}']")[0]
        source = component.xpath("properties/property[@name='OpenRowset']/text()")[0]
        matches = re.findall(r'\[([^\]]+)\]', source)
        return matches[1]
    
    @staticmethod
    def parse_destination_component(dataflow, name): # returns the final destination table name
        component = dataflow.xpath(f"//components/component[@name='{name}']")[0]
        destination = component.xpath("properties/property[@name='OpenRowset']/text()")[0]
        matches = re.findall(r'\[([^\]]+)\]', destination)
        return matches[-1]
    
    @staticmethod
    def parse_execsql(execsql, name): # returns the procedure name that is executed in this task
        namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts', 'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'}
        statement = execsql.xpath("//DTS:ObjectData/SQLTask:SqlTaskData/@SQLTask:SqlStatementSource", namespaces=namespaces)[0]
        procedure_name = statement[5:]
        return procedure_name

    @staticmethod
    def parse_merge(dataflow, name, table1, table2): # returns the query required to join the two tables and create new table
        merge = dataflow.xpath(f"//components/component[@name='{name}']")[0]
        print(merge.xpath("@name")[0])
        joins = {0:"OUTER JOIN", 1:"LEFT JOIN", 2:"INNER JOIN"}
        namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts'}
        joinid = int(merge.xpath("properties/property[@name='JoinType']/text()")[0])
        key_count = int(merge.xpath("properties/property[@name='NumKeyColumns']/text()")[0])
        join = joins[joinid]

        right_inputs = merge.xpath("inputs/input[@name='Merge Join Right Input']/inputColumns/inputColumn")
        left_inputs = merge.xpath("inputs/input[@name='Merge Join Left Input']/inputColumns/inputColumn")

        left_cols, left_sort = SSIS_Fabric.get_input_columns_for_merge(left_inputs)
        right_cols, right_sort = SSIS_Fabric.get_input_columns_for_merge(right_inputs)

        output_cols = merge.xpath("outputs/output[@name='Merge Join Output']/outputColumns/outputColumn/@name")
        print(name)
        print("Left:", left_cols, left_sort)
        print("Right:", right_cols, right_sort)
        print("Output:", output_cols)
        query = "SELECT "
        for col in output_cols:
            if col in left_cols:
                query = query + f"t1.{col}, "
            elif col in right_cols:
                query = query + f"t2.{col}, "
        query = query[:-2] + f" FROM schema.{table1} AS t1 {join} schema.{table2} AS t2 ON t1.{left_sort[1]} = t2.{right_sort[1]}"

        if key_count > 1:
            query = query + " WHERE "
            for i in range(2, key_count + 1):
                if i > 2:
                    query = query + f" AND t1.{left_sort[i]} = t2.{right_sort[i]}"
                else:
                    query = query + f" t1.{left_sort[i]} = t2.{right_sort[i]}"
            query += ";"
        else:
            query += ";"    
        return query

    @staticmethod    
    def parse_lookup(dataflow, name, table1, columns): # returns the query required to join the two tables and create new table
        lookup = dataflow.xpath(f"//components/component[@name='{name}']")[0]
        lookup_details = lookup.xpath("properties/property[@name='SqlCommand']/text()")
        pattern = r"FROM\s+\[([^\]]+)\]\.\[([^\]]+)\]"
        match = re.search(pattern, lookup_details[0], re.IGNORECASE)
        lookup_schema = match.group(1)
        lookup_table = match.group(2)

        join_col1 = lookup.xpath("inputs/input/inputColumns/inputColumn/@cachedName")
        join_col2 = lookup.xpath("inputs/input/inputColumns/inputColumn/properties/property[@name='JoinToReferenceColumn']/text()")
        ref_cols = lookup.xpath("outputs/output/outputColumns/outputColumn/properties/property[@name='CopyFromReferenceColumn']/text()")

        print(name, columns)
        query = "SELECT "
        for col in columns:
            query = query + f"t1.{col}, "
        for col in ref_cols:
            query = query + f"t2.{col}, "
        query = query[:-2] + f" FROM schema.{table1} AS t1 JOIN schema.{lookup_table} AS t2 ON t1.{join_col1[0]} = t2.{join_col2[0]}"
        return lookup_table, query
    
    def get_columns_for_lookup(self, dataflow, component_name):
        component = dataflow.xpath(f"//components/component[@name='{component_name}']")[0]
        if self.component_map[component_name][0] == "Microsoft.Lookup":
            prev_comp = self.dependency_map[component_name][0]
            self.get_columns_for_lookup(dataflow, prev_comp)
        else:
            columns =  component.xpath("outputs/output[contains(@name,'Source Output')]/outputColumns/outputColumn/@name")
            return columns

    def copy_activity_json(self, schema, table_name, activity_name): # creates the json for the copy activity from source to stage schema in warehouse
        with open("activity_templates/pipeline.json", "r") as file:
            pipeline = json.load(file)

        with open(f"activity_templates/copyactivity_wh.json", "r") as file:
            copy = json.load(file)
        warehouse_details = {
            "endpoint": self.endpoint,
            "artifactId": self.warehouse_id,
            "workspaceId": self.workspace_id
        }
        copy["name"] = activity_name
        copy["typeProperties"]["sink"]["datasetSettings"]["linkedService"]["name"] = self.warehouse
        copy["typeProperties"]["sink"]["datasetSettings"]["linkedService"]["properties"]["typeProperties"] = warehouse_details        
        copy["typeProperties"]["sink"]["datasetSettings"]["typeProperties"] = {
            "schema": schema,
            "table": table_name
        }

        pipeline["properties"]["activities"] += [copy]
        with open(f"activity_templates/pipeline.json", "w") as json_file:
            json.dump(pipeline, json_file, indent=4)        
        print("\nJSON for copy activity created successfully.")

    def procedure_json(self, procedure_name, activity_name, depends): # creates a json for the storedprocedure acitivity for merge join / lookup
        with open("activity_templates/pipeline.json", "r") as file:
            pipeline = json.load(file)
        with open("activity_templates/wait.json", "r") as file:
            wait = json.load(file)
        with open(f"activity_templates/procedure_activity.json", "r") as file:
            procedure = json.load(file)
        
        wait_name = f"Wait{self.counts["wait"]}"
        wait["name"] = wait_name
        for d in depends:
            wait["dependsOn"] += [{"activity": d, "dependencyConditions": ["Succeeded"]}]
        self.counts["wait"] += 1
        warehouse_details = {
            "endpoint": self.endpoint,
            "artifactId": self.warehouse_id,
            "workspaceId": self.workspace_id
        }
        procedure["name"] = activity_name
        procedure["dependsOn"] = [{"activity": wait_name, "dependencyConditions": ["Succeeded"]}]
        procedure["typeProperties"]["storedProcedureName"] = procedure_name
        procedure["linkedService"]["name"] = self.warehouse
        procedure["linkedService"]["objectId"] = self.warehouse_id
        procedure["linkedService"]["properties"]["typeProperties"] = warehouse_details

        pipeline["properties"]["activities"] += [wait, procedure]

        with open(f"activity_templates/pipeline.json", "w") as json_file:
            json.dump(pipeline, json_file, indent=4)
        print("\nJSON for stored procedure activity created successfully.")

    @staticmethod
    def design_procedure(procedure_name, destination_table, query):
        return f"""
                    CREATE PROCEDURE {procedure_name} AS
                    BEGIN
                        IF OBJECT_ID('{destination_table}', 'U') IS NOT NULL
                        BEGIN
                            DROP TABLE {destination_table};
                        END
                        CREATE TABLE {destination_table} AS
                        {query}
                    END;
                """

    def create_procedure_fabric(self, sql_query):
        user = "vivek.goli@kanerika.com"
        password = "Vivek@16"
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={self.endpoint};"
            f"DATABASE={self.warehouse};"
            "Authentication=ActiveDirectoryPassword;"
            f"UID={user};"
            f"PWD={password}"
        )

        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_query)
                conn.commit()
                print("Stored procedure created successfully.")

    @staticmethod
    def encode_json_to_base64():
        with open("activity_templates/pipeline.json", "rb") as file:
            file_content = file.read()
            base64_encoded_data = base64.b64encode(file_content)
            base64_string = base64_encoded_data.decode("utf-8")
        return base64_string

    @staticmethod
    def create_payload_json(pipeline_name, encoded_json): # creates the payload that is pushed to fabric via API
        pipeline = {
            "displayName": pipeline_name,
            "type": "DataPipeline",
            "definition": {
                "parts": [
                    {
                        "path": "pipeline.json",
                        "payload": encoded_json,
                        "payloadType": "InlineBase64"
                    }
                ]
            }
        }
        with open(f"activity_templates/payload.json", "w") as json_file:
            json.dump(pipeline, json_file, indent=4)

    def create_pipeline(self, pipeline_json):
        base_url = "https://api.fabric.microsoft.com/v1/"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        response = requests.post(f"{base_url}workspaces/{self.workspace_id}/items", headers=headers, data=pipeline_json)
        if response.status_code == 201:
            print("Pipeline created successfully")
            print(response.json())
        else:
            print(f"Failed to create pipeline. Status code: {response.status_code}")
            print(response.json())

    def parse_dataflow(self, dataflow, name):    
        namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts', 'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'}
        task_name = dataflow.xpath("@DTS:ObjectName", namespaces=namespaces)[0]
        print(f"Data Flow Task: {task_name}")
        components = dataflow.xpath(f"//components/component")
        for component in components:
            component_class = component.xpath("@componentClassID")[0]
            component_name = component.xpath("@name")[0]
            self.component_map[component_name] = [component_class, False]
            if "Source" in component_class:
                table = SSIS_Fabric.parse_source_component(dataflow, component_name)
                self.component_map[component_name] = self.component_map[component_name] + [table]
            self.flows[component_name] = []
            self.dependency_map[component_name] = []
        print("\nComponent map: ", self.component_map)

        data_paths = dataflow.xpath("//paths/path")
        for path in data_paths:
            source_id = path.xpath("@startId")[0]
            destination_id = path.xpath("@endId")[0]
            start_comp = source_id[source_id.find("\\")+1: source_id.find(".")].split("\\")[1]
            end_comp = destination_id[destination_id.find("\\")+1: destination_id.find(".")].split("\\")[1]

            self.flows[start_comp] = self.flows[start_comp] + [[end_comp, self.component_map[end_comp][0]]]
            self.dependency_map[end_comp] = self.dependency_map[end_comp] + [start_comp]
        print("\nFlows: ", self.flows)
        print("\nDependencies: ", self.dependency_map)
        print()

    # driving function 2
    def parse_components(self, dataflow, dataflow_name):
        count = 0
        completed = []
        while count < len(self.component_map.keys()):
            for name, comp_type in self.component_map.items():
                if self.component_map[name][1] == True:
                    if name not in completed:
                        completed = completed + [name]
                    count = len(completed)

                elif "Source" in self.component_map[name][0]:
                    next_comp_type = self.flows[name][0][1]
                    next_comp_name = self.flows[name][0][0]
                    activity_name = f"CopyActivity{self.counts["copy"]}"
                    if (next_comp_type == "Microsoft.Sort" and self.flows[next_comp_name][0][1] == "Microsoft.MergeJoin") or next_comp_type == "Microsoft.Lookup":
                        table_name = SSIS_Fabric.parse_source_component(dataflow, name) # get source table name
                        # self.copy_activity_json("stage", table_name, activity_name)
                    elif "Destination" in next_comp_name:
                        table_name = SSIS_Fabric.parse_destination_component(dataflow, next_comp_name)
                        self.copy_activity_json("main", table_name, activity_name)
                        # self.executables[dataflow_name] += [activity_name]
                    self.component_map[name] = self.component_map[name] + [activity_name] # add activity name
                    self.component_map[name][1] = True
                    self.counts["copy"] += 1
                
                elif self.component_map[name][0] == "Microsoft.Sort":
                    if self.component_map[self.dependency_map[name][0]][1] == True:
                        self.component_map[name][1] = True
                        self.component_map[name] = self.component_map[name] + [self.component_map[self.dependency_map[name][0]][2]] # add output table name to component_map
                        self.component_map[name] = self.component_map[name] + [self.component_map[self.dependency_map[name][0]][3]]
                
                elif self.component_map[name][0] == "Microsoft.MergeJoin":
                    d1 = self.dependency_map[name][0]
                    d2 = self.dependency_map[name][1]
                    if self.component_map[d1][1] and self.component_map[d2][1]:
                        t1 = self.component_map[d1][2]
                        t2 = self.component_map[d2][2]
                        query = SSIS_Fabric.parse_merge(dataflow, name, t1, t2)
                        activity_name = f"StoredProcedure{self.counts["procedure"]}"
                        dest_table = ""
                        if "Destination" in self.flows[name][0][1]:
                            dest_table = SSIS_Fabric.parse_destination_component(dataflow, self.flows[name][0][0])
                            self.executables[dataflow_name] += [activity_name]
                        else:
                            self.component_map[name] = self.component_map[name] + [f"{t1}_{t2}"] # add output table name to component_map
                            dest_table = f"{t1}_{t2}"
                        query = query.replace("schema", "stage")
                        # print(query)
                        
                        procedure_name = f"Merge_{dest_table}"
                        procedure = SSIS_Fabric.design_procedure(procedure_name, dest_table, query)
                        print("Procedure: ", procedure)
                        #self.create_procedure_fabric(procedure)
                        activity1 = self.component_map[d1][3]
                        activity2 = self.component_map[d2][3]
                        # self.procedure_json(procedure_name, activity_name, [activity1, activity2])
                        self.component_map[name] += [activity_name]
                        self.component_map[name][1] = True
                        self.counts["procedure"] += 1
                
                elif self.component_map[name][0] == "Microsoft.Lookup":
                    dependency = self.dependency_map[name][0]
                    if self.component_map[dependency][1] == True:
                        t1 = self.component_map[dependency][2]
                        columns = self.get_columns_for_lookup(dataflow, dependency)
                        t2, query = SSIS_Fabric.parse_lookup(dataflow, name, t1, columns)

                        copy_name = f"CopyActivity{self.counts["copy"]}"
                        self.copy_activity_json("stage", t2, copy_name)
                        activity_name = f"StoredProcedure{self.counts["procedure"]}"
                        dest_table = ""
                        if "Destination" in self.flows[name][0][1]:
                            dest_table = SSIS_Fabric.parse_destination_component(dataflow, self.flows[name][0][0])
                            self.executables[dataflow_name] += [activity_name]
                        else:
                            self.component_map[name] = self.component_map[name] + [f"{t1}_{t2}"] # add output table name to component_map
                            dest_table = f"{t1}_{t2}"
                        query = query.replace("schema", "stage")
                        print(query)
                        
                        procedure_name = f"Lookup_{dest_table}"
                        procedure = SSIS_Fabric.design_procedure(procedure_name, dest_table, query)
                        # self.create_procedure_fabric(procedure)
                        activity1 = self.component_map[dependency][3]
                        activity2 = copy_name
                        # self.procedure_json(procedure_name, activity_name, [activity1, activity2])
                        self.component_map[name] += [dest_table]
                        self.component_map[name] += [activity_name]
                        self.component_map[name][1] = True
                        self.counts["procedure"] += 1
                
                else:
                    if self.component_map[self.dependency_map[name][0]][1] == True:
                        self.component_map[name][1] = True
                print(f"{name}: {self.component_map[name][1]}")

    #driving function 1
    def parse_ssis_pipeline(self, filepath):
        tree = etree.parse(filepath)
        namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts', 'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'}
        pipeline_executables = tree.xpath("//DTS:Executables/DTS:Executable", namespaces=namespaces)
        executables_names = tree.xpath("//DTS:Executables/DTS:Executable/@DTS:ObjectName", namespaces=namespaces)
        n = len(pipeline_executables)
        print("Executables\n")
        for i in range(n):
            exec_type = pipeline_executables[i].xpath("@DTS:ExecutableType", namespaces=namespaces)[0]
            name = pipeline_executables[i].xpath("@DTS:ObjectName", namespaces=namespaces)[0]
            print(exec_type, name)
            self.executables[name] = []
            if exec_type == "Microsoft.Pipeline":
                self.parse_dataflow(pipeline_executables[i], name)
                self.parse_components(pipeline_executables[i], name)
            elif exec_type == "Microsoft.ExecuteSQLTask":
                procedure_name = SSIS_Fabric.parse_execsql(pipeline_executables[i], name)
                self.counts["procedure"] += 1
                activity_name = f"StoredProcedure{self.counts["procedure"]}"
                self.executables[name] += [activity_name]
                self.procedure_json(procedure_name, activity_name, self.executables[executables_names[i-1]])
                print(procedure_name)
        
        with open("activity_templates/pipeline.json", "r") as file:
            pipeline = json.load(file)
        pipeline["name"] = self.pipeline_name
        with open("activity_templates/pipeline.json", "w") as json_file:
            json.dump(pipeline, json_file, indent=4)
        
        print(self.component_map)
        print(self.executables)