from flask import Flask, render_template, request, jsonify
import os
import json
import traceback
from module import parse_ssis, parse_plain_dataflow, parse_mergejoin_dataflow, parse_procedure_dataflow, parse_lookup_dataflow
from module import create_token, get_workspaceid, get_lakehouseid, get_warehouseid
from module import create_json_copydata, create_json_copydata_procedure, create_json_mergejoin, create_json_lookup, create_pipeline, encode_json_to_base64, create_procedure_fabric, create_join_procedure

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/migrate', methods=['POST'])
def migrate():
    try:
        dtsx_file = request.files['dtsxFile']
        workspace_name = request.form['workspaceName']
        lakehouse_name = request.form['LakehouseName']
        warehouse_name = request.form['WarehouseName']
        pipeline_name = request.form['pipelineName']
        endpoint = request.form['endpoint']

        print(dtsx_file.filename)
        # Save uploaded file
        dtsx_file_path = os.path.join('uploads', dtsx_file.filename)
        dtsx_file.save(dtsx_file_path)
        
        # Get access token
        client_id = "c657b643-3d0b-42a2-8680-2d05f60ffef0"
        tenant_id = "ea8bd1fd-ac34-4ae2-b421-6cfa2fcff243"
        client_secret = "rdp8Q~jD1IjhvSolje-wW9~dk87m.J2yeGV8xcel"
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        scope = ['https://api.fabric.microsoft.com/.default']
        scopes = ["https://api.fabric.microsoft.com/Workspace.ReadWrite.All", "https://api.fabric.microsoft.com/Item.ReadWrite.All"]
        
        access_token = create_token(client_id, authority, scope)
        
        workspace_id = get_workspaceid(access_token, workspace_name)
        pipeline_type = parse_ssis(dtsx_file_path)
        print(pipeline_type)
        pipeline_base64 = ""
        if pipeline_type == "copy":
            print("Creating a copy pipeline")
            details = parse_plain_dataflow(dtsx_file_path)
            lakehouse_id = get_lakehouseid(access_token, workspace_id, lakehouse_name)
            
            create_json_copydata(
                pipeline_name=pipeline_name,
                workspace_id=workspace_id,
                artifact_id=lakehouse_id,
                lakehouse_folder="Tables",
                destination_table=details["destination_table"]
            )
            pipeline_base64 = encode_json_to_base64("json_templates/copydata_template.json")
        
        elif pipeline_type == "copy-procedure":
            print("Creating a pipeline with copy and procedure")
            details = parse_procedure_dataflow(dtsx_file_path)
            warehouse_id = get_warehouseid(access_token, workspace_id, warehouse_name)

            create_json_copydata_procedure(
                pipeline_name=pipeline_name,
                workspace_id=workspace_id,
                artifact_id=warehouse_id,
                warehouse=warehouse_name,
                destination_table=details["destination_table"],
                procedure=details["procedure_name"],
                endpoint=endpoint
            )
            pipeline_base64 = encode_json_to_base64("json_templates/copydata_procedure_template.json")
        
        elif pipeline_type == "merge-join":
            print("Creating a Merge Join pipeline")
            query, details = parse_mergejoin_dataflow(dtsx_file_path)
            warehouse_id = get_warehouseid(access_token, workspace_id, warehouse_name)
            lakehouse_id = get_lakehouseid(access_token, workspace_id, lakehouse_name)

            query = query.replace("Lakehouse", lakehouse_name)

            dest_table = f"{details["destination"]["schema"]}.{details["destination"]["table"]}"
            procedure_name = f"Merge_{details["sources"][0]["table"]}_{details["sources"][1]["table"]}"
            procedure = create_join_procedure(procedure_name, dest_table, query)
            print("Procedure:\n", procedure)

            create_procedure_fabric(endpoint, warehouse_name, procedure)
            create_json_mergejoin(
                pipeline_name=pipeline_name,
                source=[details["sources"][0]["table"], details["sources"][1]["table"]],
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id, 
                warehouse=warehouse_name, 
                warehouse_id=warehouse_id, 
                endpoint=endpoint,
                procedure=procedure_name
            )
            pipeline_base64 = encode_json_to_base64("json_templates/merge_template.json")
        
        elif pipeline_type == "lookup":
            print("Creating a Lookup pipeline")
            query, details = parse_lookup_dataflow(dtsx_file_path)
            warehouse_id = get_warehouseid(access_token, workspace_id, warehouse_name)
            lakehouse_id = get_lakehouseid(access_token, workspace_id, lakehouse_name)
            query = query.replace("Lakehouse", lakehouse_name)

            dest_table = f"{details["destination"]["schema"]}.{details["destination"]["table"]}"
            procedure_name = f"Lookup_{details["source"]["table"]}_{details["lookup"]["table"]}"
            procedure = create_join_procedure(procedure_name, dest_table, query)

            create_procedure_fabric(endpoint, warehouse_name, procedure)
            create_json_lookup(
                pipeline_name=pipeline_name,
                source=[details["source"]["table"], details["lookup"]["table"]],
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id, 
                warehouse=warehouse_name, 
                warehouse_id=warehouse_id, 
                endpoint=endpoint, 
                procedure=procedure_name
            )
            pipeline_base64 = encode_json_to_base64("json_templates/lookup_template.json")

        with open(f"json_templates/pipeline.json", "r") as file:
            pipeline = json.load(file)
        pipeline["displayName"] = pipeline_name
        pipeline["definition"]["parts"][0]["payload"] = pipeline_base64

        with open(f"json_templates/pipeline.json", "w") as json_file:
            json.dump(pipeline, json_file, indent=4)

        with open(f"json_templates/pipeline.json", "r") as file:
            pipeline = file.read()
        create_pipeline(access_token, workspace_id, pipeline)
        
        return jsonify({"message": "Pipeline created successfully!"})
    except Exception as e:
        tb_str = traceback.format_exc()
        print("Traceback details:")
        print(tb_str)
        return jsonify({"message": f"Failed to migrate: {str(e)}"}), 500


if __name__ == '__main__':
    if not os.path.exists('uploads'):
        os.makedirs('uploads')
    app.run(debug=True)
