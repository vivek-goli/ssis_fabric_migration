from flask import Flask, render_template, request, jsonify
import os
from project_modules import SSIS_Fabric
import traceback

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
        obj = SSIS_Fabric(workspace_name, lakehouse_name, warehouse_name, pipeline_name, endpoint)
        obj.create_token()
        obj.get_workspace_id()
        obj.get_lakehouse_id()
        obj.get_warehouse_id()

        obj.parse_ssis_pipeline(dtsx_file_path)

        encoded = obj.encode_json_to_base64()
        obj.create_payload_json(pipeline_name, encoded)
        with open(f"activity_templates/payload.json", "r") as file:
            pipeline_payload = file.read()
        obj.create_pipeline(pipeline_payload)
        
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