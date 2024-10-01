from project_modules import SSIS_Fabric

# dtsx_file_path = "C:/Users/VenkataVivekGoli/Downloads/onlyLookupRenameColumn.dtsx"
dtsx_file_path = "C:/Users/VenkataVivekGoli/Downloads/Multiple_LookupMergePipeline.dtsx"
workspace_name = "Kanerika Full Demo"
lakehouse_name = "Bronze_Lakehouse"
warehouse_name = "DataMart"
pipeline_name = "a-merge-lookup-test"
endpoint = "7xiyx2ruvtrevnbbnt5c7t7sim-exz2tbz7blrubfknxxc6ew6yxe.datawarehouse.fabric.microsoft.com"

obj = SSIS_Fabric(workspace_name, lakehouse_name, warehouse_name, pipeline_name, endpoint)
obj.create_token()
obj.get_workspace_id()
obj.get_lakehouse_id()
obj.get_warehouse_id()

obj.parse_ssis_pipeline(dtsx_file_path)

encoded = obj.encode_json_to_base64()
# obj.create_payload_json(pipeline_name, encoded)
# with open(f"activity_templates/payload.json", "r") as file:
#     pipeline_payload = file.read()
# obj.create_pipeline(pipeline_payload)