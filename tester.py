from project_modules import SSIS_Fabric

dtsx_file_path = "C:/Users/VenkataVivekGoli/source/repos/SSIS_Sample_Pipelines/Multi-Merge.dtsx"
workspace_name = "Kanerika Full Demo"
lakehouse_name = "Bronze_Lakehouse"
warehouse_name = "DataMart"
pipeline_name = "multiple-merges"
endpoint = "7xiyx2ruvtrevnbbnt5c7t7sim-exz2tbz7blrubfknxxc6ew6yxe.datawarehouse.fabric.microsoft.com"

obj = SSIS_Fabric(workspace_name, lakehouse_name, warehouse_name, pipeline_name, endpoint)
# obj.create_token()
# obj.get_workspace_id()
# obj.get_lakehouse_id()
# obj.get_warehouse_id()

obj.parse_ssis_pipeline(dtsx_file_path)