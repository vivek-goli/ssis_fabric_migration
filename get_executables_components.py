from lxml import etree
import xml.etree.ElementTree as ET
import re

def get_input_columns_for_merge(inputs):
    cols = []
    sort_map = {}
    for i in range(len(inputs)):
        col_name = inputs[i].xpath("@cachedName")[0]
        cols.append(col_name)
        sortid = inputs[i].xpath("@cachedSortKeyPosition")
        if len(sortid) > 0:
            sort_map[int(sortid[0])] = col_name
    return cols, sort_map

def parse_merge(merge):
    joins = {0:"OUTER JOIN", 1:"LEFT JOIN", 2:"INNER JOIN"}
    namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts'}
    joinid = int(merge.xpath("//properties/property[@name='JoinType']/text()")[0])
    key_count = int(merge.xpath("//properties/property[@name='NumKeyColumns']/text()")[0])
    join = joins[joinid]

    right_inputs = merge.xpath("//inputs/input[contains(@name,'Right Input')]/inputColumns/inputColumn")
    left_inputs = merge.xpath("//inputs/input[contains(@name,'Left Input')]/inputColumns/inputColumn")

    left_cols, left_sort = get_input_columns_for_merge(left_inputs)
    right_cols, right_sort = get_input_columns_for_merge(right_inputs)

    output_cols = merge.xpath("//outputs/output[@name='Merge Join Output']/outputColumns/outputColumn/@name")
    query = "SELECT "
    for col in output_cols:
        if col in left_cols:
            query = query + f"t1.{col}, "
        else:
            query = query + f"t2.{col}, "
    query = query[:-2] + f" FROM Lakehouse.schema1.table1 AS t1 {join} Lakehouse.schema2.table2 AS t2 ON t1.{left_sort[1]} = t2.{right_sort[1]}"

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

def parse_lookup(lookup, columns):
    lookups = lookup.xpath("//properties/property[@name='SqlCommand']/text()")
    pattern = r"FROM\s+\[([^\]]+)\]\.\[([^\]]+)\]"
    match = re.search(pattern, lookups[0], re.IGNORECASE)
    lookup_schema = match.group(1)
    lookup_table = match.group(2)

    join_col1 = lookup.xpath("//inputs/input/inputColumns/inputColumn/@cachedName")
    join_col2 = lookup.xpath("//inputs/input/inputColumns/inputColumn/properties/property[@name='JoinToReferenceColumn']/text()")
    ref_cols = lookup.xpath("//outputs/output/outputColumns/outputColumn/properties/property[@name='CopyFromReferenceColumn']/text()")

    query = "SELECT "
    for col in columns:
        query = query + f"t1.{col}, "
    for col in ref_cols:
        query = query + f"t2.{col}, "
    query = query[:-2] + f" FROM Lakehouse.source_schema.source_table AS t1 JOIN Lakehouse.{lookup_schema}.{lookup_table} AS t2 ON t1.{join_col1[0]} = t2.{join_col2[0]}"
    return query

def parse_dataflow(dataflow, name):    
    namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts', 'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'}
    task_name = dataflow.xpath("@DTS:ObjectName", namespaces=namespaces)[0]
    print(f"Data Flow Task: {task_name}")
    components = dataflow.xpath(f"//components/component")
    component_map = {}
    flows = {}
    dependency_map = {}
    for component in components:
        component_class = component.xpath("@componentClassID")[0]
        component_name = component.xpath("@name")[0]
        component_map[component_name] = [component_class, False]
        columns = component.xpath("//outputs/output[contains(@name,'Source Output')]/outputColumns/outputColumn/@name")
        flows[component_name] = []
        dependency_map[component_name] = []
        print(f"  Component: {component_name}")

    data_paths = dataflow.xpath("//paths/path")
    for path in data_paths:
        source_id = path.xpath("@startId")[0]
        destination_id = path.xpath("@endId")[0]
        start_comp = source_id[source_id.find("\\")+1: source_id.find(".")].split("\\")[1]
        end_comp = destination_id[destination_id.find("\\")+1: destination_id.find(".")].split("\\")[1]

        flows[start_comp] = flows[start_comp] + [[end_comp, component_map[end_comp][0]]]
        dependency_map[end_comp] = dependency_map[end_comp] + [start_comp]
        print(start_comp, flows[start_comp])
    print(dependency_map)
    return component_map, dependency_map, flows

def parse_execsql(execsql, name):
    namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts', 'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'}
    statement = execsql.xpath("//DTS:ObjectData/SQLTask:SqlTaskData/@SQLTask:SqlStatementSource", namespaces=namespaces)[0]
    procedure_name = statement[5:]
    return procedure_name

def get_executables(filepath):
    tree = etree.parse(filepath)
    namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts', 'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'}
    executables = tree.xpath("//DTS:Executables/DTS:Executable", namespaces=namespaces)
    print("Executables\n")
    for exec in executables:
        exec_type = exec.xpath("@DTS:ExecutableType", namespaces=namespaces)
        name = exec.xpath("@DTS:ObjectName", namespaces=namespaces)
        print(exec_type, name)
        if exec_type[0] == "Microsoft.Pipeline":
            parse_dataflow(exec, name)
        elif exec_type[0] == "Microsoft.ExecuteSQLTask":
            procedure_name = parse_execsql(exec, name)
            print(procedure_name)
    print()


path = "C:/Users/VenkataVivekGoli/source/repos/AzureIntegrationProject/AzureIntegrationProject/CopyProcedure.dtsx"
get_executables(path)

path = "C:/Users/VenkataVivekGoli/source/repos/AzureIntegrationProject/AzureIntegrationProject/MergeJoin.dtsx"
get_executables(path)