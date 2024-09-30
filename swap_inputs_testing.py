import json
import msal
import requests
from lxml import etree
import re
import base64
import pyodbc

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

def parse_merge(dataflow, name, table1, table2): # returns the query required to join the two tables and create new table
    merge = dataflow.xpath(f"//components/component[@name='{name}']")[0]
    print(merge.xpath("@name")[0])
    joins = {0:"OUTER JOIN", 1:"LEFT JOIN", 2:"INNER JOIN"}
    namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts'}
    joinid = int(merge.xpath("properties/property[@name='JoinType']/text()")[0])
    key_count = int(merge.xpath("properties/property[@name='NumKeyColumns']/text()")[0])
    join = joins[joinid]

    if joinid == 1:
        inputs = merge.xpath("inputs/input")
        print(len(inputs))
        print(inputs[0].xpath("@name"))
        print(inputs[1].xpath("@name"))
        left_inputs = inputs[0].xpath("inputColumns/inputColumn")
        right_inputs = inputs[1].xpath("inputColumns/inputColumn")
    else:
        right_inputs = merge.xpath("inputs/input[@name='Merge Join Right Input']/inputColumns/inputColumn")
        left_inputs = merge.xpath("inputs/input[@name='Merge Join Left Input']/inputColumns/inputColumn")

    left_cols, left_sort = get_input_columns_for_merge(left_inputs)
    right_cols, right_sort = get_input_columns_for_merge(right_inputs)

    output_cols = merge.xpath("outputs/output[@name='Merge Join Output']/outputColumns/outputColumn/@name")
    old_cols_text = merge.xpath("outputs/output[@name='Merge Join Output']/outputColumns/outputColumn/properties/property[@name='InputColumnID']/text()")
    old_col_names = []
    for old_col_text in old_cols_text:
        matches = re.findall(r'\[([^\]]+)\]', old_col_text)
        old_col_names.append(matches[1])
    print("Left:", left_cols, left_sort)
    print("Right:", right_cols, right_sort)
    print("Output:", output_cols)
    query = "SELECT "
    for i in range(len(old_col_names)):
        if old_col_names[i] in left_cols:
            query = query + f"t1.{old_col_names[i]} AS {output_cols[i]}, "
        elif old_col_names[i] in right_cols:
            query = query + f"t2.{old_col_names[i]} AS {output_cols[i]}, "
    if inputs[0].xpath("@name")[0] == "Merge Join Right Input":
        query = query[:-2] + f" FROM schema.{table2} AS t1 {join} schema.{table1} AS t2 ON t1.{right_sort[1]} = t2.{left_sort[1]}"
    else:
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
    print(query)

filepath = "C:/Users/VenkataVivekGoli/source/repos/SSIS_Sample_Pipelines/Merge-Lookup.dtsx"
tree = etree.parse(filepath)
namespaces = {'DTS': 'www.microsoft.com/SqlServer/Dts', 'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'}
pipeline_executables = tree.xpath("//DTS:Executables/DTS:Executable", namespaces=namespaces)[0]

parse_merge(pipeline_executables, "Merge Join", "APurchases", "AProducts")
