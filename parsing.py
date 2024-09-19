from lxml import etree
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
    joinid = merge.xpath("//properties/property[@name='JoinType']/text()")

    key_count = int(merge.xpath("//properties/property[@name='NumKeyColumns']/text()")[0])

    join = joins[joinid]

    left_inputs = merge.xpath("//inputs/input[contains(@name,'Right Input')]/inputColumns/inputColumn")
    right_inputs = merge.xpath("//inputs/input[contains(@name,'Left Input')]/inputColumns/inputColumn")

    left_cols, left_sort = get_input_columns_for_merge(left_inputs)
    right_cols, right_sort = get_input_columns_for_merge(right_inputs)

    output_cols = merge.xpath("//outputs/output/outputColumns/outputColumn/@name")
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

def parse_lookup(lookup):
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