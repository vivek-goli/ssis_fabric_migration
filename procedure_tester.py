import json
import msal
import requests
from lxml import etree
import re
import base64
import pyodbc

def design_procedure(procedure_name, destination_table, query):
        return f"""
                    CREATE PROCEDURE main.{procedure_name} AS
                    BEGIN
                        IF OBJECT_ID('main.{destination_table}', 'U') IS NOT NULL
                        BEGIN
                            DROP TABLE main.{destination_table};
                        END
                        CREATE TABLE main.{destination_table} AS
                        {query}
                    END;
                """

def create_procedure_fabric(sql_query):
        user = "vivek.goli@kanerika.com"
        password = "Vivek@16"
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER=7xiyx2ruvtrevnbbnt5c7t7sim-exz2tbz7blrubfknxxc6ew6yxe.datawarehouse.fabric.microsoft.com"
            f"DATABASE=DataMart;"
            "Authentication=ActiveDirectoryPassword;"
            f"UID={user};"
            f"PWD={password}"
        )

        with pyodbc.connect(conn_str) as conn:
            print(conn)
            with conn.cursor() as cursor:
                print(cursor)
                cursor.execute(sql_query)
                conn.commit()
                print("Stored procedure created successfully.")

pname = "test_procedure"
table_name = "tester"
query = "SELECT * FROM stage.Country"
procedure = design_procedure(pname, table_name, query)
create_procedure_fabric(procedure)