"""Interface for CRUD operations on the database"""
from __future__ import annotations
import redis
import sqlalchemy
from typing import Union, List


class SQLDatabase:


    def __init__(self, url: str) -> None:
        self.url = url


    def has_conn(self):
        return hasattr(self,"connection")


    def __enter__(self) -> SQLDatabase:
        self.establish_connection()
        return self


    def __exit__(self, exc_type, exc_val, tracback):
        self.close_connection()


    def establish_connection(self):
        self.connection = sqlalchemy.create_engine(self.url).connect()


    def close_connection(self):
        self.connection.close()
        del self.connection


    def create_table(self, table_name: str, columns: list):
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(columns)});"
        self.connection.execute(sql)


    def get_tables(self):

        sql = "SELECT table_name \
            FROM information_schema.tables \
            WHERE table_schema = 'public';"
        
        return self.connection.execute(sql).fetchall()


    def remove_table(self, table_name: str):
        sql = f"DROP TABLE IF EXISTS {table_name};"
        self.connection.execute(sql)


    def create_record(self, table: str, data: dict) -> None:
        """create a new record in an existing table"""
        fields = ','.join(list(data.keys()))
        values = tuple(data.values())
        sql = f"INSERT INTO {table} ({fields}) VALUES ({('%s,'*len(values))[:-1]});"
        self.connection.execute(sql,values)

    
    def update_record(self, table: str, id: int, set_: str, to: str) -> None:
        """update an existing record in a table using an id"""
        sql = f"UPDATE {table} SET {set_} = %s WHERE id = %s;"
        self.connection.execute(sql, (to, id))


    def delete_record(self, table: str, id: int) -> None:
        """delete an existing record in a table using an id"""
        sql = f"DELETE FROM {table} WHERE id = %s;"
        self.connection.execute(sql, id)


    def select_records(self, table: str, columns: list = ['*'], constraints: Union[dict,None] = None) -> List[tuple]:
        """select all or subset of records in a table"""

        sql = f"SELECT {','.join(columns)} FROM {table};"
        if constraints:
            sql_addition = " WHERE "
            for col in constraints:
                sql_addition += f"{col} = %s AND "
            sql_addition = sql_addition[:-5]+";"

            sql = sql.replace(";",sql_addition)
            return self.connection.execute(sql, tuple(constraints.values())).fetchall()
        else:
            return self.connection.execute(sql).fetchall()


