import MySQLdb
from typing import List


class MySQLClientConnection:

    def __init__(self, mysql_info_config):
        self.conn: MySQLdb.Connection = \
            MySQLdb.connect(host=mysql_info_config['db_host'],
                            user=mysql_info_config['db_user'],
                            port=mysql_info_config['db_port'],
                            password=mysql_info_config['db_password'],
                            db=mysql_info_config['db_name'],
                            charset='utf8mb4')

    def select_data_from_database(self, table0: str, **kwargs):
        """ Select from data table.

        Args:+
            table0 (str): Name of data table

        Returns:
            Any: List of data objects
        """
        query = "SELECT * FROM " + table0
        conditions = []
        values = []
        for field, value in kwargs.items():
            conditions.append(f"`{field}` = %s")
            values.append(value)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        with self.conn.cursor(MySQLdb.cursors.DictCursor) as cursor:
            cursor.execute(query, values)
            result = cursor.fetchall()
            self.conn.commit()
        return result

    def delete_data_from_database(self, table0: str, **kwargs):
        """ Delete items from data table.

        Args:
            table0 (str): Name of data table
        """
        query = "DELETE FROM " + table0
        conditions = []
        values = []

        for field, value in kwargs.items():
            conditions.append(f"`{field}` = %s")
            values.append(value)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        with self.conn.cursor() as cursor:
            cursor.execute(query, values)
            self.conn.commit()

    def insert_data_to_database(self, table0: str, **kwargs):
        """ Insert data items to data table.

        Args:
            table0 (str): Name of data table

        Returns:
            Any: _description_
        """
        fields = ', '.join([f"`{field}`" for field in kwargs.keys()])
        placeholders = ', '.join(['%s'] * len(kwargs))
        query = f"INSERT INTO {table0} ({fields}) VALUES ({placeholders})"
        values = list(kwargs.values())

        with self.conn.cursor() as cursor:
            cursor.execute(query, values)
            self.conn.commit()
            return cursor.lastrowid

    def update_data_to_database(self, table0: str, columns: List[str], conditions: List[str]):
        """ Update data table

        Args:
            table0 (str): Name of data table
            columns (List[str]): Columns of data table
            conditions (List[str]): Update conditions
        """
        update_query = f"UPDATE {table0} SET "
        update_query += ", ".join([f"`{column}` = %s" for column in columns.keys()])
        condition_str = ' AND '.join([f"`{key}` = %s" for key in conditions.keys()])
        update_query += " WHERE " + condition_str
        values = list(columns.values()) + list(conditions.values())

        with self.conn.cursor() as cursor:
            cursor.execute(update_query, values)
            self.conn.commit()

