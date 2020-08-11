from airflow.hooks.base_hook import BaseHook

conn = BaseHook.get_connection('mariadb')
print(f"AIRFLOW_CONN_{conn.conn_id.upper()}='{conn.get_uri()}'")