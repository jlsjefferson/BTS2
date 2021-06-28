-- Export tables

\copy test.invoice TO '/opt/airflow/dags/data_exp/invoice.tsv' WITH DELIMITER E'\t' CSV HEADER null as ';';

\copy test.customer TO '/opt/airflow/dags/data_exp/customer.tsv' WITH DELIMITER E'\t' CSV HEADER null as ';';