-- Drop tables

DROP TABLE IF EXISTS test.invoice;

DROP TABLE IF EXISTS test.customer;

-- test.customer definition

CREATE TABLE IF NOT EXISTS test.customer (
	id int4 NOT NULL,
	firstname varchar NULL,
	lastname varchar NULL,
	address varchar NULL,
	phone varchar NULL,
	CONSTRAINT customer_pk PRIMARY KEY (id)
);

-- test.invoice definition

CREATE TABLE IF NOT EXISTS test.invoice (
	InvoiceID int4 NOT NULL,
	CustomerID int4 NULL,
	orderdate date NULL,
	subtotal float4 NULL,
	CONSTRAINT invoice_pk PRIMARY KEY (InvoiceID),
	CONSTRAINT customer_fk FOREIGN KEY (CustomerID) REFERENCES test.customer(id)
);