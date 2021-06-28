-- Drop tables


DROP TABLE IF EXISTS test.invoice;

DROP TABLE IF EXISTS test.customer;

SET GLOBAL local_infile=1;
-- test.customer definition

CREATE TABLE IF NOT EXISTS `test`.`customer` (
  `id` int NOT NULL,
  `firstname` varchar(100) DEFAULT NULL,
  `lastname` varchar(100) DEFAULT NULL,
  `address` varchar(100) DEFAULT NULL,
  `phone` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- test.invoice definition

CREATE TABLE IF NOT EXISTS `test`.`invoice` (
  `InvoiceID` varchar(100) NOT NULL,
  `CustomerID` int DEFAULT NULL,
  `orderdate` date DEFAULT NULL,
  `subtotal` float DEFAULT NULL,
  PRIMARY KEY (`InvoiceID`),
  KEY `invoice_FK` (`CustomerID`),
  CONSTRAINT `invoice_FK` FOREIGN KEY (`CustomerID`) REFERENCES `customer` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;