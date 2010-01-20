DROP DATABASE IF EXISTS Import_Logger;
CREATE DATABASE Import_Logger;
USE Import_Logger;
CREATE TABLE  `Import_Logger`.`big_blue_import_errors_log` (
  `master_number_id` int(11) NOT NULL,
  `voxcontract_id` varchar(40) DEFAULT NULL,
  `source_id` int(11) DEFAULT NULL,
  `source_id_strace` varchar(255) DEFAULT NULL,
  `source_database_id` int(11) DEFAULT NULL,
  `source_database_id_strace` varchar(255) DEFAULT NULL,
  `region_id` int(11) DEFAULT NULL,
  `region_id_strace` varchar(255) DEFAULT NULL,
  `payment_option_id` int(11) DEFAULT NULL,
  `payment_option_id_strace` varchar(255) DEFAULT NULL,
  `stack_trace` varchar(255) DEFAULT NULL,
  `profit_center_id` int(11) DEFAULT NULL,
  `profit_center_id_strace` varchar(255) DEFAULT NULL,
  `product_id` int(11) DEFAULT NULL,
  `product_id_strace` varchar(255) DEFAULT NULL,
  `imported` bit(1) DEFAULT NULL,
  `sales_man_id` int(11) DEFAULT NULL,
  `sales_man_id_strace` varchar(255) DEFAULT NULL,
  `account_id` int(11) DEFAULT NULL,
  `account_id_strace` varchar(255) DEFAULT NULL,
  `cancellation_reason_id` int(11) DEFAULT NULL,
  `cancellation_reason_id_strace` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`master_number_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1

CREATE TABLE  `Import_Logger`.`global_customer_import_log` (
  `master_number_id` int(11) NOT NULL,
  `customer_id` varchar(40) DEFAULT NULL,
  `account_code` varchar(40) DEFAULT NULL,
  `customer_name` varchar(255) DEFAULT NULL,
  `global_customer_guid` varchar(40) DEFAULT NULL,
  `account_id` int(11) DEFAULT NULL,
  `action` varchar(40) DEFAULT NULL,
  `system_id` varchar(40) DEFAULT NULL,
  PRIMARY KEY (`master_number_id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1