
/*
   =====================================================================================
   Quality Checks
   =====================================================================================
   Purpose of sript: 
         This script performs quality checks to validate the integrity, consistency 
		 and accuracy od the Gold layer. These checks ensures:
		 - uniqueness of surrogate keys in the dimension tables
		 - Referential integrity between fact and dimension tables.
         - Validation of relationships in the data model for analytical purposes.

   Usage:
     - Investigate and resolve any discrepancies found during the checks.
*/



-- =====================================================================================
-- Checking 'gold.product_key'
-- =====================================================================================
-- Check for uniqueness of product key in gold.dim_products
-- Expectations: No results


SELECT 
product_key,
COUNT(*) AS Duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1


-- =====================================================================================
-- Checking 'gold.customer_key'
-- =====================================================================================
-- Check for uniqueness of customer key in gold.dim_customer
-- Expectations: No results


SELECT 
customer_key,
COUNT(*) AS Duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1



-- =====================================================================================
-- Checking 'fact.fact_table'
-- =====================================================================================
-- Checking the data model connectivity between fact and dimension views

SELECT 
	*
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
	ON s.product_key = p.product_key
LEFT JOIN gold.dim_customers c
	ON s.product_key = c.customer_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL;


