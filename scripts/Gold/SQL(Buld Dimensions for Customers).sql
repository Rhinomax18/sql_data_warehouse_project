-- Now check for any Duplicates or Nulls in the table.

SELECT Cst_id, COUNT(*) [No. of Duplicates] FROM 
(
	SELECT
			ci.cst_id,
			ci.cst_key,
			ci.cst_fn,
			ci.cst_ln,
			ci.cst_gender,
			ci.cst_marital_status,
			ci.cst_create_date,
			ec.gender,
			ec.birth_Date,
			el.country
	FROM Silver.crm_customer_info as ci
	LEFT JOIN Silver.erp_cust_az12 as ec
	ON		  ci.cst_key = ec.cst_id
	LEFT JOIN silver.erp_loc_a101 as el
	ON		  ci.cst_key = el.cst_id
)t 
GROUP BY cst_id 
HAVING COUNT(*) > 1

-- AS you can notice that there are two genders tables, we have to discuss it with the expert that which is the master tables.
-- We can assume that the master table is the crm table.

	SELECT DISTINCT
	ci.cst_gender,
	ec.gender,
	CASE WHEN ci.cst_gender != 'N/A' THEN ci.cst_gender 
		 ELSE COALESCE(ec.gender,'N/A')
		 END new_gender
	FROM Silver.crm_customer_info as ci
	LEFT JOIN Silver.erp_cust_az12 as ec
	ON		  ci.cst_key = ec.cst_id
	LEFT JOIN silver.erp_loc_a101 as el
	ON		  ci.cst_key = el.cst_id
	ORDER BY 1,2



-- Table Transformation:- (It is a Dimension Table)
SELECT
		ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS Customer_key,
		ci.cst_id as Customer_id,
		ci.cst_key as Customer_number,
		ci.cst_fn as First_Name,
		ci.cst_ln as Last_Name,
		el.country as Country,
	CASE WHEN ci.cst_gender != 'N/A' THEN ci.cst_gender 
		 ELSE COALESCE(ec.gender,'N/A')
		 END AS Gender,
		ci.cst_marital_status AS Marital_Status,
		ci.cst_create_date as Create_date,
		ec.birth_Date AS BirthDate
FROM Silver.crm_customer_info as ci
LEFT JOIN Silver.erp_cust_az12 as ec
ON		  ci.cst_key = ec.cst_id
LEFT JOIN silver.erp_loc_a101 as el
ON		  ci.cst_key = el.cst_id


-- Now create A VIEW for the above table.

CREATE VIEW gold.dim_customers AS
SELECT
		ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS Customer_key, -- Surrogate Key
		ci.cst_id as Customer_id,
		ci.cst_key as Customer_number,
		ci.cst_fn as First_Name,
		ci.cst_ln as Last_Name,
		el.country as Country,
	CASE WHEN ci.cst_gender != 'N/A' THEN ci.cst_gender 
		 ELSE COALESCE(ec.gender,'N/A')
		 END AS Gender,
		ci.cst_marital_status AS Marital_Status,
		ci.cst_create_date as Create_date,
		ec.birth_Date AS BirthDate
FROM Silver.crm_customer_info as ci
LEFT JOIN Silver.erp_cust_az12 as ec
ON		  ci.cst_key = ec.cst_id
LEFT JOIN silver.erp_loc_a101 as el
ON		  ci.cst_key = el.cst_id
