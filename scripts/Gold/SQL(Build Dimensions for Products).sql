
-- Data Standerdization & Consistency
SELECT DISTINCT
		CASE WHEN ep.category IS NULL THEN 'N/A'
			 ELSE ep.category
			 END
FROM Silver.crm_product_info as pi
LEFT JOIN Silver.erp_px_cat_g1v2 as ep
ON		  pi.cat_id = ep.cst_id


SELECT DISTINCT 
				CASE WHEN ep.sub_category IS NULL THEN 'N/A'
			 ELSE ep.sub_category
			 END
FROM Silver.crm_product_info as pi
LEFT JOIN Silver.erp_px_cat_g1v2 as ep
ON		  pi.cat_id = ep.cst_id


-- Check for any duplicates in the Table.

SELECT prd_id, COUNT(*) FROM 
(
	SELECT
			pi.prd_id,
			pi.prd_key,
			pi.prd_name,
			pi.cat_id,
			ep.category,
			ep.sub_category,			
			pi.prd_cost,
			pi.prd_line,
			pi.prd_start_date,
			ep.maintenance
	FROM Silver.crm_product_info as pi
	LEFT JOIN Silver.erp_px_cat_g1v2 as ep
	ON		  pi.cat_id = ep.cst_id
	WHERE pi.prd_end_date IS NULL
)t
GROUP BY prd_id
HAVING COUNT(*) > 1


-- Transformation Table:-
CREATE VIEW Gold.dim_product AS 
SELECT
		ROW_NUMBER() OVER(ORDER BY pi.prd_start_date) AS product_key, -- SURROGATE KEY
		pi.prd_id AS Product_ID,
		pi.prd_key AS Product_Number,
		pi.prd_name AS Product_Name,
		pi.cat_id AS Category_ID,
		ep.category AS Category,
		ep.sub_category AS SubCategory,	
		ep.maintenance AS Maintenance,
		pi.prd_cost AS Product_Cost, 
		pi.prd_line AS Product_Line,
		pi.prd_start_date AS Start_Date
FROM Silver.crm_product_info as pi
LEFT JOIN Silver.erp_px_cat_g1v2 as ep
ON		  pi.cat_id = ep.cst_id
WHERE prd_end_date IS NULL -- Filter out all historical data


SELECT Product_Number, COUNT(*) FROM 
(
SELECT
		ROW_NUMBER() OVER(ORDER BY pi.prd_start_date) AS product_key, -- SURROGATE KEY
		pi.prd_id AS Product_ID,
		pi.prd_key AS Product_Number,
		pi.prd_name AS Product_Name,
		pi.cat_id AS Category_ID,
		ep.category AS Category,
		ep.sub_category AS SubCategory,	
		ep.maintenance AS Maintenance,
		pi.prd_cost AS Product_Cost, 
		pi.prd_line AS Product_Line,
		pi.prd_start_date AS Start_Date
FROM Silver.crm_product_info as pi
LEFT JOIN Silver.erp_px_cat_g1v2 as ep
ON		  pi.cat_id = ep.cst_id
WHERE prd_end_date IS NULL
)t
GROUP BY Product_Number
HAVING COUNT(*) > 1