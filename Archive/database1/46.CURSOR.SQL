SELECT  saproduct.product_name, dproduct.product_name,
           saproduct.product_code, dproduct.product_ID, saproduct.is_last, saproduct.price 
	FROM SA_product_A AS saproduct
    LEFT JOIN D_product_A dproduct 
        ON saproduct.product_code = dproduct.product_code AND
        saproduct.product_ID = dproduct.product_ID:: varchar --?
    WHERE saproduct.product_name != COALESCE (dproduct.product_name, '000') AND 
        saproduct.is_last = 'true'; 
		
SELECT * FROM SA_product_A;

