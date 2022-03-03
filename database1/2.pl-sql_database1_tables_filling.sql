-- realization in Oracle / реализация в Оракл
CREATE OR REPLACE PROCEDURE filling_D_product( -- проверить название процедуры в соответствии с внутренним стандартом организации
    pr_id IN D_product.product_id%TYPE -- можно ли называть одинаковым именем, как и в столбце таблицы?
    pr_name IN D_product.p_name%TYPE
    pr IN D_product.price%TYPE
    prlog_id IN D_product.log_id%TYPE
)
IS 
BEGIN
    INSERT INTO D_product VALUES (pr_id, pr_name, pr, prlog_id);
    dbms_output.put_line('Product added.')
END;

EXECUTE filling_D_product(1, 'Apple', 20, 00001); --cmd command

SELECT * FROM D_product WHERE product_id = 1; --cmd command


-- не работает переименовать
-- CREATE OR REPLACE PROCEDURE filling_D_product(
--     product_id IN D_product.product_id%TYPE 
--     p_name IN D_product.p_name%TYPE
--     price IN D_product.price%TYPE
--     log_id IN D_product.log_id%TYPE
-- )
-- IS 
-- BEGIN
--     INSER INTO D_product VALUES (product_id, p_name, price, log_id);
--     dbms_output.put_line('Product added.')
-- END;

-- realization in Microsoft / реализация в Майкрософт
CREATE PROCEDURE filling_D_product 
    @product_id BIGSERIAL, 
    @p_name VARCHAR(50), 
    @price MONEY, 
    @log_id INTEGER
   AS  
   BEGIN  
      -- The print statement returns text to the user  
      --PRINT 'Enter the data ' + CAST(@price AS varchar(50));  
      -- A second statement starts here  
      --SELECT ProductName, Price FROM vw_Names    
      SET CAST(@price AS varchar(50));
      SET @p_name = LTRIM(RTRIM(@p_name));
    
      INSERT INTO D_product (product_id, p_name, price, log_id)
        VALUES (@product_id, @p_name, @price, @log_id)

      SELECT * FROM D_product
      WHERE product_id = @product_id

   END  
GO   
-- не понятно, сохранилась ли процедура.


-----------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE filling_D_product( -- проверить название процедуры в соответствии с внутренним стандартом организации
    pr_id IN D_product.product_id%TYPE 
    pr_name IN D_product.p_name%TYPE
    pr IN D_product.price%TYPE
    prlog_id IN D_product.log_id%TYPE
)
IS 
BEGIN
    INSERT INTO D_product VALUES (pr_id, pr_name, pr, prlog_id);
    dbms_output.put_line('Product added.')
END;

EXECUTE filling_D_product(1, 'Apple', 20, 00001); --cmd command

SELECT * FROM D_product WHERE product_id = 1; --cmd command








