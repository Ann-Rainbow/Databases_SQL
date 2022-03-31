--------------FIRST TUTORIAL
--Pl/SQL :-

--1. Basic Anonymous Block
--2. Procedure
--3. Function

DECLARE
    a NUMBER := 10;
    b NUMBER := 20;
    c NUMBER(1);

BEGIN
    c := a+b;
    dbms_output.put_line('The sum of two numbers is:' || c);

EXCEPTION -- optional block
    WHEN OTHERS THEN
        dbms_output.put_line(SQLERRM);
        dbms_output.put_line(SQLCODE);
END;

SQLERRM---<error message
SQLCODE---< Oracle error code

-- Pre-defined exception: -
NO_DATA_FOUND
TOO_MANY_ROWS
DUP_VAL_ON_INDEX --trying to insert the duplicate value
ZERO_DIVIDE

----------------------------------------------------------------------------------------------------------------------------------------------
DECLARE
    V_LAST_NAME employees.last_name%TYPE; --the same type of variable as in the column
    V_SALARY employees.salary%TYPE;
    V_EMPLOYEE_ID NUMBER(10) := &EMP_ID; --& take the value from &EMP_ID
    V_EMP_REC employees%rowtype; --V_EMP_REC capture all the columns inside of the table employees

BEGIN
    SELECT last_name, salary --implicit cursors - a memory allocated for that query to be executed
    INTO V_LAST_NAME, V_SALARY
    FROM employees
    WHERE emloyee_id = v_employee_id; -- implicit cursors, cursor is nothing, but the memory area, which is allocated by the program.

    dbms_output.put_line('Last name is '||V_LAST_NAME||' and salary is'||V_SALARY);

    SELECT * --capture all the columns into V_EMP_REC
    INTO V_EMP_REC --object in pl/sql
    FROM employees
    WHERE employee_id = v_employee_id;
    dbms_output.put_line('Last name is '||V_EMP_REC.LAST_NAME||
                         'and salary is: '||V_EMP_REC.SALARY);


EXCEPTION --firstly predefined exceptions, then defined exceptions, keep when others block the last.
    WHEN NO_DATA_FOUND THEN
        raise_application_error(-2000, 'Employee id does not exist');
    WHEN TOO_MANY_ROWS THEN
        raise_application_error(-2001, 'There is more than one employee with the same emp no';
    WHEN OTHERS THEN
        raise_application_error(-2002, SQLERRM);


END;
----------------------------------------------------------------------------------------------------------------------------------------------

--Cursors have two types
--Implicit cursor :- all the DML statements(INSERT, UPDATE, DELETE), SELECT INTO.
--Explicit cursor :- must be defined by developer explicitly

--how to define the explicit cursor
--cursor's lifecycle
DECLARE
OPEN 
FETCH
CLOSE

DECLARE
    V_LAST_NAME employees.last_name%TYPE;
    V_SALARY employees.salary%TYPE;
    V_DEPT_ID NUMBER(10) := &dept_id;

    CURSOR GET_EMP IS -- each will be associated with one select statement
    SELECT LAST_NAME, SALARY
    FROM EMPLOYEES
    WHERE DEPARTMENT_ID = V_DEPT_ID;

BEGIN
--in pl/sql 3 types of loops: basic loop, do-while loop, funnel

    OPEN GET_EMP;

    LOOP 
        
        FETCH GET_EMP INTO V_LAST_NAME, V_SALARY;
        EXIT WHEN GET_EMP%NOTFOUND--the statement to write whenever using a basic loops, 
                                  --the coursor should come out of the loop when there is no data
        dbms_output.put_line('Last name is '||V_LAST_NAME||' and salary is: '||V_SALARY);


    END LOOP;

    CLOSE GET_EMP;

EXCEPTION --firstly predefined exceptions, then defined exceptions, keep when others block the last.
    WHEN NO_DATA_FOUND THEN
        raise_application_error(-2000, 'Employee id does not exist');
    WHEN OTHERS THEN
        raise_application_error(-2002, SQLERRM);
END;

------------------------------------------------------------------------------------------------------------------------------------------------
-- procedures and functions

-- PROCEDURE:- it may return the data OR the output value OR it may not.
             --may or may not return the data.
-- FUNCTION:- must return the data.

--In data warehouses procedures are used to drop the indexes to create the indexes or analyze the tables and so on.

--->drop indexes
--->create indexes
--->analyze tables
--->truncate partition
--->exchange the data between partition


--the procedure which read all the indexes defined in the table and drop all the indexes.

EXECUTE IMMEDIATE 'DROP TABLE A'
DROP TABLE A;

CREATE OR REPLACE PROCEDURE SP_DROP_INDEXES(P_TAB_NAME IN VARCHAR2, P_ERRORMSG OUT VARCHAR2)
IS 

--dynamic sql - input for the statement is determined based on the result or based on the input you want to pass.
--you can't use the DDL statements inside a pl/sql. DDL statements should be used only with a dynamic sql.

--DECLARE is optional here

    V_SQL VARCHAR2(1000);

    CURSOR GET_INDEXES
    IS 
    SELECT INDEX_NAME
    FROM USER_INDEXES
    WHERE TABLE_NAME = P_TAB_NAME;


BEGIN
    FOR REC IN GET_INDEXES
    LOOP
        V_SQL := 'DROP INDEX '||REC.INDEX_NAME;
        EXECUTE IMMEDIATE V_SQL;

    END LOOP;


EXCEPTION
    WHEN OTHERS THEN
        P_ERRORMSG := SQLERRM;

END SP_DROP_INDEXES;

-----------------------------------------------------------------------------------------------------------------------------------------------

-- create the index
CREATE INDEX IDX_REPID ON FACT_SALES(REP_ID);

-----------------------------------------------------------------------------------------------------------------------------------------------

DROP TABLE DBINDEXES;
    CREATE TABLE "DBINDEXES"
    ( "INDEX_NAME" VARCHAR2 (100 BYTE),
    "TABLE_NAME" VARCHAR2 (100 BYTE),
    "COLUMN_NAME" VARCHAR2 (100 BYTE)
    );

INSERT INTO DBINDEXES (INDEX_NAME, TABLE_NAME, COLUMN_NAME)
VALUES ('REP_IDX', 'FACT_SALES', 'REP_ID');
INSERT INTO DBINDEXES (INDEX_NAME, TABLE_NAME, COLUMN_NAME)
VALUES ('VEN_IDX', 'FACT_SALES', 'VENDOR_ID');

COMMIT;
-----------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE 
PROCEDURE ANALYZE_TABLE (P_TAB_NAME IN VARCHAR2, P_ERRORMSG OUT VARCHAR2)
IS

    V_SQL VARCHAR2(1000);
BEGIN

    V_SQL := 'ANALYZE TABLE '||P_TAB_NAME||' COMPUTE STATISTICS';
    EXECUTE IMMEDIATE V_SQL;

EXCEPTION
    WHEN OTHERS THEN 
        P_ERRORMSG := SQLERRM;
END ANALYZE_TABLE;




-----------------------------------------------------------------------------------------------------------------------------------------------
--------------SECOND TUTORIAL
-- PROCEDURE 
---> A type of subprogram
---> Is a database object used to perform repeated execution
---> Value can be supplied through parameters
---> Formal parameter is passed when we define procedure while Actual parameter is passed when we invoke

-- Modes of PARAMETERS
---> IN Parameter  -- default parameter, the actual parameter passes the value to the formal parameter
                   -- from actual to formal parameter
---> OUT Parameter -- from formal parameter passes the value to the actual parameter
                   -- from formal to actual parameter
---> IN OUT Parameter


CREATE OR REPLACE PROCEDURE test_procedure IS
BEGIN
    dbms_output.put_line('Test procedure executed successfully')
END;

EXECUTE test_procedure; --cmd command


-----------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE add_dept(
    p_did IN departments.department_id%TYPE, --by default all the parameters are all the IN mode
    p_dname IN departments.department_name%TYPE,
    p_mid IN departments.manager_id%TYPE,
    p_lid IN departments.location_id%TYPE)
IS 
BEGIN
    INSERT INTO departments VALUES (p_did, p_dname, p_mid, p_lid);
    dbms_output.put_line('Department added.');
END;
/ --cmd command

EXECUTE add_dept(500, 'New Dept', 104, 1000); --cmd command

SELECT location_id FROM locations; --cmd command
SELECT * FROM departments WHERE department_id = 500; --cmd command

-----------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE add_dept(
    p_did IN departments.department_id%TYPE,
    p_dname OUT departments.department_name%TYPE)
IS
BEGIN
    SELECT department_name
    INTO p_dname 
    FROM departments
    WHERE department_id = p_did;
END;
/ --cmd command

DECLARE 
    dname varchar(20);
BEGIN
    add_dept(500, dname);
    dbms_output.put_line(dname);
END;
/

-----------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE format_phone_number(p_phone_no IN OUT varchar2) IS
BEGIN
    p_phone_no := '(' || SUBSTR(p_phone_no, 1, 3) || ')' || --three numbers starting from 1
                         SUBSTR(p_phone_no, 4, 3) || '-' || --the next three numbers 
                         SUBSTR(p_phone_no, 7);
END;
/

DECLARE
    v_p_no VARCHAR2(15);
BEGIN
    v_p_no := '1234567890';
    format_phone_number(v_p_no);
    dbms_output.put_line(v_p_no);
END;

-----------------------------------------------------------------------------------------------------------------------------------------------







































































