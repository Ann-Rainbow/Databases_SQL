-- realization in Oracle / реализация в Оракл
-- CREATE OR REPLACE PROCEDURE filling_D_product( -- проверить название процедуры в соответствии с внутренним стандартом организации
--     pr_id IN D_product.product_id%TYPE -- можно ли называть одинаковым именем, как и в столбце таблицы?
--     pr_name IN D_product.p_name%TYPE
--     pr IN D_product.price%TYPE
--     prlog_id IN D_product.log_id%TYPE
-- )
-- IS 
-- BEGIN
--     INSERT INTO D_product VALUES (pr_id, pr_name, pr, prlog_id);
--     dbms_output.put_line('Product added.')
-- END;

-- EXECUTE filling_D_product(1, 'Apple', 20, 00001); --cmd command

-- SELECT * FROM D_product WHERE product_id = 1; --cmd command


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
-- CREATE PROCEDURE filling_D_product 
--     @product_id BIGSERIAL, 
--     @p_name VARCHAR(50), 
--     @price MONEY, 
--     @log_id INTEGER
--    AS  
--    BEGIN  
--       -- The print statement returns text to the user  
--       --PRINT 'Enter the data ' + CAST(@price AS varchar(50));  
--       -- A second statement starts here  
--       --SELECT ProductName, Price FROM vw_Names    
--       SET CAST(@price AS varchar(50));
--       SET @p_name = LTRIM(RTRIM(@p_name));
    
--       INSERT INTO D_product (product_id, p_name, price, log_id)
--         VALUES (@product_id, @p_name, @price, @log_id)

--       SELECT * FROM D_product
--       WHERE product_id = @product_id

--    END  
-- GO   
-- не понятно, сохранилась ли процедура.
----------------------------------------------------------------------------------------------------------
"files.encoding": "windows1251"

-- Образец --

create or replace package Load_from_sa is

/*----------------------------------------------------------
  -- created: --
  -- changes:
    -- 20181012 - ��������� merge ��� meta$metacolumn 
        - ��� ��������� ���������� � UPPERCASE
        - ��������� ���������� ID
        - �������� �������� �������
-------------------------------------------------------*/

  v_to_uppercase boolean;

  procedure load;
  function to_uppercase(p_str varchar2) return varchar2;
  procedure init_params(p_project varchar2);
                                                  
end Load_from_sa;

create or replace package body Load_from_sa is

procedure load is
  v_rec_metatab meta$metatable%rowtype;
  v_project meta$metatable.project%type;
  v_tab_name meta$metatable.tab_name%type;

  row_count number;
begin
  

  for tab in (select distinct tab_name, project from sa_metatable order by tab_name) loop
    v_tab_name := tab.tab_name;
    v_project := tab.project;

    init_params(v_project);

    for cur_row in (select * from sa_metatable where tab_name = tab.tab_name and project = tab.project
                    and col_name is null) loop
                    
      if instr(cur_row.num, '�������:') > 0 then
        v_rec_metatab.tab_name := to_uppercase(cur_row.col_type);
        v_rec_metatab.project := cur_row.project;
        v_rec_metatab.curr_date := to_date(substr(cur_row.is_historical, 1, 10), 'yyyy/mm/dd');
      elsif instr(cur_row.num, '��� �������') > 0 then
        v_rec_metatab.tab_type := cur_row.col_type;
      elsif instr(cur_row.num, '������ �����:') > 0 then
        v_rec_metatab.descr := cur_row.col_type;
      elsif instr(cur_row.num, '��������:') > 0 then
        v_rec_metatab.source := cur_row.col_type;
        v_rec_metatab.alias := cur_row.source;
      elsif instr(cur_row.num, '������� ����������') > 0 then
        v_rec_metatab.filter := cur_row.col_type;
      elsif instr(cur_row.num, '����� ��������') > 0 then
        v_rec_metatab.load_method := cur_row.col_type;
      elsif instr(cur_row.num, '������� �������') > 0 then
        if cur_row.col_type = '��' then
          v_rec_metatab.is_historical := 1;
        else
          v_rec_metatab.is_historical := 0;
        end if;         
      elsif instr(cur_row.num, '������� ��������:') > 0 then
        v_rec_metatab.storage_conditions := cur_row.col_type;
      end if;
    end loop;

    select count(*) into row_count from meta$metatable t
                             where t.project = tab.project and t.tab_name = tab.tab_name;

    if row_count > 0 then
       update meta$metatable set
        curr_date = v_rec_metatab.curr_date,
        tab_type = v_rec_metatab.tab_type,
        descr = v_rec_metatab.descr,
        source = v_rec_metatab.source,
        alias = v_rec_metatab.alias,
        filter = v_rec_metatab.filter,
        load_method = v_rec_metatab.load_method,
        is_historical = v_rec_metatab.is_historical,
        storage_conditions = v_rec_metatab.storage_conditions
        where project = tab.project and tab_name = tab.tab_name;
    else
       insert into meta$metatable values v_rec_metatab;
    end if;
    commit;

    dbms_output.put_line('--BF merge ' || tab.project || ':' || tab.tab_name);

    merge into meta$metacolumn col using
          (select to_uppercase(col_name) col_name, col_type
              --, pk, uk
              , regexp_substr( pk, '[{0-9}]+',1,1) pk
              , regexp_substr( uk, '[{0-9}]+',1,1) uk
              , regexp_substr( num, '[{0-9}]+',1,1) num_as_id
              --, change_num
              , regexp_substr(change_num, '[{0-9}]+',1,1) change_num
              , descr, fk, project, tab_name,
          case when fk is not null then 1 else null end is_fk,
          case when is_nullable is not null then 1 else null end is_nullable
          from sa_metatable
          where to_uppercase(tab_name) = to_uppercase(tab.tab_name) and project = tab.project and col_name is not null and num <> '� �/�') sa
    on (upper(col.col_name) = upper(sa.col_name) and upper(col.tab_name) = upper(sa.tab_name) and col.project = sa.project)
    when matched then
      update set col.col_data_tp = sa.col_type,
      col.is_pk = sa.pk,
      col.is_fk = sa.is_fk,
      col.ref_tab_name = sa.fk,
      col.is_bus_key = sa.uk,
      col.is_nullable = sa.is_nullable,
      col.descr = sa.descr,
      col.change_number = sa.change_num
      , col.id = sa.num_as_id
    when not matched then
      insert (col.tab_name,
        col.project,
        col.col_name,
        col.col_data_tp,
        col.is_pk,
        col.is_fk,
        col.ref_tab_name,
        col.is_bus_key,
        col.is_nullable,
        col.descr,
        col.change_number
        , col.id)
      values (to_uppercase(sa.tab_name),
        sa.project,
        to_uppercase(sa.col_name),
        to_uppercase(sa.col_type),
        sa.pk,
        sa.is_fk,
        sa.fk,
        sa.uk,
        sa.is_nullable,
        sa.descr,
        sa.change_num
        , sa.num_as_id);
    commit;
    -- �������� ���������� �������
    delete from meta$metacolumn m 
    where 
      (upper(m.project), upper(m.tab_name)) in 
         (select upper(sa.project), upper(sa.tab_name) from sa_metatable sa 
            where sa.project is not null and sa.tab_name is not null)
      and 
      (upper(m.project), upper(m.tab_name), upper(m.col_name)) not in
        ( select upper(sa.project), upper(sa.tab_name), upper(sa.col_name) from sa_metatable sa
          where sa.project is not null and sa.tab_name is not null and sa.col_name is not null
        )
    ;
    commit;

  end loop;

end;

procedure init_params(p_project varchar2) is
  v_str varchar2(100);
  begin
    begin 
      select upper(prop_value) into v_str from datavault.meta$properties s
      where s.prop_name='to_uppercase' and project=p_project;
     if upper(v_str)='TRUE' then
       v_to_uppercase:=true;
     else
       v_to_uppercase:=false;
     end if;    
    exception
      when no_data_found
        then v_to_uppercase:=true;
    end;
  end   init_params;
  
  function to_uppercase(p_str varchar2) return varchar2 is
  begin
    if v_to_uppercase then
      return upper(p_str);
    else
      return p_str;
    end if;  
  end to_uppercase;
end Load_from_sa;











