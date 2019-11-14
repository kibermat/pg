/**
 * PgPartition v 1.0
 * for PostgreSQL 11
 * no dependencies
 * author: Almaz Mustakimov
 */


/* set configs
SET max_locks_per_transaction to 128; --restart
SET enable_partition_pruning to on;

set synchronous_commit to off;
max_parallel_workers(8); <--max_worker_processes()
*/

CREATE SCHEMA IF NOT EXISTS partitions;

SET search_path TO partitions;

CREATE SEQUENCE IF NOT EXISTS partitions_serial START 111;

drop table if exists partition_tables;
create table if not exists partition_tables
(
    sh            text,
    tbl           text,
    date_field    text,
    date_from     date,
    date_to       date,
    intervl       interval,
    status        integer default null,
    serial_number integer default nextval('partitions_serial'),
    CONSTRAINT pk_partition_tables PRIMARY KEY (tbl)
);
comment on table partition_tables
is 'The table contains data for partitioning';

comment on column partition_tables.status
is 'processing status 200 - success ';


/*
 * Input params 
 */
insert into partition_tables
    values
     ('tf_proc', 'tp_caseraw', 'date_2', '2018-01-01'::date, '2021-01-01'::date, '1 month', null)
    ,('tf_proc', 'tp_case', 'date_2', '2018-01-01'::date, '2021-01-01'::date, '1 month', null)
    ,('tf_proc', 'tp_casebill', 'date_2', '2018-01-01'::date, '2021-01-01'::date, '1 month', null)
    ,('core', 'log', 'log_date', '2018-01-01'::date, '2021-01-01'::date, '1 month', 200)
;



create table if not exists partition_fn_constraints
(
    fn_name    text,
    from_sh    text,
    from_tbl   text,
    from_field text,
    prt_sh     text,
    prt_tbl    text,
    prt_field  text,
    fn_args    text default '()',
    CONSTRAINT pk_partition_fn_constraints PRIMARY KEY (fn_name)
);

create table if not exists partition_tr_constraints
(
    tr_name text,
    fn_name text,
    sh      text,
    tbl     text,
    CONSTRAINT pk_partition_tr_constraints PRIMARY KEY (tr_name),
    CONSTRAINT pk_partition_tr_constraints_fn FOREIGN KEY (fn_name)
        REFERENCES partition_fn_constraints (fn_name)
        ON DELETE CASCADE
        ON UPDATE CASCADE
        NOT DEFERRABLE
);

create table if not exists partition_redo_journal
(
    action     text,
    object     text,
    context    text,
    query      text,
    date_write timestamp  default current_timestamp,
    proc_pid   int        default pg_backend_pid(),
    c_user     name       default current_user,
    serial_number integer default nextval('partitions_serial')
);

create table if not exists partition_undo_journal
(
    action        text,
    object        text,
    context       text,
    query         text,
    date_write    timestamp,
    proc_pid      int       default pg_backend_pid(),
    c_user        name      default current_user,
    serial_number integer   default nextval('partitions_serial')
);

create table if not exists partition_deferred_jobs
(
    name          text,
    action        text,
    before_action text      default null,
    after_action  text      default null,
    status        int       default null,
    proc_pid      int       default pg_backend_pid(),
    date_write    timestamp default current_timestamp,
    c_user        name      default current_user,
    serial_number integer   default nextval('partitions_serial')
);


/**
 * functions
 */

--для отладки
CREATE OR REPLACE FUNCTION _partition_tmp_def_constraint(_fn_name text, _from_sh text, _from_tbl text, _from_field text,
                                                         _prt_sh text, _prt_tbl text, _prt_field text)
    RETURNS text AS
$body$
declare
    _sql     text;
    _fn_args text default '()';
BEGIN

    _sql := 'CREATE OR REPLACE FUNCTION ' || _fn_name || _fn_args||' RETURNS trigger AS ' ||
            $declare$
$$
DECLARE
  _fk      boolean default false;
  _v0      numeric;
  _v1      numeric;
  _msg     text;
BEGIN
$declare$ || '
  EXECUTE format(''select $1.%I, $2.%I'', ''' || _from_field || ''', ''' || _from_field || ''') USING OLD, NEW INTO _v0, _v1;

  if _v1 is null or _v0 = _v1 then
     RETURN NEW;
  end if;

  select true from ' || quote_ident(_prt_sh) || '.' || quote_ident(_prt_tbl) ||
            ' where ' || quote_ident(_prt_field) || ' = _v1 LIMIT 1 INTO _fk;

  IF NOT FOUND OR NOT _fk THEN
      RAISE EXCEPTION E''% в таблице "%" (триггер).\nDETAIL: На ключ (' || _from_field || ')=(%) нет ссылки в таблице "' ||
            _prt_tbl || '" в поле ' || _prt_field || ' '',
      TG_OP, TG_TABLE_NAME, _v1;
  END IF;

  RETURN NEW;

END;' ||
            $end$
$$ LANGUAGE plpgsql;
$end$;
    execute _sql;
    return _fn_name;
end;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;

--для отладки
CREATE OR REPLACE FUNCTION _partition_tmp_recreate_fns()
    RETURNS pg_catalog.void AS
$body$
declare
    _r       record;
    _fn_name text;
BEGIN
    for _r in SELECT
                  fn_name,
                  from_sh,
                  from_tbl,
                  from_field,
                  prt_sh,
                  prt_tbl,
                  prt_field,
                  fn_args
              FROM partition_fn_constraints
              WHERE from_field is not null
        LOOP
            _fn_name := _partition_tmp_def_constraint(_r.fn_name, _r.from_sh, _r.from_tbl, _r.from_field, _r.prt_sh, _r.prt_tbl, _r.prt_field);
        END LOOP;

END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Запись логов
 */
CREATE OR REPLACE PROCEDURE _partition_write_log(_action text, _object text, _context text, _query text) AS
$body$
declare
    _dt timestamp;
BEGIN
    _dt := clock_timestamp();
    if _action = 'undo' then
        insert into partitions.partition_undo_journal values (_action, _object, _context, _query, _dt);
        --commit;
    else
        insert into partitions.partition_redo_journal values (_action, _object, _context, _query, _dt);
        --commit;
        raise notice  '% %>>>% % %', pg_backend_pid(), _dt, _action, _object, _context;
    end if;
END
$body$
    LANGUAGE plpgsql;


/*
 * Получить параметры парт.
 */
CREATE OR REPLACE FUNCTION _partition_table_row(_tbl varchar)
    RETURNS record AS
$body$
declare
    _row record;
BEGIN
    select * from partition_tables t where t.tbl = _tbl limit 1 into _row;

    IF NOT FOUND THEN
        return null;
    END IF;

    RETURN _row;

END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Запуск отложенных задач
 */
CREATE OR REPLACE PROCEDURE partition_run_jobs(_name text default null, _pid int default null) AS
$body$
DECLARE
    _r record;
    _main_action text;
BEGIN
    call _partition_write_log('start jobs', coalesce('pid '||_pid, 'common'), 'partition_run_jobs', null);

    for _r in with cte as (
        select "name",
               before_action,
               after_action,
               "action",
               serial_number
        from partition_deferred_jobs
        where (proc_pid = _pid or _pid is null)
          and (name = _name or _name is null)
          and status is null
          and ((_pid is not null and _name is not null) or
               pg_catalog.pg_try_advisory_xact_lock(tableoid::INTEGER, serial_number))
        order by serial_number
    )
              select "name",
                     before_action,
                     after_action,
                     array_agg(distinct "action" order by "action") actions,
                     array_agg(serial_number) serials
              from cte
              group by 1, 2, 3

        loop
            if _r.before_action is not null then
                call _partition_write_log('run jobs', _r.before_action, 'partition_run_jobs', _r.before_action);
                execute _r.before_action;
            end if;

            foreach _main_action in array _r.actions
                loop
                    if _main_action isnull then
                        continue;
                    end if;
                    call _partition_write_log('run jobs', substr(_main_action, 0, 55), 'partition_run_jobs', _main_action);
                    execute _main_action;
                end loop;

            if _r.after_action is not null then
                call _partition_write_log('run jobs', _r.after_action, 'partition_run_jobs', _r.after_action);
                execute _r.after_action;
            end if;

            perform _partition_job_update(_r.serials);

        end loop;

    call _partition_write_log('finish jobs', coalesce('pid '||_pid, 'common'), 'partition_run_jobs', null);

END
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Ввод входных данных
 */
CREATE OR REPLACE FUNCTION _partition_insert_params(_sh text, _tbl text, _field text, _from date, _to date, _interval interval)
    RETURNS pg_catalog.void AS
$body$
BEGIN
    insert into partition_tables values (_sh, _tbl, _field, _from, _to, _interval);
END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Зарегистрировать задачу
 */
CREATE OR REPLACE FUNCTION _partition_set_job(_name varchar, _action varchar,
                                              _before_action varchar default null,
                                              _after_action varchar default null,
                                              _status int default null,
                                              _pid int default pg_backend_pid())
    RETURNS pg_catalog.void AS
$body$
BEGIN
    insert into partition_deferred_jobs
        values (_name, _action, _before_action, _after_action, _status, _pid, current_timestamp)
    ;
END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Обновить partition_deferred_jobs
 */
CREATE OR REPLACE FUNCTION _partition_job_update(_serial_number int[])
    RETURNS pg_catalog.void AS
$body$
BEGIN

    update partition_deferred_jobs u
    set status = 1
    where u.serial_number = any (_serial_number);

    call _partition_write_log('set job status', '1', '_partition_job_update', null);

END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Обновить partition_table
 */
CREATE OR REPLACE FUNCTION _partition_table_update(_pt record, _status numeric default 1)
    RETURNS pg_catalog.void AS
$body$
BEGIN

    update partition_tables u set status = _status
    where u.tbl = _pt.tbl;

    /* TODO удалить вн.ключи  */
    --call _partition_drop_references(_pt.sh, _pt.tbl);

END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Выбрать название для сущности органичения
 * @dependent function _partition_create_event_trigger
 */
CREATE OR REPLACE FUNCTION _partition_choose_constraint_name(_kind varchar, _tbl varchar, _from varchar)
    RETURNS text AS
$body$
declare
    _name text;
BEGIN
    if _kind = 'f' then
        _name := format('%s_fk_prt_%s_%s', _kind, _tbl, _from);
    elsif _kind = 'fd' then
        _name := format('%s_fk_prt_%s_%s', _kind, _tbl, _from);
    elsif _kind = 'tr' then
        _name := format('%s_%s', _kind, _from);
    else
        raise exception 'partition_choose_constraint_name>>> constraint % not implemented', _kind;
    end if;
    return _name;
END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/* Создаем новую парицированную таблилицу */
CREATE OR REPLACE FUNCTION _partition_create_parent_table(_pt record)
    RETURNS pg_catalog.void AS
$body$
declare
    _r    record;
    _sql  text;
    _sh   text;
    _tbl  text;
    _undo text;
BEGIN
    with cel as (
        SELECT table_schema,
               table_name,
               array_agg(
                       concat(
                               column_name, ' ', data_type,
                               coalesce('(' || character_maximum_length::text || ')',
                                        '(' || numeric_precision || ',' || numeric_scale || ')', ''),
                               coalesce(' DEFAULT ' || column_default, ''),
                               case when is_nullable = 'NO' then ' NOT NULL ' else '' end
                           ) order by ordinal_position) v
        FROM information_schema.columns
        WHERE table_name = _pt.tbl
          and table_schema = _pt.sh
        group by 1,2
    )
    select concat('CREATE TABLE ', _pt.tbl, ' (', chr(10), chr(9), array_to_string(v, ', ' || chr(10) || chr(9)),
                  ') PARTITION BY RANGE (', _pt.date_field, ')'),
           concat('CREATE TABLE IF NOT EXISTS ', _pt.sh, '.', _pt.tbl,
                  ' (', chr(10), chr(9), array_to_string(v, ', ' || chr(10) || chr(9)), ') '),
           current_schema(),
           _pt.tbl
    into _sql, _undo, _sh, _tbl
    from cel;

    EXECUTE _sql;
    call _partition_write_log('undo', _pt.tbl, '_partition_create_parent_table', _undo); _undo := '';

    FOR _r IN SELECT row_number() over ()     n,
                     d.description,
                     cl.column_name,
                     obj_description(pgc.oid) dscr
              FROM pg_catalog.pg_description d
                       join information_schema.columns cl on d.objsubid = cl.ordinal_position
                       join pg_class pgc on d.objoid = pgc.oid
                  and pgc.relname = _pt.tbl
                  and pgc.relnamespace = (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = _pt.sh)
              WHERE cl.table_name = _pt.tbl
                AND cl.table_schema = _pt.sh

        loop
            if _r.n = 1 then
                EXECUTE format('COMMENT ON TABLE %I.%I
                        IS %L;', _sh, _tbl, concat(_r.dscr, ' (Партиция)')
                    );
                _undo := _undo ||';'||chr(10) || format('COMMENT ON TABLE %I.%I IS %L', _pt.sh, _pt.tbl, _r.dscr);
            end if;
            EXECUTE format('COMMENT ON COLUMN %I.%I.%I
                      IS %L;', _sh, _tbl, _r.column_name, _r.description
                );
            _undo := _undo ||';'||chr(10)||  format('COMMENT ON COLUMN %I.%I.%I IS %L', _pt.sh, _pt.tbl, _r.column_name, _r.description);
        end loop;

    call _partition_write_log('create', _pt.tbl, '_partition_create_parent_table', _sql);
    call _partition_write_log('undo', _pt.tbl, '_partition_create_parent_table', _undo);

END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Получить ссылки ( table -> x )
 */
CREATE OR REPLACE FUNCTION _partition_get_references(_sh VARCHAR, _tbl VARCHAR)
    RETURNS TABLE
            (
                constraint_oid    oid, --pg_get_constraintdef
                constraint_name   information_schema.table_constraints.constraint_name%type,
                constraint_type   information_schema.table_constraints.constraint_type%type,
                constraint_sh     information_schema.table_constraints.constraint_schema%type,
                constraint_tbl    information_schema.table_constraints.table_name%type,
                constraint_column text,
                references_sh     information_schema.constraint_column_usage.constraint_schema%type,
                references_tbl    information_schema.constraint_column_usage.table_name%type,
                references_column information_schema.constraint_column_usage.column_name%type
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH cte as (
            SELECT pg_get_constraintdef(pgc.oid)            AS constraint_def,
                   tc.constraint_name                       AS "constraint_name",
                   tc.constraint_type                       AS constraint_type,
                   tc.constraint_schema                     AS constraint_sh,
                   tc.table_name                            AS constraint_tbl,
                   ccu.constraint_schema                    AS references_sh,
                   ccu.table_name                           AS references_tbl,
                   ccu.column_name                          AS references_column,
                   array_agg(kcu.column_name)               AS arr_constraint_column,
                   array_agg(pgc.oid order by pgc.oid desc) AS arr_oid
            FROM information_schema.table_constraints tc
                     LEFT JOIN information_schema.key_column_usage kcu
                               ON tc.constraint_catalog = kcu.constraint_catalog
                                   AND tc.constraint_schema = kcu.constraint_schema
                                   AND tc.constraint_name = kcu.constraint_name
                     LEFT JOIN information_schema.referential_constraints rc
                               ON tc.constraint_catalog = rc.constraint_catalog
                                   AND tc.constraint_schema = rc.constraint_schema
                                   AND tc.constraint_name = rc.constraint_name
                     LEFT JOIN information_schema.constraint_column_usage ccu
                               ON rc.unique_constraint_catalog = ccu.constraint_catalog
                                   AND rc.unique_constraint_schema = ccu.constraint_schema
                                   AND rc.unique_constraint_name = ccu.constraint_name
                     LEFT JOIN pg_catalog.pg_constraint pgc
                               ON pgc.conname::information_schema.sql_identifier = tc.constraint_name
                                   AND pgc.connamespace = tc.constraint_schema::regnamespace
            WHERE tc.table_name = _tbl
              AND tc.table_schema = _sh
              AND pg_get_constraintdef(pgc.oid) is not null
            GROUP BY 1,2,3,4,5,6,7,8
        ) SELECT t.arr_oid [1]                                 AS constraint_oid,
                 t."constraint_name"                           AS "constraint_name",
                 t.constraint_type                             AS constraint_type,
                 t.constraint_sh                               AS constraint_sh,
                 t.constraint_tbl                              AS constraint_tbl,
                 array_to_string(t.arr_constraint_column, ',') AS constraint_column,
                 t.references_sh                               AS references_sh,
                 t.references_tbl                              AS references_tbl,
                 t.references_column                           AS references_column
        FROM cte t;
END;
$$
    LANGUAGE 'plpgsql';


/*
 * Получить ограничения ( x -> table )
 */
CREATE OR REPLACE FUNCTION _partition_get_constraints(_sh VARCHAR, _tbl VARCHAR)
    RETURNS TABLE
            (
                constraint_oid    oid, --pg_get_constraintdef
                constraint_name   information_schema.key_column_usage.constraint_name%type,
                constraint_type   information_schema.table_constraints.constraint_type%type,
                constraint_sh     information_schema.key_column_usage.constraint_schema%type,
                constraint_tbl    information_schema.key_column_usage.table_name%type,
                constraint_column information_schema.key_column_usage.column_name%type,
                part_sh           information_schema.constraint_table_usage.constraint_schema%type,
                part_tbl          information_schema.constraint_column_usage.table_name%type,
                part_column       information_schema.constraint_column_usage.column_name%type
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT distinct cn.oid              as constraint_oid,
                        k.constraint_name   as constraint_name,
                        c.constraint_type   as constraint_type,
                        k.constraint_schema as constraint_sh,
                        k.table_name        as constraint_tbl,
                        k.column_name       as constraint_column,
                        t.constraint_schema as part_sh,
                        l.table_name        as part_tbl,
                        l.column_name       as part_column
        FROM information_schema.constraint_table_usage t
                 JOIN information_schema.constraint_column_usage l
                      ON t.constraint_name = l.constraint_name
                 JOIN information_schema.key_column_usage k
                      ON t.constraint_name = k.constraint_name
                 JOIN information_schema.table_constraints c
                      ON t.constraint_name = c.constraint_name
                 JOIN pg_catalog.pg_constraint cn
                      ON cn.conname = t.constraint_name
        WHERE t.table_name = _tbl
          AND t.table_schema = _sh;
END;
$$
    LANGUAGE 'plpgsql';


/*
 * Пересоздать зависимые вью на партицию
 */
CREATE OR REPLACE FUNCTION _partition_replace_view(_pt record)
    RETURNS pg_catalog.void
AS
$$
declare
    _r record;
    _v record;
    _sql text;
BEGIN
    FOR _r IN with cte as (
        SELECT distinct
            r.ev_class::regclass::text as _name,
            pg_get_viewdef(r.ev_class::regclass, false) as _ddl
        FROM pg_catalog.pg_attribute as a
                 join pg_catalog.pg_depend as d on d.refobjid = a.attrelid AND d.refobjsubid = a.attnum
                 join pg_catalog.pg_rewrite as r ON d.objid = r.oid
        WHERE a.attrelid::regclass = concat(_pt.sh, '.', _pt.tbl)::regclass
    ) SELECT concat('CREATE OR REPLACE VIEW ', _name, ' AS ', _ddl) as undo
              FROM cte WHERE _ddl IS NOT NULL

        LOOP
            _sql := _r.undo;

            FOR _v IN select * from partition_tables order by serial_number
                LOOP
                    _sql := regexp_replace(_sql,
                                           concat(_v.sh, '.', _v.tbl, '(\s)'),
                                           concat(current_schema(), '.', _v.tbl, '\1'), 'gin');
                END LOOP;

            --TODO replace view
            call _partition_write_log('undo', _pt.tbl, '_partition_replace_view', _r.undo);
            perform _partition_set_job('1200 replace view', _sql, null, null, null, -1);

        END LOOP;
END;
$$
    LANGUAGE 'plpgsql'
    SET search_path TO partitions;


/*
 * Удаление связей 
 * DROP CONSTRAINTS
 */
CREATE OR REPLACE PROCEDURE _partition_drop_references(_sh text, _tbl text) AS
$body$
declare
    _sql text;
BEGIN
   select concat('ALTER TABLE ', _sh, '.', _tbl, 
          array_to_string( array_agg( chr(10)||' DROP CONSTRAINT '|| t.constraint_name ), ', ') ) as s 
    into _sql 
    from partitions._partition_get_references(_sh, _tbl) as t;

   if not found then 
      return;
   end if; 

   call _partition_write_log('drop references', _tbl, '_partition_drop_references', _sql);

   perform _partition_set_job('1101 drop references', _sql);

END;
$body$
    LANGUAGE plpgsql;


/*
 * Восстанавливает связи таблицы
 * SET CONSTRAINTS ALL DEFERRED
 */
CREATE OR REPLACE FUNCTION _partition_restore_referrences(_pt record)
    RETURNS pg_catalog.void AS
$body$
declare
    _r record;
BEGIN
    FOR _r IN  with r as (
        select constraint_type,
               "constraint_name",
               array_agg(references_tbl order by constraint_oid desc) as arr_ref_tbl,
               array_agg(distinct constraint_oid order by constraint_oid desc) arr_sql_oid
        from _partition_get_references( _pt.sh,  _pt.tbl)
        where pg_get_constraintdef(constraint_oid) is not null
        group by 1,2
    ), q as (
        select pg_get_constraintdef(unnest(arr_sql_oid)) as "sql",
               arr_ref_tbl,
               constraint_type,
               "constraint_name"
        from r
    )
    select case when r.constraint_type = 'PRIMARY KEY' then
                     concat('ALTER TABLE  ', _pt.tbl, ' ADD CONSTRAINT ', r.constraint_name, '_prt ',
                            replace(r."sql", ')', ', '||_pt.date_field||')') )
                 else
                     concat('ALTER TABLE  ', _pt.tbl, ' ADD CONSTRAINT ', r.constraint_name, '_prt ', r."sql")
                end as "sql",
            concat('ALTER TABLE  ', _pt.sh, '.', _pt.tbl, ' ADD CONSTRAINT ', r.constraint_name, ' ', r."sql") undo,
            r.arr_ref_tbl
     from q as r

        LOOP
            -- если ссылаемся не на партицию
            if _partition_table_row(_r.arr_ref_tbl[1]) is null then
                begin
                    SET CONSTRAINTS ALL DEFERRED;
                    EXECUTE _r.sql;
                EXCEPTION when others then
                    call _partition_write_log('error recreate_constraint_table', _pt.tbl, SQLERRM, _r.sql);
                    continue;
                end;
                call _partition_write_log('add constraint', _pt.tbl, '_partition_recreate_constraint_table', _r.sql);
            end if;

            call _partition_write_log('undo', _pt.tbl, '_partition_recreate_constraint_table', _r.undo);

        END LOOP;

END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/* Создаем дочерние таблицы */
CREATE OR REPLACE FUNCTION _partition_create_child_tables(_pt record)
    RETURNS pg_catalog.void AS
$body$
declare
    _r   record;
    _sql varchar;
BEGIN
    FOR _r IN WITH RECURSIVE
                  /* входные данные */
                  dates AS (
                      SELECT _pt.sh                as sh,
                             _pt.tbl               as tbl,
                             _pt.date_from :: DATE AS dt1,
                             _pt.date_to :: DATE   AS dt2,
                             _pt.intervl           AS interval
                  ), pr AS (
            SELECT 1                       AS i,
                   (SELECT dt1 FROM dates) AS dt
            UNION
            SELECT i + 1                                              AS i,
                   ((SELECT dt1
                     FROM dates) + (SELECT interval
                                               FROM dates) * i) :: DATE AS dt
            FROM pr
            WHERE (((SELECT dt1
                     FROM dates) + (SELECT interval
                                               FROM dates) * i) :: DATE) <= (SELECT dt2
                                                                             FROM dates)
        ), prt as (
            SELECT _pt.sh,
                   _pt.tbl                                         tbl_from,
                   concat(_pt.tbl, '_', to_char(pr.dt, 'yyyy_mm')) tbl_to,
                   lag(pr.dt) over (order by pr.dt)                date_from,
                   pr.dt                                           date_to
            FROM pr
            order by pr.dt
        )
              select _pt.sh,
                     _pt.tbl                      tbl_from,
                     concat(_pt.tbl, '_minvalue') tbl_to,
                     '-infinity'::date            date_from,
                     _pt.date_from                date_to
              union all
              select *
              from prt p
              where p.date_from is not null
                and p.date_to is not null
              union all
              select _pt.sh,
                     _pt.tbl                      tbl_from,
                     concat(_pt.tbl, '_maxvalue') tbl_to,
                     _pt.date_to                  date_from,
                     'infinity'::date             date_to
        LOOP
            _sql := format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L); ',
                           _r.tbl_to, _r.tbl_from, _r.date_from, _r.date_to);

            EXECUTE _sql;

            call _partition_write_log('create', _r.tbl_to, '_partition_create_child_tables', _sql);

        END LOOP;
END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Создаем индексы для партицированной таблицы
 * уникальный индекс ставим при создании партиции
 */
CREATE OR REPLACE FUNCTION _partition_create_index(_pt record)
    RETURNS pg_catalog.void AS
$body$
declare
    _r record;
    _before_action text;
    _after_action text;
    _work_mem integer;
    _work_mem2 integer;
BEGIN

    select setting::integer,
           case when setting::integer * 2 < max_val::integer then setting::integer * 2 else max_val::integer end
    into _work_mem, _work_mem2
    from pg_settings
    where "name" = 'maintenance_work_mem';

    _before_action := 'set maintenance_work_mem to '|| _work_mem2;
    _after_action := 'set maintenance_work_mem to ' || _work_mem;

    for _r in
        with cte as (
            SELECT --replace(lower(pg_get_indexdef(c2.oid)), 'create index ', 'create index concurrently ') as ddl,
                   pg_get_indexdef(c2.oid) as ddl,
                   c2.relname as index_name,
                   t.relname,
                   array_to_string(array_agg(attname order by attnum), ', ') as fields
            FROM pg_index c
                     JOIN pg_class t
                          ON c.indrelid = t.oid
                     JOIN pg_class AS c2
                          ON (c2.oid = c.indexrelid)
                     LEFT JOIN pg_attribute a
                               ON a.attrelid = t.oid AND a.attnum = ANY (indkey)
            WHERE t.relnamespace::regnamespace::text = _pt.sh
              and t.relname = _pt.tbl
              and t.relkind = 'r'
              and not c.indisunique
            GROUP BY 1, 2, 3
        )
        select replace(replace(ddl, _pt.sh, current_schema()), '_' || relname, '_p_' || relname) q,
               ddl as undo,
               index_name
        from cte
        LOOP
            --execute _r.q;
            perform _partition_set_job('100 create index', _r.q, _before_action, _after_action);
            call _partition_write_log('create index', _r.index_name, '_partition_create_index', _r.q);
            call _partition_write_log('undo', _r.index_name, '_partition_create_index', _r.undo);
        end LOOP;

END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Реализация внешних ключей на партицию - определение инструментов
 * Создать функцию проверки для парт.таблицы
 * делает селект и сравнивает с NEW значение
 * @dependet event trigger prt_event_trigger_alter_table
 */
CREATE OR REPLACE FUNCTION _partition_def_constraint_fk(_from_sh text, _from_tbl text, _from_field text,
                                                        _prt_sh text, _prt_tbl text, _prt_field text)
    RETURNS text AS
$body$
declare
    _sql     text;
    _fn_name text;
    _fn_args text default '()';
BEGIN
    _fn_name := _partition_choose_constraint_name('f', _from_tbl, _from_field);

    /* FIXME вариант с одной функцией не прошел, на партициях не видет аргументы
    EXECUTE concat(''SELECT 1 from ' || quote_ident(_sh) || '.' || quote_ident(_tbl) ||
    ' where '', quote_ident(tg_argv[1]), '' = ('',
    quote_literal(NEW), ''::'', TG_RELID::regclass,'').'', quote_ident(tg_argv[0])) INTO _id;*/

    _sql := 'CREATE OR REPLACE FUNCTION ' || _fn_name || _fn_args||' RETURNS trigger AS ' ||
            $declare$
$$
DECLARE
  _fk      boolean default false;
  _v0      numeric;
  _v1      numeric;
  _msg     text;
BEGIN
$declare$ || '
  EXECUTE format(''select $1.%I, $2.%I'', ''' || _from_field || ''', ''' || _from_field || ''') USING OLD, NEW INTO _v0, _v1;

  if _v1 is null or _v0 = _v1 then
     RETURN NEW;
  end if;

  select true from ' || quote_ident(_prt_sh) || '.' || quote_ident(_prt_tbl) ||
            ' where ' || quote_ident(_prt_field) || ' = _v1 LIMIT 1 INTO _fk;

  IF NOT FOUND OR NOT _fk THEN
      RAISE EXCEPTION E''% в таблице "%" (триггер).\nDETAIL: На ключ (' || _from_field || ')=(%) нет ссылки в таблице "' ||
            _prt_tbl || '" в поле ' || _prt_field || ' '',
      TG_OP, TG_TABLE_NAME, _v1;
  END IF;

  RETURN NEW;

END;' ||
            $end$
$$ LANGUAGE plpgsql;
$end$;

    execute _sql;

    insert into partition_fn_constraints
        values (_fn_name, _from_sh, _from_tbl, _from_field, _prt_sh, _prt_tbl, _prt_field, _fn_args)
        ON CONFLICT (fn_name) DO UPDATE SET
        from_sh = EXCLUDED.from_sh,
        from_tbl = EXCLUDED.from_tbl,
        from_field = EXCLUDED.from_field,
        prt_sh = EXCLUDED.prt_sh,
        prt_tbl = EXCLUDED.prt_tbl,
        prt_field = EXCLUDED.prt_field,
        fn_args = EXCLUDED.fn_args;

    call _partition_write_log('create function', _fn_name, '_partition_def_constraint_fk', _sql);

    return _fn_name;
end;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * FIXME: Update по ключу парт., в Posgtes реализован через delete/insert
 * Реализация внешних ключей на партицию - определение инструментов
 * Создать функцию проверки для парт.таблицы on delete
 * @dependet event trigger prt_event_trigger_alter_table
 */
CREATE OR REPLACE FUNCTION _partition_def_constraint_fk_on_delete(_pt record)
    RETURNS text AS
$body$
declare
    _sql     text;
    _fn_name text;
    _fn_args text default '()';
    _depend_tables text;
BEGIN
    _fn_name := _partition_choose_constraint_name('fd', _pt.tbl, 'id');

    /* FIXME вариант с одной функцией не прошел, на партициях не видет аргументы
    EXECUTE concat(''SELECT 1 from ' || quote_ident(_sh) || '.' || quote_ident(_tbl) ||
    ' where '', quote_ident(tg_argv[1]), '' = ('',
    quote_literal(NEW), ''::'', TG_RELID::regclass,'').'', quote_ident(tg_argv[0])) INTO _id;*/

    select array_to_string(array_agg(concat(t.constraint_sh, '.', t.constraint_tbl, ',', t.constraint_column)
                                     order by t.constraint_tbl), ',')
    into _depend_tables
    from _partition_get_constraints(_pt.sh, _pt.tbl) t
    where t.constraint_type = 'FOREIGN KEY'
      and t.constraint_sh != current_schema()
      and t.part_column = 'id';

    if not found then
        return null;
    end if;

    _sql := 'CREATE OR REPLACE FUNCTION ' || _fn_name || _fn_args||' RETURNS trigger AS ' ||
            $declare$
$$
DECLARE
  _depend_table text;
  _depend_ref text;
  _ref_depend_tables text[];
  _is_found boolean;
  _i integer;
BEGIN
$declare$ || '
  select true from '||_pt.sh||'.'||_pt.tbl||' where id = OLD.id limit 1 into _is_found;
  
  if _is_found then 
    RETURN OLD;
  end if;

  _ref_depend_tables := string_to_array(''' || _depend_tables ||''', '','');
  _i := 1;
  WHILE _i <= array_length(_ref_depend_tables, 1)
  LOOP
    _depend_table := _ref_depend_tables[_i];
    _i := _i + 1;
    _depend_ref := _ref_depend_tables[_i];
    _i := _i + 1;

    EXECUTE ''SELECT TRUE FROM '' || _depend_table ||
        '' WHERE '' || quote_ident(_depend_ref) || '' = $1.id LIMIT 1'' USING OLD INTO _is_found;

    IF _is_found THEN
      RAISE EXCEPTION E''% в таблице "%" (триггер).\nDETAIL: На ключ (id)=(%) всё ещё есть ссылка в таблице "%" в поле "%"'',
      TG_OP, TG_RELNAME, OLD.id, _depend_table, _depend_ref;
    END IF;
  END LOOP;

  RETURN OLD;

END;' ||
            $end$
$$ LANGUAGE plpgsql;
$end$;

    execute _sql;

    insert into partition_fn_constraints
        values (_fn_name, _pt.sh,  _pt.tbl, null, _pt.sh,  _pt.tbl, null, _fn_args)
        ON CONFLICT (fn_name) DO NOTHING;

    call _partition_write_log('create function on delete', _fn_name, '_partition_def_constraint_fk_on_delete', _sql);

    return _fn_name;
end;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Создать constraint-триггер на парт.таблицы
 */
CREATE OR REPLACE FUNCTION _partition_def_constraint_tr(_sh text, _tbl text, _fn_name text)
    RETURNS text AS
$body$
declare
    _tr_name   text;
    _sql       text;
BEGIN
    _tr_name := _partition_choose_constraint_name('tr', _tbl, _fn_name);

    EXECUTE format('drop trigger if exists %I on %I', _tr_name, _tbl);

    _sql := format('create constraint trigger %I AFTER INSERT OR UPDATE on %I.%I
            for each row
            execute function %I()', _tr_name, _sh, _tbl, _fn_name);

    EXECUTE _sql;

    insert into partition_tr_constraints values (_tr_name, _fn_name, _sh, _tbl)
        ON CONFLICT (tr_name) DO NOTHING;

    call _partition_write_log('create trigger', _tr_name, '_partition_def_constraint_tr', _sql);

    return _tr_name;
end;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Создать constraint-триггер на парт.таблицы on delete
 */
CREATE OR REPLACE FUNCTION _partition_def_tr_on_delete(_pt record)
    RETURNS text AS
$body$
declare
    _tr_name   text;
    _fn_name   text;
    _sql       text;
BEGIN
    _fn_name := _partition_def_constraint_fk_on_delete(_pt);

    if _fn_name is null then
        return null;
    end if;

    _tr_name := _partition_choose_constraint_name('tr', _pt.tbl, _fn_name);

    EXECUTE format('drop trigger if exists %I on %I', _tr_name, _pt.tbl);

    _sql := format('create constraint trigger %I AFTER DELETE on %I
            for each row
            execute function %I()', _tr_name, _pt.tbl, _fn_name);
    /*
      ALTER TABLE tf_proc.tp_case
      DISABLE TRIGGER tr_fd_fk_prt_tp_case_id;
    */
    EXECUTE _sql;

    insert into partition_tr_constraints values (_tr_name, _fn_name, current_schema(), _pt.tbl)
        ON CONFLICT (tr_name) DO NOTHING;

    call _partition_write_log('create trigger on delete', _tr_name, '_partition_def_constraint_tr_on_delete', _sql);

    return _tr_name;
end;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/* Поставить триггереы на партициронванную таблицу */
CREATE OR REPLACE FUNCTION _partition_restore_triggers(_pt record)
    RETURNS pg_catalog.void AS
$body$
declare
    _r record;
BEGIN
    for _r in
        select 'CREATE TRIGGER' || ' ' ||
               t.trigger_name || ' ' ||
               t.action_timing || ' ' ||
               array_to_string(array_agg(t.event_manipulation order by t.action_order), ' OR ') ||
               concat(' ON ', current_schema(), '.', t.event_object_table, ' ') ||
               concat(' FOR EACH ', t.action_orientation) || ' ' ||
               coalesce(' WHEN ' || t.action_condition, '') || ' ' ||
               t.action_statement as sql,
               current_schema()      sh,
               t.event_object_table  tbl,
               t.trigger_name     as name
        from information_schema.triggers t
        where t.event_object_table = _pt.tbl
          and t.event_object_schema = _pt.sh
        group by t.event_object_schema, t.event_object_table,
                 t.trigger_name, t.action_orientation,
                 t.action_timing, t.action_condition, t.action_statement
        LOOP
            execute format('DROP TRIGGER IF EXISTS %I ON %I.%I', _r.name, _r.sh, _r.tbl);

            execute _r.sql;

            call _partition_write_log('recreate trigger', _r.name, '_partition_recreate_original_triggers', _r.sql);

            call _partition_write_log('undo', _pt.tbl, '_partition_recreate_original_triggers',
                                      replace(_r.sql, concat(current_schema(), '.'), _pt.sh || '.'));

        END LOOP;

END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/* Реализация внешних ключей на партицию
 * 1.1 -Удаляем ограничения внешних ключей
 * 1.2 -Ставим триггеры на insert/update/delete
 */
CREATE OR REPLACE FUNCTION _partition_rebuild_constraints(_pt record)
    RETURNS pg_catalog.void AS
$body$
declare
    _r       record;
    _tr_name text;
    _fn_name text;
BEGIN
    for _r in SELECT concat('ALTER TABLE ', constraint_sh, '.', constraint_tbl, ' ',
                            'DROP CONSTRAINT IF EXISTS ', "constraint_name") as "sql",
                     concat('ALTER TABLE ', constraint_sh, '.', constraint_tbl, ' ',
                            'ADD CONSTRAINT ', "constraint_name", ' ', pg_get_constraintdef(constraint_oid)) as "undo",
                     case
                         when _pt.tbl = constraint_tbl
                             then current_schema()
                         else constraint_sh
                         end                                                 as sh,
                     constraint_tbl                                          as tbl,
                     constraint_column                                       as column_from,
                     part_column                                             as column_to,
                     current_schema()                                        as current_sh
              FROM _partition_get_constraints(_pt.sh, _pt.tbl)
              WHERE constraint_type = 'FOREIGN KEY'
        LOOP
            --TODO drop constraint
            perform _partition_set_job('1100 drop constraint', _r.sql);

            call _partition_write_log('undo', _r.tbl, '_partition_create_constraint_triggers', _r.undo);

            _fn_name := _partition_def_constraint_fk(_r.sh, _r.tbl, _r.column_from, _r.current_sh, _pt.tbl, _r.column_to);

            _tr_name := _partition_def_constraint_tr(_r.sh, _r.tbl, _fn_name);

        END LOOP;

END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


/*
 * Копируем данные
 */
CREATE OR REPLACE FUNCTION _partition_copy_data(_pt record)
    RETURNS pg_catalog.void AS
$body$
DECLARE
    _sql text;
BEGIN
    _sql := format('insert into %I select * from %I.%I on conflict do nothing', _pt.tbl, _pt.sh, _pt.tbl);

    call _partition_write_log('start copy data', _pt.tbl, '_partition_copy_data_in_current_sh', _sql);

    SET CONSTRAINTS ALL DEFERRED;

    execute format('alter table %I DISABLE TRIGGER ALL ', _pt.tbl);
    execute _sql;
    execute format('alter table %I ENABLE TRIGGER ALL ', _pt.tbl);

    perform _partition_table_update(_pt, 2);

    call _partition_write_log('finish copy data', _pt.tbl, '_partition_copy_data_in_current_sh', null);
    call _partition_write_log('undo', _pt.tbl, '_partition_copy_data_in_current_sh',
                              format('insert into %I.%I select * from %I.%I', _pt.sh, _pt.tbl, current_schema(), _pt.tbl));

END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


CREATE OR REPLACE PROCEDURE _partition_copy_data_fd(_to_sh text, _to_tbl text, _from_sh text, _from_tbl text) AS
$body$
BEGIN
    set constraints all deferred;
    call _partition_write_log('start copy fd', '', '_partition_copy_data', null);
    execute format('alter table %I.%I disable trigger all ', _to_sh, _to_tbl);
    execute format('insert into %I.%I select * from %I.%I on conflict do nothing', _to_sh, _to_tbl, _from_sh, _from_tbl);
    execute format('alter table %I.%I enable trigger all ', _to_sh, _to_tbl);
    call _partition_write_log('finish copy fd', '', 'partition_copy_data', null);
END;
$body$
    LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION _partition_create_event_trigger()
    RETURNS pg_catalog.void AS
$body$
BEGIN
    CREATE OR REPLACE FUNCTION f_ev_prt_alter_table()
        RETURNS event_trigger
        LANGUAGE plpgsql AS
    $event$
    DECLARE
        _pt                 record;
        _query              text;
        _old_obj_name       text;
        _new_obj_name       text;
        _ddl_renamed_column text;
        _ddl_renamed_table  text;
        _ddl_set_schema     text;

    BEGIN
        if tg_tag != 'ALTER TABLE' then
            return;
        end if;

        _query := trim(lower(current_query()));

        _ddl_renamed_column := coalesce(trim(substring(_query from '%rename%column%#" ([0-9a-z_])+ #"%to%' for '#')),
                                        trim(substring(_query from '%rename #"%#" to%' for '#')));
        _ddl_renamed_column := case when trim(_ddl_renamed_column) like '% %' then null else _ddl_renamed_column end;
        _ddl_renamed_table := trim(substring(_query from 'alter%table %.#"([0-9a-z_])+ #"%rename%to%' for '#'));
        _ddl_set_schema := trim(substring(_query from '%set%schema%#" ([0-9a-z_])+#"(;)?%' for '#'));

        if _ddl_renamed_column is not null then
            FOR _pt IN with cmd as (
                select c.schema_name,
                       c.objid::regclass::text table_name,
                       substring(lower(c.object_identity) from
                                 concat(c.schema_name, '.', c.objid::regclass::text, '.#"%#"') for
                                 '#') as  ddl_new_column
                from pg_event_trigger_ddl_commands() c
            )
                       select fn.fn_name,
                              fn.fn_args,
                              fn.from_sh       as _from_sh,
                              fn.from_tbl      as _from_tbl,
                              c.ddl_new_column as _from_field,
                              fn.prt_sh        as _to_sh,
                              fn.prt_tbl       as _to_tbl,
                              fn.prt_field     as _to_field
                       from cmd c
                                join partitions.partition_fn_constraints fn
                                     on fn.from_tbl = c.table_name
                                         and fn.from_sh = c.schema_name
                                         and fn.from_field = _ddl_renamed_column
                       where c.ddl_new_column != _ddl_renamed_column
                       union
                       select fn.fn_name,
                              fn.fn_args,
                              fn.from_sh       as _from_sh,
                              fn.from_tbl      as _from_tbl,
                              fn.from_field    as _from_field,
                              fn.prt_sh        as _to_sh,
                              fn.prt_tbl       as _to_tbl,
                              c.ddl_new_column as _to_field
                       from cmd c
                                join partitions.partition_fn_constraints fn
                                     on fn.prt_tbl = c.table_name
                                         and fn.prt_sh = c.schema_name
                                         and fn.prt_field = _ddl_renamed_column
                       where c.ddl_new_column != _ddl_renamed_column
                LOOP
                    raise log 'f_ev_prt_alter_table>>>running event trigger on rename column %', _ddl_renamed_column;
                    execute 'delete from partition_tr_constraints where fn_name = $1 returning tr_name'
                        into _old_obj_name
                        using _pt.fn_name;
                    execute 'delete from partition_fn_constraints where fn_name = $1' using _pt.fn_name;

                    if _old_obj_name is not null then
                        execute format('drop trigger if exists %I on %I.%I', _old_obj_name, _pt._from_sh, _pt._from_tbl);
                    end if;

                    execute format('drop function if exists %I'|| _pt.fn_args ||' cascade', _pt.fn_name);

                    _new_obj_name := _partition_def_constraint_fk(
                            _pt._from_sh,
                            _pt._from_tbl,
                            _pt._from_field,
                            _pt._to_sh,
                            _pt._to_tbl,
                            _pt._to_field);

                    perform _partition_def_constraint_tr(_pt._from_sh, _pt._from_tbl, _new_obj_name);

                END LOOP;
        elsif _ddl_renamed_table is not null then
            FOR _pt IN with cmd as (
                select c.schema_name,
                       c.objid::regclass::text table_name
                from pg_event_trigger_ddl_commands() c
            )
                       select fn.fn_name,
                              fn.fn_args,
                              fn.from_sh                                                                        as _from_sh,
                              case when fn.from_tbl = _ddl_renamed_table then c.table_name else fn.from_tbl end as _from_tbl,
                              fn.from_field                                                                     as _from_field,
                              fn.prt_sh                                                                         as _to_sh,
                              c.table_name                                                                      as _to_tbl,
                              fn.prt_field                                                                      as _to_field
                       from cmd c
                                join partitions.partition_fn_constraints fn
                                     on fn.prt_tbl = _ddl_renamed_table
                                         and fn.prt_sh = c.schema_name
                       where c.table_name != _ddl_renamed_table and fn.prt_field is not null
                       union
                       select fn.fn_name,
                              fn.fn_args,
                              fn.from_sh                                                                        as _from_sh,
                              case when fn.from_tbl = _ddl_renamed_table then c.table_name else fn.from_tbl end as _from_tbl,
                              fn.from_field                                                                     as _from_field,
                              fn.prt_sh                                                                         as _to_sh,
                              c.table_name                                                                      as _to_tbl,
                              fn.prt_field                                                                      as _to_field
                       from cmd c
                                join partitions.partition_fn_constraints fn
                                     on fn.from_tbl = _ddl_renamed_table
                                         and fn.from_sh = c.schema_name
                       where c.table_name != _ddl_renamed_table and fn.prt_field is not null
                LOOP
                    raise log 'f_ev_prt_alter_table>>>running event trigger on rename table %', _ddl_renamed_table;
                    execute 'delete from partition_tr_constraints where fn_name = $1 returning tr_name'
                        into _old_obj_name
                        using _pt.fn_name;
                    execute 'delete from partition_fn_constraints where fn_name = $1' using _pt.fn_name;

                    if _old_obj_name is not null then
                        execute format('drop trigger if exists %I on %I.%I', _old_obj_name, _pt._from_sh, _pt._from_tbl);
                    end if;

                    execute format('drop function if exists %I'|| _pt.fn_args ||' cascade', _pt.fn_name);

                    _new_obj_name := _partition_def_constraint_fk(
                            _pt._from_sh,
                            _pt._from_tbl,
                            _pt._from_field,
                            _pt._to_sh,
                            _pt._to_tbl,
                            _pt._to_field);

                    perform _partition_def_constraint_tr(_pt._from_sh, _pt._from_tbl, _new_obj_name);
                END LOOP;
        elsif _ddl_set_schema is not null then
            FOR _pt IN with cmd as (
                select c.schema_name as new_sh,
                       replace(c.object_identity, c.schema_name || '.', '') as table_name,
                       coalesce(trim(substring(_query from 'alter%table%#" ([0-9a-z_])+#"(.)?%set%schema%' for '#')), current_schema()) as old_sh
                from pg_event_trigger_ddl_commands() c
            )
                       select fn.fn_name,
                              fn.fn_args,
                              case when fn.from_sh = c.old_sh then c.new_sh else fn.from_sh end as _from_sh,
                              fn.from_tbl                                                       as _from_tbl,
                              fn.from_field                                                     as _from_field,
                              c.new_sh                                                          as _to_sh,
                              fn.prt_tbl                                                        as _to_tbl,
                              fn.prt_field                                                      as _to_field
                       from cmd c
                                join partitions.partition_fn_constraints fn
                                     on fn.prt_tbl = c.table_name
                                         and fn.prt_sh = c.old_sh
                       where c.old_sh != c.new_sh and fn.prt_field is not null
                       union
                       select fn.fn_name,
                              fn.fn_args,
                              case when fn.from_sh = c.old_sh then c.new_sh else fn.from_sh end as _from_sh,
                              fn.from_tbl                                                       as _from_tbl,
                              fn.from_field                                                     as _from_field,
                              c.new_sh                                                          as _to_sh,
                              fn.prt_tbl                                                        as _to_tbl,
                              fn.prt_field                                                      as _to_field
                       from cmd c
                                join partitions.partition_fn_constraints fn
                                     on fn.from_sh = c.table_name
                                         and fn.from_tbl = c.old_sh
                       where c.old_sh != c.new_sh and fn.prt_field is not null
                LOOP
                    raise log 'f_ev_prt_alter_table>>>delete from partition_tr_constraints where fn_name = % returning tr_name',_pt.fn_name;

                    execute 'delete from partition_tr_constraints where fn_name = $1 returning tr_name'
                        into _old_obj_name
                        using _pt.fn_name;
                    execute 'delete from partition_fn_constraints where fn_name = $1' using _pt.fn_name;

                    if _old_obj_name is not null then
                        execute format('drop trigger if exists %I on %I.%I', _old_obj_name, _pt._from_sh, _pt._from_tbl);
                    end if;

                    execute format('drop function if exists %I'|| _pt.fn_args ||' cascade', _pt.fn_name);

                    _new_obj_name := _partition_def_constraint_fk(
                            _pt._from_sh,
                            _pt._from_tbl,
                            _pt._from_field,
                            _pt._to_sh,
                            _pt._to_tbl,
                            _pt._to_field);

                    perform _partition_def_constraint_tr(_pt._from_sh, _pt._from_tbl, _new_obj_name);
                END LOOP;
        end if;

    END;
    $event$ SET search_path TO partitions;

    call _partition_write_log('create event function', 'f_ev_prt_alter_table', '_partition_create_event_trigger',null);

    DROP EVENT TRIGGER IF EXISTS prt_event_trigger_alter_table;
    CREATE EVENT TRIGGER prt_event_trigger_alter_table
        ON ddl_command_end
        WHEN TAG IN ('ALTER TABLE')
    EXECUTE PROCEDURE f_ev_prt_alter_table();

    call _partition_write_log('create event trigger', 'prt_event_trigger_alter_table', '_partition_create_event_trigger',
                              null);
    call _partition_write_log('undo', 'event trigger', 'prt_event_trigger_alter_table',
                              'drop event trigger if exists prt_event_trigger_alter_table cascade');

END;
$body$
    LANGUAGE plpgsql
    SET search_path TO partitions;


CREATE OR REPLACE PROCEDURE partition_copy_data(
) AS
$body$
DECLARE
    _pt     record;
    _locked bool;
BEGIN
    call _partition_write_log('start copy', '', 'partition_copy_data', null);

    FOR _pt IN select * from partition_tables where status = 1 order by serial_number
        LOOP
            select true into _locked
            from partition_tables t
            where tbl = _pt.tbl
              and status = 1
              and pg_catalog.pg_try_advisory_xact_lock(tableoid::INTEGER, serial_number)
            limit 1;

            IF NOT FOUND THEN
                continue;
            END IF;

            call _partition_write_log('copy ' || _pt.tbl, _pt.tbl, 'partition_copy_data', null);

            perform _partition_copy_data(_pt);

        END LOOP;

    call _partition_write_log('finish copy', '', 'partition_copy_data', null);

END;

$body$
    LANGUAGE 'plpgsql'
    SET search_path TO partitions;


CREATE OR REPLACE PROCEDURE partition_run(
) AS
$body$
DECLARE
    _pt     record;
    _locked bool;
    _detail text array;
    -- для версии 12 есть полная поддержка, сценарий будет другой
    _ver    numeric;
BEGIN
    call _partition_write_log('start', 'pid '|| pg_backend_pid(), 'partition_run', null);

    select setting::numeric into _ver from pg_settings where "name" = 'server_version';

    set synchronous_commit to off;

    FOR _pt IN select * from partition_tables where status is null order by serial_number
        LOOP
            select true into _locked
            from partition_tables t
            where tbl = _pt.tbl
              and status is null
              and pg_catalog.pg_try_advisory_xact_lock(tableoid::INTEGER, serial_number)
            limit 1;

            IF NOT FOUND THEN
                continue;
            END IF;

            call _partition_write_log('start loop ' || _pt.tbl, _pt.tbl, 'partition_run', null);

            perform _partition_create_parent_table(_pt);

            perform _partition_create_child_tables(_pt);

            perform _partition_copy_data(_pt);

            perform _partition_rebuild_constraints(_pt);

            perform _partition_restore_referrences(_pt);

            perform _partition_restore_triggers(_pt);

            perform _partition_def_tr_on_delete(_pt);

            perform _partition_create_index(_pt);

            perform _partition_replace_view(_pt);

            perform _partition_table_update(_pt);

            call _partition_write_log('end loop ' || _pt.tbl, _pt.tbl, 'partition_run', null);

            select array_append(_detail, _pt.tbl) into _detail;

        END LOOP;

    perform _partition_set_job('9000 create event trigger', 'select _partition_create_event_trigger()', null, null, null, null);

    select array_append(_detail, to_char(age(clock_timestamp(), transaction_timestamp()), 'age HH24:MI:SS')) into _detail;

    call _partition_write_log('finish ' || array_to_string(_detail, '; '), 'main', 'partition_run', null);

END;

$body$
    LANGUAGE 'plpgsql';


/*
SET search_path TO partitions;
SET synchronous_commit to off;

call partitions.partition_run();
call partitions.partition_run_jobs();

analyze tf_proc.tp_caseraw;
analyze tf_proc.tp_case;
analyze tf_proc.tp_casebill;

alter table tf_proc.tp_casebill set schema public;
alter table tf_proc.tp_case set schema public;
alter table tf_proc.tp_caseraw set schema public;

alter table partitions.tp_casebill set schema tf_proc;
alter table partitions.tp_case set schema tf_proc;
alter table partitions.tp_caseraw set schema tf_proc;

ALTER TABLE tf_proc.tp_caseraw ALTER COLUMN date_2 SET DEFAULT CURRENT_DATE;
ALTER TABLE tf_proc.tp_case ALTER COLUMN date_2 SET DEFAULT CURRENT_DATE;
ALTER TABLE tf_proc.tp_casebill ALTER COLUMN date_2 SET DEFAULT CURRENT_DATE;

*/

