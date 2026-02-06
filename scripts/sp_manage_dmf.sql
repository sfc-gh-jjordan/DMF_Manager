
CREATE OR REPLACE PROCEDURE SP_MANAGE_DMF(
    CONFIG_TABLE VARCHAR,
    ACTION VARCHAR,
    FILTER_CONDITION VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    sql_stmt VARCHAR;
    config_cursor CURSOR FOR SELECT * FROM IDENTIFIER(:CONFIG_TABLE) WHERE IS_ACTIVE = TRUE;
    rec VARIANT;
    results ARRAY := ARRAY_CONSTRUCT();
    row_result OBJECT;
    dmf_sql VARCHAR;
    schedule_sql VARCHAR;
    col_list VARCHAR;
    full_table_name VARCHAR;
    current_db VARCHAR;
    current_schema VARCHAR;
    current_table VARCHAR;
    current_columns VARCHAR;
    current_dmf VARCHAR;
    current_schedule VARCHAR;
    error_msg VARCHAR;
    success_count NUMBER := 0;
    error_count NUMBER := 0;
    dmf_exists NUMBER := 0;
    check_sql VARCHAR;
BEGIN
    IF (UPPER(:ACTION) NOT IN ('ADD', 'VALIDATE')) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'message', 'Invalid action. Must be ADD or VALIDATE.'
        );
    END IF;

    IF (:FILTER_CONDITION IS NOT NULL AND :FILTER_CONDITION != '') THEN
        sql_stmt := 'SELECT * FROM ' || :CONFIG_TABLE || ' WHERE IS_ACTIVE = TRUE AND ' || :FILTER_CONDITION;
    ELSE
        sql_stmt := 'SELECT * FROM ' || :CONFIG_TABLE || ' WHERE IS_ACTIVE = TRUE';
    END IF;

    LET rs RESULTSET := (EXECUTE IMMEDIATE :sql_stmt);
    LET cur CURSOR FOR rs;

    FOR rec IN cur DO
        current_db := rec."DATABASE_NAME"::VARCHAR;
        current_schema := rec."SCHEMA_NAME"::VARCHAR;
        current_table := rec."TABLE_NAME"::VARCHAR;
        current_columns := rec."COLUMN_NAMES"::VARCHAR;
        current_dmf := rec."DMF_NAME"::VARCHAR;
        current_schedule := rec."DMF_SCHEDULE"::VARCHAR;
        
        full_table_name := current_db || '.' || current_schema || '.' || current_table;
        
        col_list := NVL(current_columns, '');

        BEGIN
            IF (current_db IS NULL OR current_schema IS NULL OR current_table IS NULL OR current_dmf IS NULL) THEN
                row_result := OBJECT_CONSTRUCT(
                    'table', full_table_name,
                    'dmf', current_dmf,
                    'columns', col_list,
                    'action', :ACTION,
                    'status', 'ERROR',
                    'error', 'Required field is NULL: DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, or DMF_NAME'
                );
                error_count := error_count + 1;
                results := ARRAY_APPEND(results, row_result);
                CONTINUE;
            END IF;

            IF (UPPER(:ACTION) = 'ADD') THEN
                -- Check if DMF already exists on this table
                LET dmf_short VARCHAR;
                dmf_short := UPPER(SPLIT_PART(current_dmf, '.', -1));
                
                IF (current_columns IS NULL OR current_columns = '') THEN
                    -- Table-level DMF: check for empty REF_ARGUMENTS
                    check_sql := 'SELECT COUNT(*) FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(REF_ENTITY_NAME => ''' || full_table_name || ''', REF_ENTITY_DOMAIN => ''TABLE'')) WHERE UPPER(METRIC_NAME) = ''' || dmf_short || ''' AND REF_ARGUMENTS = ''[]''';
                ELSE
                    -- Column-level DMF: extract column names from REF_ARGUMENTS JSON
                    check_sql := 'SELECT COUNT(*) FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(REF_ENTITY_NAME => ''' || full_table_name || ''', REF_ENTITY_DOMAIN => ''TABLE'')) WHERE UPPER(METRIC_NAME) = ''' || dmf_short || ''' AND REF_ARGUMENTS LIKE ''%' || UPPER(SPLIT_PART(current_columns, ',', 1)) || '%''';
                END IF;
                
                LET check_rs RESULTSET := (EXECUTE IMMEDIATE :check_sql);
                LET check_cur CURSOR FOR check_rs;
                FOR check_rec IN check_cur DO
                    dmf_exists := check_rec."COUNT(*)"::NUMBER;
                END FOR;
                
                IF (dmf_exists > 0) THEN
                    row_result := OBJECT_CONSTRUCT(
                        'table', full_table_name,
                        'dmf', current_dmf,
                        'columns', col_list,
                        'action', 'ADD',
                        'status', 'SKIPPED',
                        'message', 'Data metric function already exists on this table'
                    );
                    results := ARRAY_APPEND(results, row_result);
                    dmf_exists := 0;
                    CONTINUE;
                END IF;
                
                -- Handle table-level DMFs (no columns) vs column-level DMFs
                IF (current_columns IS NULL OR current_columns = '') THEN
                    -- Table-level DMF: ROW_COUNT, FRESHNESS, etc. - use empty parentheses
                    dmf_sql := 'ALTER TABLE ' || full_table_name || ' ADD DATA METRIC FUNCTION ' || current_dmf || ' ON ()';
                ELSE
                    -- Column-level DMF: NULL_COUNT, DUPLICATE_COUNT, etc.
                    dmf_sql := 'ALTER TABLE ' || full_table_name || ' ADD DATA METRIC FUNCTION ' || current_dmf || ' ON (' || current_columns || ')';
                END IF;
                EXECUTE IMMEDIATE :dmf_sql;
                
                IF (current_schedule IS NOT NULL AND current_schedule != '') THEN
                    schedule_sql := 'ALTER TABLE ' || full_table_name || ' SET DATA_METRIC_SCHEDULE = ''' || current_schedule || '''';
                    EXECUTE IMMEDIATE :schedule_sql;
                END IF;
                
                row_result := OBJECT_CONSTRUCT(
                    'table', full_table_name,
                    'dmf', current_dmf,
                    'columns', col_list,
                    'schedule', current_schedule,
                    'action', 'ADD',
                    'status', 'SUCCESS',
                    'sql', dmf_sql
                );
                success_count := success_count + 1;
                
            ELSEIF (UPPER(:ACTION) = 'VALIDATE') THEN
                -- Execute the DMF to validate it works
                LET dmf_result VARIANT;
                LET dmf_short_name VARCHAR;
                dmf_short_name := UPPER(SPLIT_PART(current_dmf, '.', -1));
                
                IF (current_columns IS NULL OR current_columns = '') THEN
                    -- Table-level DMFs: ROW_COUNT, FRESHNESS cannot be called directly
                    -- Use equivalent SQL to validate
                    IF (dmf_short_name = 'ROW_COUNT') THEN
                        dmf_sql := 'SELECT COUNT(*) FROM ' || full_table_name;
                    ELSEIF (dmf_short_name = 'FRESHNESS') THEN
                        -- FRESHNESS requires the DMF to be applied; validate table is accessible
                        dmf_sql := 'SELECT COUNT(*) FROM ' || full_table_name || ' LIMIT 1';
                    ELSE
                        -- For other table-level DMFs, try to call directly
                        dmf_sql := 'SELECT ' || current_dmf || '()';
                    END IF;
                ELSE
                    -- Column-level DMF: NULL_COUNT, DUPLICATE_COUNT, etc.
                    dmf_sql := 'SELECT ' || current_dmf || '(SELECT ' || current_columns || ' FROM ' || full_table_name || ')';
                END IF;
                
                LET validate_rs RESULTSET := (EXECUTE IMMEDIATE :dmf_sql);
                LET validate_cur CURSOR FOR validate_rs;
                FOR validate_rec IN validate_cur DO
                    dmf_result := validate_rec[0];
                END FOR;
                
                row_result := OBJECT_CONSTRUCT(
                    'table', full_table_name,
                    'dmf', current_dmf,
                    'columns', col_list,
                    'action', 'VALIDATE',
                    'status', 'SUCCESS',
                    'result', dmf_result,
                    'sql_executed', dmf_sql
                );
                success_count := success_count + 1;
            END IF;
            
        EXCEPTION
            WHEN OTHER THEN
                error_msg := SQLERRM;
                row_result := OBJECT_CONSTRUCT(
                    'table', full_table_name,
                    'dmf', current_dmf,
                    'columns', col_list,
                    'action', :ACTION,
                    'status', 'ERROR',
                    'error', error_msg,
                    'sql', dmf_sql
                );
                error_count := error_count + 1;
        END;
        
        results := ARRAY_APPEND(results, row_result);
    END FOR;

    RETURN OBJECT_CONSTRUCT(
        'action', :ACTION,
        'summary', OBJECT_CONSTRUCT(
            'total', success_count + error_count,
            'success', success_count,
            'errors', error_count
        ),
        'results', results
    );
END;
$$;
