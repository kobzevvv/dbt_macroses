-- readme 
    -- aggregated materialization by parts with realtime view output
    -- https://docs.google.com/presentation/d/1Oa-DMHodcdWv3jQJ3tX40E6v0FU96edkgXOUBen5tfw/edit#slide=id.g1217c22e021_0_196

    --  input data info
        --   materialization_start, DateTime i.e ‘2022-02-02’

        --    input_model i.e session_events or log_events
        --    input_timestamp_column i.e event time

        --    output_id_column uniq id for output model
        --    output_session_end_column name, i.e process end

        --    time_unit_name: usually day, sometimes hour or minute
        --    interval_fluctuation,  i.e max user session duration
        --    materialized_window one materialized inserted time interval
        --    life_section [true, false] whether or not to include a live section
        --    unfinished_changes_filter [true, false] whether or not to filter life_section data from unfinished_section

---------ADAPTERS--------------
---- Default

    {% macro default__insert_as (sql, target_relation, input_models_set, input_columns_set, output_column, max_having_right, left_where, right_where, left_having, right_having, debug_mode) %}
        
        {% set inserting_sql = 
                adapter.dispatch("get_sql_for_insert", macro_namespace="dbt_improvado_utils")( sql, input_models_set, input_columns_set, output_column, max_having_right, left_where, right_where, left_having, right_having, debug_mode) %}

        INSERT INTO {{target_relation}} {{inserting_sql}}

        {% set target_relation_interval_insert %}
           INSERT INTO {{target_relation}} {{inserting_sql}}
        {% endset %}

        {{ return (target_relation_interval_insert)}}
    {% endmacro %}

    {% macro default__get_sql_for_insert ( sql, input_models_set, input_columns_set, output_column, max_having_right, left_where, right_where, left_having, right_having, debug_mode ) %}
        
        {% set model_sql = namespace(query=sql) %}

        {% for index in range(input_models_set|length) %}
            {% set input_model = input_models_set[index] %}
            {% set input_column = input_columns_set[index] %}
            {% set input_relation = schema ~ '.' ~ input_model %}
            {% set sql_replacement = "(select * from " ~ input_relation ~ " where " ~ input_column ~ " between '" ~ left_where ~ "' and '" ~ right_where ~ "')" %}
            
           
            {% set model_sql.query = (model_sql.query | replace(input_relation, sql_replacement)) %}

        {% endfor %}


        {% set target_relation_interval_insert %}
            WITH            _sql as ({{ sql }})
            SELECT          *
            FROM            _sql
            where          toDateTime({{ output_column }}) > '{{ left_having }}'
                            and toDateTime({{ output_column }}) <= '{{ right_having }}'                                             
                            and toDateTime({{ output_column }}) <= '{{ max_having_right }}'
            {% if debug_mode %} LIMIT 1 {% endif %}
        {% endset %}

        {{ return (target_relation_interval_insert)}}

    {% endmacro %}

    {% macro default__get_max_date_time_in_table ( relation, column ) %}
        
        {% set get_max %}
            SELECT      toDateTime( max( {{ column }}))
            FROM        {{ relation }}
        {%- endset -%}

        {%- if execute -%}
            {{ return (run_query (get_max)[0][0])}}
        {% endif %}

    {% endmacro %}

    {% macro default__union_all_relation (sections_set, output_id_column, unfinished_changes_filter ) %}

        {% set history_section      = ( sections_set.get('history') | as_bool ) %}
        {% set unfinished_section   = ( sections_set.get('unfinished') | as_bool ) %}
        {% set live_section         = ( sections_set.get('live') | as_bool ) %}
        
        {% set output_sql %}
            {% if history_section %}
                SELECT      *
                FROM        {{sections_set.get('history')}}
            {% endif %}
            {% if unfinished_section %}
                {% if history_section %} UNION ALL {% endif %}
                SELECT      *
                FROM        {{sections_set.get('unfinished')}}
                {% if live_section and unfinished_changes_filter %}
                    WHERE   {{output_id_column}} not in (
                                    SELECT   {{output_id_column}}
                                    FROM    {{sections_set.get('live')}}  )
                {% endif %}
            {% endif %}
            {% if live_section %}
                {% if history_section or unfinished_section %} UNION ALL {% endif %}
                SELECT      *
                FROM        {{sections_set.get('live')}}
            {% endif %}
        {% endset %}

        {{ return (output_sql) }}

    {% endmacro %}
----
---- Bigquery

    {% macro bigquery__insert_as (sql, target_relation, input_models_set, input_columns_set, output_column, max_having_right, left_where, right_where, left_having, right_having, debug_mode) %}
        
        {% set inserting_sql = 
                adapter.dispatch("get_sql_for_insert", macro_namespace="dbt_improvado_utils")( sql, input_models_set, input_column, output_column, max_having_right, left_where, right_where, left_having, right_having, debug_mode) %}

        INSERT INTO {{target_relation}} {{inserting_sql}}

        {% set target_relation_interval_insert %}
           INSERT INTO {{target_relation}} {{inserting_sql}}
        {% endset %}

        {{ return (target_relation_interval_insert)}}
    {% endmacro %}

    {% macro bigquery__get_sql_for_insert ( sql, input_models_set, input_columns_set, output_column, max_having_right, left_where, right_where, left_having, right_having, debug_mode ) %}
        
        {% set model_sql = namespace(query=sql) %}

        {% for index in range(input_models_set|length) %}
            {% set input_model = input_models_set[index] %}
            {% set input_column = input_columns_set[index] %}
            {% set input_relation = schema ~ '.' ~ input_model %}
            {% set sql_replacement = "(select * from " ~ input_relation ~ " where " ~ input_column ~ " between '" ~ left_where ~ "' and '" ~ right_where ~ "')" %}
            
           
            {% set model_sql.query = (model_sql.query | replace(input_relation, sql_replacement)) %}

        {% endfor %}

        {% set target_relation_interval_insert %}
            WITH            _sql as ({{ sql }})
            SELECT          *
            FROM            _sql
            where          DATETIME({{ output_column }}) > '{{ left_having }}'
                            and DATETIME({{ output_column }}) <= '{{ right_having }}'                                             
                            and DATETIME({{ output_column }}) <= '{{ max_having_right }}'
            {% if debug_mode %} LIMIT 1 {% endif %}
        {% endset %}

        {{ return (target_relation_interval_insert)}}

    {% endmacro %}

    {% macro bigquery__get_max_date_time_in_table ( relation, column ) %}
        
        {% set get_max %}
            SELECT      DATETIME( ifnull(max( {{ column }}), '1970-01-01 00:00:00' ))
            FROM        {{ relation }}
        {%- endset -%}

        {%- if execute -%}
            {{ return (run_query (get_max)[0][0])}}
        {% endif %}

    {% endmacro %}

    {% macro bigquery__union_all_relation (sections_set, output_id_column, unfinished_changes_filter ) %}

        {% set history_section      = ( sections_set.get('history') | as_bool ) %}
        {% set unfinished_section   = ( sections_set.get('unfinished') | as_bool ) %}
        {% set live_section         = ( sections_set.get('live') | as_bool ) %}
        
        {% set output_sql %}
            {% if history_section %}
                SELECT      *
                FROM        {{sections_set.get('history')}}
            {% endif %}
            {% if unfinished_section %}
                {% if history_section %} UNION ALL {% endif %}
                SELECT      *
                FROM        {{sections_set.get('unfinished')}}
                {% if live_section and unfinished_changes_filter %}
                    WHERE   {{output_id_column}} not in (
                                    SELECT   {{output_id_column}}
                                    FROM    {{sections_set.get('live')}}  )
                {% endif %}
            {% endif %}
            {% if live_section %}
                {% if history_section or unfinished_section %} UNION ALL {% endif %}
                SELECT      *
                FROM        {{sections_set.get('live')}}
            {% endif %}
        {% endset %}

        {{ return (output_sql) }}

    {% endmacro %}

----
---- Clickhouse

    {% macro clickhouse__insert_as (sql, target_relation, input_models_set, input_columns_set, output_column, max_having_right, left_where, right_where, left_having, right_having, debug_mode) %}
        
        {% set inserting_sql = 
                adapter.dispatch("get_sql_for_insert", macro_namespace="dbt_improvado_utils")( sql, input_models_set, input_columns_set, output_column, max_having_right, left_where, right_where, left_having, right_having, debug_mode) %}

        INSERT INTO {{target_relation}} {{inserting_sql}}

        {% set target_relation_interval_insert %}
           INSERT INTO {{target_relation}} {{inserting_sql}}
        {% endset %}

        {{ return (target_relation_interval_insert)}}
    {% endmacro %}

    {% macro clickhouse__get_sql_for_insert ( sql, input_models_set, input_columns_set, output_column, max_having_right, left_where, right_where, left_having, right_having, debug_mode ) %}
        
        {% set model_sql = namespace(query=sql) %}

        {% for index in range(input_models_set|length) %}
            {% set input_model = input_models_set[index] %}
            {% set input_column = input_columns_set[index] %}
            {% set input_relation = schema ~ '.' ~ input_model %}
            {% set sql_replacement = "(select * from " ~ input_relation ~ " where " ~ input_column ~ " between '" ~ left_where ~ "' and '" ~ right_where ~ "')" %}
            
           
            {% set model_sql.query = (model_sql.query | replace(input_relation, sql_replacement)) %}

        {% endfor %}

        {% set target_relation_interval_insert %}
            WITH            _sql as ({{ model_sql.query }})
            SELECT          *
            FROM            _sql
            where          toDateTime({{ output_column }}) > '{{ left_having }}'
                            and toDateTime({{ output_column }}) <= '{{ right_having }}'                                             
                            and toDateTime({{ output_column }}) <= '{{ max_having_right }}'
            {% if debug_mode %} LIMIT 1 {% endif %}
        {% endset %}

        {{ return (target_relation_interval_insert)}}

    {% endmacro %}

    {% macro clickhouse__get_max_date_time_in_table ( relation, column ) %}
        
        {% set get_max %}
            SELECT      toDateTime( max( {{ column }}))
            FROM        {{ relation }}
        {%- endset -%}

        {%- if execute -%}
            {{ return (run_query (get_max)[0][0])}}
        {% endif %}

    {% endmacro %}

    {% macro clickhouse__union_all_relation (sections_set, output_id_column, order_by, unfinished_changes_filter ) %}

        {% set history_section      = ( sections_set.get('history') | as_bool ) %}
        {% set unfinished_section   = ( sections_set.get('unfinished') | as_bool ) %}
        {% set live_section         = ( sections_set.get('live') | as_bool ) %}

        {% set output_sql %}
            {% if history_section %}
                SELECT      *
                FROM        {{sections_set.get('history')}}
            {% endif %}
            {% if unfinished_section or live_section  %}
                {% if history_section %} UNION ALL {% endif %}
                WITH unfinished_and_live_sections AS (
                    {% if live_section %}
                    SELECT      *
                    FROM        {{sections_set.get('live')}}
                    {% endif %}
                    {% if unfinished_section and live_section %} UNION ALL {% endif %}
                    {% if unfinished_section %}
                    SELECT      *
                    FROM        {{sections_set.get('unfinished')}}
                    {% endif %}
                )
                SELECT *
                FROM unfinished_and_live_sections
                {% if unfinished_changes_filter %}
                    ORDER BY {{order_by}} DESC
                    LIMIT 1 BY {{output_id_column}}
                {% endif %}
            {% endif %}
        {% endset %}

        {{ return (output_sql) }}

    {% endmacro %}

----

-----------------------

{% materialization _and_live   %}

    -- input data fields
        {%  set input_models_str                    = config.get('input_models') -%}
        {%  set input_timestamp_columns_str         = config.get('input_timestamp_columns') -%}
        
        {%  set input_models_set                    = input_models_str.split(', ') %}
        {%  set input_columns_set                   = input_timestamp_columns_str.split(', ') %}

        {%  set output_id_column                    = config.get('output_id_column') -%}
        {%  set output_session_end_column           = config.get('output_session_end_column') -%} -- TODO: remove word session
        
        {%  set time_unit_name                      = config.get('time_unit_name') -%}
        {%  set start_time_settings                 = config.get('start_time') -%}
        {%  set dev_days_offset                     = config.get('dev_days_offset', default = 0) -%}
        {%  set interval_fluctuation                = config.get('interval_fluctuation') -%}
        {%  set materialized_window                 = config.get('materialized_window') -%}
        {%  set partition_by                        = config.get('partition_by') %}
        {%  set engine                              = config.get('engine') %}
        {%  set order_by                            = config.get('order_by') %}

        {%  set life_section                        = config.get('life_section', default = True) %}
        {%  set unfinished_section                  = config.get('unfinished_section', default = True) %}
        {%  set unfinished_changes_filter           = config.get('unfinished_changes_filter', default = True) %}

        {%  set debug_mode                          = config.get('debug_mode', default = False) %}
        {%  set silence_mode                        = config.get('silence_mode', default = False) %}

        {%  set production_schema                   = config.get('production_schema',
            default = var('main_production_schema','internal_analytics')) %}

        {%  set fixed_now                   = modules.datetime.datetime.now().replace( microsecond=0 ) %}

        {#  {%- set if_grouped_by_time          = config.get('min_group_by_time_interval'  ) -%} #} -- TODO: for models when we use grouping by date it may be needed

        {% set sections_arr = [] %}

        {% set sections_set = { 'history'   : none,
                                'unfinished': none,
                                'live'      : none} %}

        {%- if target.schema == production_schema -%}
            {%- do log("Starting build to production schema: "~ target.schema, not silence_mode) -%}
        {%- else -%}    
            {%- set start_time_settings = (modules.datetime.datetime.today().date()-
                modules.datetime.timedelta(days = dev_days_offset)).isoformat() -%}
            {%- do log("Starting build to dev schema: "~ target.schema ~ 
                "\nsetting short start time due to dev target schema: " ~ start_time_settings ~
                "\noriginal value: " ~ config.get('start_time'), not silence_mode) -%}
        {%- endif -%}

        {{ log( "input_models" ~ input_models_set, not silence_mode) }}

        {{ log( "input_columns" ~ input_columns_set, not silence_mode) }}
        
        {{ log( "Fixed now time: " ~ fixed_now, not silence_mode) }} 

        -- column consistents (check comparison columns)
        {% set is_column_changed = 
                dbt_improvado_utils.is_table_change (   database = none, 
                                    schema = model.schema, 
                                    identifier_check = this.identifier ~ '_history', 
                                    type = 'table', 
                                    actual_sql = sql, 
                                    debug_mode = debug_mode, 
                                    silence_mode = silence_mode) %}

    --
    -- history table
        {{ log( "\n\n************** history **************\n\n", not silence_mode) }} 
        -- get_or_create history relation
            {% set target_history_relation_exists, target_history_relation = 
                    dbt_improvado_utils.get_or_create_or_update_relation (  database = none, 
                                                        schema = model.schema, 
                                                        identifier = this.identifier ~ '_history', 
                                                        type = 'table', 
                                                        update = is_column_changed, 
                                                        temporary = False, 
                                                        sql = dbt_improvado_utils.select_limit_0(sql), 
                                                        debug_mode = debug_mode, 
                                                        silence_mode = silence_mode) %}
            {% if not target_history_relation.is_table %}
                {% do exceptions.relation_wrong_type ( target_history_relation, 'table' ) %}
            {% endif %}
        --
        -- append history relation to section set
            {%- do sections_arr.append(target_history_relation) -%}
            {%- do sections_set.update({'history': target_history_relation}) -%} 
        --
        ---- previos run last_history_timestamp calculation
            {% set start_time_settings_ms = 
                    modules.datetime.datetime.strptime(start_time_settings, '%Y-%m-%d') %}
            
            -- if target_history_relation existed initially
            {% if target_history_relation_exists %}
                {% set last_history_timestamp =
                        adapter.dispatch("get_max_date_time_in_table", macro_namespace="dbt_improvado_utils")( target_history_relation, output_session_end_column) %}

                {% set start_time_ms = [last_history_timestamp, start_time_settings_ms] | max %}

                {{ log( "Target [history] relation existed initially, get max time column from relation... | " ~ 
                        "max_time = " ~ start_time_ms, debug_mode) }}
            {% else %}
                {% set start_time_ms = start_time_settings_ms %}

                {{ log( "Target [history] relation NOT existed initially, get default time ... | " ~ 
                        "max_time = " ~ start_time_ms, debug_mode) }}
            {% endif %}

            {% set start_time = start_time_ms.replace(microsecond=0) %}
        --
        ---- interval counts calculation 

            {% set max_history_timestamp = 
                    fixed_now - dbt_improvado_utils.get_interval (value = interval_fluctuation, unit = time_unit_name) %}

            {% set interval_count_tmp = 
                    dbt_improvado_utils.dateDiff (startdate = start_time, enddate = max_history_timestamp, unit = time_unit_name)
                    - interval_fluctuation %}

            {% set parts_count = 
                    (interval_count_tmp / materialized_window + 0.5) | round | int %}

            {{ log( "Interval counts calculation... | parts_count = " ~ parts_count, not silence_mode) }} 
        --
        ---- last aggregated interval insert right border limit calculation
            {% set max_history_allowed_timestamp = fixed_now - dbt_improvado_utils.get_interval ( value = interval_fluctuation, unit = time_unit_name) %}

            {{ log( "Last aggregated interval insert right border limit calculation... | max_history_allowed_timestamp = " 
                    ~ max_history_allowed_timestamp, debug_mode) }}
        --
        ---- insert queries to _history aggregated table -- TODO: change name to insert_intervals
            {% for index in range(parts_count + 2) %}
                -- inserting intervals list calculation
                {% set left_where_condition, right_where_condition, left_having_condition, right_having_condition = 
                        dbt_improvado_utils.get_interval_list ( intervals_offset = index*materialized_window, 
                                            time_unit_name = time_unit_name, 
                                            start_time = start_time, 
                                            interval_fluctuation = interval_fluctuation, 
                                            materialized_window = materialized_window ) %}
                
                -- inserting intervals list calculation
                {% set target_history_insert_query = 
                        adapter.dispatch("insert_as", macro_namespace="dbt_improvado_utils")(  sql = sql,
                                                        target_relation = target_history_relation,
                                                        input_models_set = input_models_set, 
                                                        input_columns_set = input_columns_set, 
                                                        output_column = output_session_end_column,
                                                        max_having_right = max_history_allowed_timestamp,
                                                        left_where = left_where_condition, 
                                                        right_where = right_where_condition, 
                                                        left_having = left_having_condition, 
                                                        right_having = right_having_condition,
                                                        debug_mode = debug_mode)%}

                {% if execute %}
                    {% if loop.first %}
                        {{ log( "INSERT to : " ~ target_history_relation, not silence_mode) }}
                        {{ log( "index | left_where_condition | right_where_condition | left_having_condition | right_having_condition", not silence_mode) }}
                    {% endif %}

                    {% call statement(  'append history rows to target') -%}
                        {{target_history_insert_query}} 
                    {%- endcall -%} 
                    
                    {{ log( index ~ "     | " ~ left_where_condition ~ "  | " ~
                            right_where_condition ~ "   | " ~ left_having_condition ~ "   | " ~ right_having_condition, not silence_mode) }}
                {% endif %}
            {% endfor %}
        --
    --
    -- unfinished (not_final) materialized table
        {% if unfinished_section %}
        
            {{ log( "\n\n************** unfinished **************\n\n", not silence_mode) }} 
            -- update or create history relation

                {% set target_unfinished_relation_exists, target_unfinished_relation = 
                        dbt_improvado_utils.get_or_create_or_update_relation (  database = none, 
                                                            schema = model.schema, 
                                                            identifier = this.identifier ~ '_unfinished', 
                                                            type = 'table', 
                                                            update = True, 
                                                            temporary = False, 
                                                            sql = dbt_improvado_utils.select_limit_0(sql), 
                                                            debug_mode = debug_mode, 
                                                            silence_mode = silence_mode) %}

            --
            -- append unfinished relation to section set
                {%- do sections_arr.append(target_unfinished_relation) -%}
                {%- do sections_set.update({'unfinished': target_unfinished_relation}) -%}
            --
            -- calculation prewhere for unfinished (left and right borders of interval)

                {% set left_unfinished_pre_where_timestamp = 
                        fixed_now - dbt_improvado_utils.get_interval (value = interval_fluctuation * 2, unit = time_unit_name) %}
                {% set right_unfinished_pre_where_timestamp = fixed_now %}
            --
            -- post having condition (max_history_timestamp for having condition)

                {% set max_fact_history_timestamp =
                        adapter.dispatch("get_max_date_time_in_table", macro_namespace="dbt_improvado_utils")( target_history_relation, output_session_end_column) %}   

                {{ log( "max_history_timestamp for having condition: " ~ max_fact_history_timestamp, debug_mode) }} 
            --
            -- unfinished insert query 
                {% set target_unfinished_insert_query = 
                        adapter.dispatch("insert_as", macro_namespace="dbt_improvado_utils")(  sql = sql,
                                                        target_relation = target_unfinished_relation,
                                                        input_models_set = input_models_set, 
                                                        input_columns_set = input_columns_set, 
                                                        output_column = output_session_end_column,
                                                        max_having_right = right_unfinished_pre_where_timestamp,
                                                        left_where = left_unfinished_pre_where_timestamp, 
                                                        right_where = right_unfinished_pre_where_timestamp, 
                                                        left_having = max_fact_history_timestamp, 
                                                        right_having = right_unfinished_pre_where_timestamp,
                                                        debug_mode = debug_mode)%}                                                          
                
                {{ log( "INSERT to : " ~ target_unfinished_relation, not silence_mode) }}
                {{ log( "left_where_condition | right_where_condition | left_having_condition | right_having_condition", not silence_mode) }}

                {% call statement(  'append unfinished rows to target') -%}
                    {{ target_unfinished_insert_query }}  
                {%- endcall -%}

                {{ log( left_unfinished_pre_where_timestamp ~ "  | " ~ right_unfinished_pre_where_timestamp ~ "   | " ~ 
                        right_unfinished_pre_where_timestamp ~ "   | " ~ right_unfinished_pre_where_timestamp, not silence_mode) }}
       
        {% endif %}        
        --
    -- 
    -- live view for recent updates
        {% if life_section %}
          
            {{ log( "\n\n************** live **************\n\n", not silence_mode) }} 
            -- pre-where live view condition (left and right borders of interval)

                {% set left_live_pre_where_timestamp = 
                        fixed_now - dbt_improvado_utils.get_interval (value = interval_fluctuation * 2, unit = time_unit_name) %}

                {% set right_live_pre_where_timestamp = 
                        fixed_now.replace( hour=0, minute=0, second=0, microsecond=0 ) + dbt_improvado_utils.get_interval (value = 365, unit = 'day') %} 
            --
            -- [SQL for]: create _live_ view

                {% set target_live_create_query = 
                        adapter.dispatch("get_sql_for_insert", macro_namespace="dbt_improvado_utils")( sql = sql, 
                                                                input_models_set = input_models_set, 
                                                                input_columns_set = input_columns_set, 
                                                                output_column = output_session_end_column, 
                                                                max_having_right = right_live_pre_where_timestamp,
                                                                left_where = left_live_pre_where_timestamp, 
                                                                right_where = right_live_pre_where_timestamp, 
                                                                left_having = fixed_now, 
                                                                right_having = right_live_pre_where_timestamp,
                                                                debug_mode = debug_mode)%}  
            --
            -- update or create live view relation
                {% set target_live_relation_exists, target_live_relation = 
                        dbt_improvado_utils.get_or_create_or_update_relation (  database = none, 
                                                            schema = model.schema, 
                                                            identifier = this.identifier ~ '_live', 
                                                            type = 'view', 
                                                            update = True, 
                                                            temporary = False, 
                                                            sql = target_live_create_query, 
                                                            debug_mode = debug_mode, 
                                                            silence_mode = silence_mode) %}
            --
            --append live relation to section set
                {%- do sections_arr.append(target_live_relation) -%}
                {%- do sections_set.update({'live': target_live_relation}) -%}
            --
        {% endif %}
    --
    -- Output view. Union: history + not final + live
        {{ log( "\n\n************** output **************\n\n", not silence_mode) }} 

        -- [SQL for]: create output view + remove ids finished after unfinished materilization
            {% set output_sql = 
                    adapter.dispatch("union_all_relation", macro_namespace="dbt_improvado_utils")( sections_set,
                                                            output_id_column,
                                                            order_by,
                                                            unfinished_changes_filter) %}
        --
        -- update or create Output view relation
            {% set target_output_relation_exists, target_output_relation =
                    dbt_improvado_utils.get_or_create_or_update_relation (  database = none, 
                                                        schema = model.schema, 
                                                        identifier = this.identifier, 
                                                        type = 'view', 
                                                        update = True, 
                                                        temporary = False, 
                                                        sql = output_sql, 
                                                        debug_mode = debug_mode, 
                                                        silence_mode = silence_mode) %}
        --
        -- append output relation
            {%- do sections_arr.append(target_output_relation) -%}
        --
    -- return
        {% call noop_statement('main', 'Done') -%} {%- endcall %}
        {% do return ({'relations': sections_arr}) -%}
{% endmaterialization %}







{% macro is_table_change (database, schema, identifier_check, type, actual_sql, debug_mode, silence_mode) %}
    {# --
        Returns true if the existing table is not consistent with the query being executed. 
        The history table is used to check
    -- #}

    -- namespace for allow carrying a value from within a loop body to an outer scope
    {% set ns = namespace() %}

    {% set ns.is_change = False %}

    -- get relation check_table
    {% set relation = 
            adapter.get_relation( database = database, schema = schema, identifier = identifier_check ) %}
    
    {% if relation and execute %}

        {{ log( "-Check exist table consistent (columns types and count) with the query being executed...", debug_mode) }}

        -- create tmp table for comparison columns
       {% set check_relation_exists, check_relation = 
                dbt_improvado_utils.get_or_create_or_update_relation (  database = database, 
                                                    schema = schema, 
                                                    identifier = identifier_check~'_consistent_tmp', 
                                                    type = type, 
                                                    update = False, 
                                                    temporary = False, 
                                                    sql = dbt_improvado_utils.select_limit_0(actual_sql), 
                                                    debug_mode = debug_mode, 
                                                    silence_mode = silence_mode) %}
        
        {{ log( "-Check exist table consistent (columns types and count) with the query being executed...", debug_mode) }}

        {% set columns_new = adapter.get_columns_in_relation(relation) %}
        {% set columns_old = adapter.get_columns_in_relation(check_relation) %}

        -- drop tmp table for comparison columns
        {% do adapter.drop_relation(check_relation) %}

        {% if columns_old | length != columns_new | length %} 
            {% set ns.is_change = True %}
            {{ log( "Exist table columns number NOT consistent! Full update will be done.", not silence_mode) }}

        {% else %}
            {{ log( "Exist table columns consistent! Checking for name/data_type consistency " ~ relation, not silence_mode) }}

            {% for i in range(0, columns_new | length) %}
                {% set column_old = columns_old[i] %}
                {% set column_new = columns_new[i] %}

                {% if column_old.data_type != column_new.data_type or column_old.name != column_new.name %} 
                    {% set ns.is_change = True %}
                    {{ log( "Column name/type mismatch, \n" ~ 
                    column_old.name ~ " " ~ column_old.data_type ~ "\n" ~ 
                    column_new.name ~ " " ~ column_new.data_type ~ "\n" ~
                    "Full update will be done.", not silence_mode) }}
                {% endif %}
            {% endfor %}

        {% endif %}

    {% endif %}

    {{ return (ns.is_change) }}
{% endmacro %}

{% macro get_interval (value, unit) %}
    {# --
        A duration expressing the difference between two date, time, or datetime instances to unit resolution.
        Arguments:
            value       — The numerical value of the interval
            unit        — The type of interval for result. String. Possible values: (hour, day)
    -- #}

    {{ return (modules.datetime.timedelta(**{unit~'s': value})) }}
{% endmacro %}

{% macro dateDiff (startdate, enddate, unit) %}
    {# --
        Returns the difference between two dates or dates with time values. 
        Arguments:
            unit        — The type of interval for result. String. Possible values: (hour, day)
            startdate   — The first time value to subtract (the subtrahend). 
            enddate     — The second time value to subtract from (the minuend).
        Returned value:
            Difference between enddate and startdate expressed in unit.
    -- #}
    {{ return ((enddate-startdate).total_seconds()/ {'hour': 3600, 'day': 86400}[unit]) }}
{% endmacro %}

{% macro select_limit_0 (sql) %}
    {{return ("SELECT * FROM ("~sql~") WHERE 1!=1") }}
{% endmacro %}

{% macro get_interval_list (intervals_offset, time_unit_name, start_time, interval_fluctuation, materialized_window ) %}

    {% set interval_start           = start_time        + dbt_improvado_utils.get_interval ( value = intervals_offset, unit = time_unit_name) %}
    {% set right_where_condition    = interval_start    + dbt_improvado_utils.get_interval ( value = interval_fluctuation + materialized_window , unit = time_unit_name) %}
    {% set left_where_condition     = interval_start    - dbt_improvado_utils.get_interval ( value = interval_fluctuation, unit = time_unit_name) %}
    {% set right_having_condition   = interval_start    + dbt_improvado_utils.get_interval ( value = materialized_window, unit = time_unit_name) %}
    {% set left_having_condition    = interval_start %}

    {{return ([left_where_condition, right_where_condition, left_having_condition, right_having_condition])}}
{% endmacro %}

{% macro get_or_create_or_update_relation (database, schema, identifier, type, update, temporary, sql, debug_mode, silence_mode) %}
    {{ log( "\n*** Get or create relation: " ~ identifier ~ " | UPDATE flag = " ~ update ~ " ***\n", debug_mode) }}

    {% set relation_exists, relation = 
            get_or_create_relation( database = database, schema = schema, identifier = identifier, type = type ) %}

    -- drop previos version if (exist) and (update = true)
        {% if update and relation_exists %}
            -- drop previos version
                {{ log( "Target [ " ~ identifier ~ " ] relation exists: Deleting...", debug_mode) }}
                {% do adapter.drop_relation(relation) %}
                {{ log( "Target [ " ~ identifier ~ " ] relation -> DELETE old ", debug_mode) }}

            -- create new version
                {% set relation = 
                        api.Relation.create( database = database, schema = schema, identifier = identifier, type = type ) %}
        {% endif %}

    -- Create if relation (not exist) or (update = true)
        {% if (not relation_exists or update) and execute %}
            {{ log( "Target [ " ~ identifier ~ " ] relation not exist: Creating...", debug_mode) }} 

            {% call statement( 'Create target ' ~ identifier ~ ' ' ~ type ) %}
                {% if type == "table" %}
                    {{adapter.dispatch('create_table_as')( temporary = temporary, relation = relation, sql = sql)}}   
                {% else %}
                    {{adapter.dispatch('create_view_as')( relation = relation, sql = sql)}}
                {% endif %}
            {% endcall %}

            {{ log( "Target [ " ~ identifier ~ " ] relation -> CREATED ", not silence_mode) }} 
        {% endif %}

        {{return ([relation_exists, relation])}}
{% endmacro %}
