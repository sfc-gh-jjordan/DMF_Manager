import streamlit as st
import pandas as pd

try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except:
    from snowflake.snowpark import Session
    session = Session.builder.config('connection_name', 'default').create()

st.title("Data Metric Function Manager")

@st.cache_data(ttl=60)
def get_dmf_config():
    return session.sql("SELECT * FROM DEMO.GOVERNANCE.DMF_CONFIG WHERE IS_ACTIVE = TRUE").to_pandas()

@st.cache_data(ttl=60)
def get_applied_dmfs(table_name):
    try:
        return session.sql(f"""
            SELECT METRIC_NAME, ARRAY_TO_STRING(REF_ARGUMENTS::ARRAY, ',') AS COLUMNS
            FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
                REF_ENTITY_NAME => '{table_name}',
                REF_ENTITY_DOMAIN => 'TABLE'
            ))
        """).to_pandas()
    except:
        return pd.DataFrame(columns=['METRIC_NAME', 'COLUMNS'])

def call_sp_manage_dmf(action, filter_condition=None):
    if filter_condition:
        result = session.sql(f"CALL DEMO.GOVERNANCE.SP_MANAGE_DMF('DEMO.GOVERNANCE.DMF_CONFIG', '{action}', '{filter_condition}')").collect()
    else:
        result = session.sql(f"CALL DEMO.GOVERNANCE.SP_MANAGE_DMF('DEMO.GOVERNANCE.DMF_CONFIG', '{action}', NULL)").collect()
    return result[0][0] if result else None

config_df = get_dmf_config()

if config_df.empty:
    st.warning("No active DMF configurations found in DEMO.GOVERNANCE.DMF_CONFIG")
    st.stop()

config_df['FULL_TABLE'] = config_df['DATABASE_NAME'] + '.' + config_df['SCHEMA_NAME'] + '.' + config_df['TABLE_NAME']
config_df['DMF_SHORT'] = config_df['DMF_NAME'].apply(lambda x: x.split('.')[-1] if '.' in str(x) else x)
config_df['COLUMNS'] = config_df['COLUMN_NAMES'].fillna('')

unique_tables = config_df['FULL_TABLE'].unique()
applied_dmfs = {}
for table in unique_tables:
    applied_dmfs[table] = get_applied_dmfs(table)

def is_dmf_applied(row):
    table = row['FULL_TABLE']
    dmf_short = row['DMF_SHORT']
    cols = row['COLUMNS']
    
    if table not in applied_dmfs or applied_dmfs[table].empty:
        return False
    
    table_dmfs = applied_dmfs[table]
    for _, dmf_row in table_dmfs.iterrows():
        if dmf_row['METRIC_NAME'] == dmf_short:
            applied_cols = dmf_row['COLUMNS'] if pd.notna(dmf_row['COLUMNS']) else ''
            if applied_cols == cols:
                return True
    return False

config_df['IS_APPLIED'] = config_df.apply(is_dmf_applied, axis=1)

action = st.segmented_control(
    "Select action",
    options=["Add DMFs", "Drop DMFs", "Validate DMFs"],
    default="Add DMFs"
)

st.divider()

if action == "Add DMFs":
    st.subheader("Add new data metric functions")
    not_applied_df = config_df[~config_df['IS_APPLIED']].copy()
    
    if not_applied_df.empty:
        st.success("All configured DMFs have already been applied!")
    else:
        st.caption(f"Found {len(not_applied_df)} DMF(s) not yet applied")
        
        not_applied_df['SELECT'] = False
        edited_df = st.data_editor(
            not_applied_df[['SELECT', 'TABLE_NAME', 'DMF_NAME', 'COLUMN_NAMES', 'DMF_SCHEDULE']],
            column_config={
                "SELECT": st.column_config.CheckboxColumn("Select", default=False),
                "TABLE_NAME": st.column_config.TextColumn("Table"),
                "DMF_NAME": st.column_config.TextColumn("DMF"),
                "COLUMN_NAMES": st.column_config.TextColumn("Columns"),
                "DMF_SCHEDULE": st.column_config.TextColumn("Schedule")
            },
            hide_index=True,
            use_container_width=True,
            disabled=['TABLE_NAME', 'DMF_NAME', 'COLUMN_NAMES', 'DMF_SCHEDULE']
        )
        
        selected_rows = edited_df[edited_df['SELECT'] == True]
        
        if st.button("Add selected DMFs", type="primary", disabled=len(selected_rows) == 0):
            with st.spinner("Adding DMFs..."):
                for _, row in selected_rows.iterrows():
                    filter_cond = f"TABLE_NAME = '{row['TABLE_NAME']}' AND DMF_NAME = '{row['DMF_NAME']}'"
                    result = call_sp_manage_dmf('ADD', filter_cond)
                    st.write(result)
            st.cache_data.clear()
            st.rerun()

elif action == "Drop DMFs":
    st.subheader("Drop existing data metric functions")
    applied_df = config_df[config_df['IS_APPLIED']].copy()
    
    if applied_df.empty:
        st.info("No DMFs are currently applied to drop.")
    else:
        st.caption(f"Found {len(applied_df)} DMF(s) currently applied")
        
        applied_df['SELECT'] = False
        edited_df = st.data_editor(
            applied_df[['SELECT', 'TABLE_NAME', 'DMF_NAME', 'COLUMN_NAMES']],
            column_config={
                "SELECT": st.column_config.CheckboxColumn("Select", default=False),
                "TABLE_NAME": st.column_config.TextColumn("Table"),
                "DMF_NAME": st.column_config.TextColumn("DMF"),
                "COLUMN_NAMES": st.column_config.TextColumn("Columns")
            },
            hide_index=True,
            use_container_width=True,
            disabled=['TABLE_NAME', 'DMF_NAME', 'COLUMN_NAMES']
        )
        
        selected_rows = edited_df[edited_df['SELECT'] == True]
        
        if st.button("Drop selected DMFs", type="primary", disabled=len(selected_rows) == 0):
            with st.spinner("Dropping DMFs..."):
                for _, row in selected_rows.iterrows():
                    filter_cond = f"TABLE_NAME = '{row['TABLE_NAME']}' AND DMF_NAME = '{row['DMF_NAME']}'"
                    result = call_sp_manage_dmf('DROP', filter_cond)
                    st.write(result)
            st.cache_data.clear()
            st.rerun()

else:
    st.subheader("Validate data metric functions")
    st.caption("Execute DMFs to validate they work correctly")
    
    config_df['SELECT'] = False
    edited_df = st.data_editor(
        config_df[['SELECT', 'TABLE_NAME', 'DMF_NAME', 'COLUMN_NAMES', 'IS_APPLIED']],
        column_config={
            "SELECT": st.column_config.CheckboxColumn("Select", default=False),
            "TABLE_NAME": st.column_config.TextColumn("Table"),
            "DMF_NAME": st.column_config.TextColumn("DMF"),
            "COLUMN_NAMES": st.column_config.TextColumn("Columns"),
            "IS_APPLIED": st.column_config.CheckboxColumn("Applied", disabled=True)
        },
        hide_index=True,
        use_container_width=True,
        disabled=['TABLE_NAME', 'DMF_NAME', 'COLUMN_NAMES', 'IS_APPLIED']
    )
    
    selected_rows = edited_df[edited_df['SELECT'] == True]
    
    if st.button("Validate selected DMFs", type="primary", disabled=len(selected_rows) == 0):
        with st.spinner("Validating DMFs..."):
            for _, row in selected_rows.iterrows():
                filter_cond = f"TABLE_NAME = '{row['TABLE_NAME']}' AND DMF_NAME = '{row['DMF_NAME']}'"
                result = call_sp_manage_dmf('VALIDATE', filter_cond)
                st.write(result)

st.divider()
with st.expander("View current DMF status"):
    status_df = config_df[['TABLE_NAME', 'DMF_NAME', 'COLUMN_NAMES', 'DMF_SCHEDULE', 'IS_APPLIED']]
    st.dataframe(
        status_df,
        column_config={
            "IS_APPLIED": st.column_config.CheckboxColumn("Applied", disabled=True)
        },
        hide_index=True,
        use_container_width=True
    )
