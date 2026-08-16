from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.eczachly.trino_queries import execute_trino_query
import os

# TODO make sure to rename this if you're testing this dag out!
schema = os.environ.get('STUDENT_SCHEMA', 'zachwilson')


@dag(
    description="A dag that aggregates data from Iceberg into metrics",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2025, 5, 13),
        "retries": 0,
        "execution_timeout": timedelta(hours=1),
    },
    start_date=datetime(2025, 5, 13),
    max_active_runs=1,
    schedule="@daily",
    catchup=False,
    template_searchpath='include/eczachly',
    tags=["community"],
)
def dataexpert_snapshot_dag():

    yesterday_ds = '{{ yesterday_ds }}'
    ds = '{{ ds }}'
    source_map = {
        'dataexpert_production.public.blog_users': {
            'create': f"""
                CREATE TABLE IF NOT EXISTS {schema}.blog_users (
                        user_id BIGINT,
                        email_notifications_enabled BOOLEAN,
                        created_at TIMESTAMP,
                        updated_at TIMESTAMP,
                        ds DATE
                    )
                    WITH (
                        partitioning = ARRAY['ds']
                    
                    )
        """,
            'insert': f"""
            INSERT INTO {schema}.blog_users
                SELECT user_id, 
                       email_notifications_enabled, 
                       created_at,
                       updated_at, 
                       DATE('{ds}')
                FROM dataexpert_production.public.blog_users
            """
        },
        'dataexpert_production.bootcamp.academies': {
            'create': f"""
            CREATE TABLE IF NOT EXISTS {schema}.academies (
                academy_id BIGINT,
                academy_name VARCHAR,
                academy_domain VARCHAR,
                instructor_id BIGINT,
                ds DATE
            )
            WITH (
                partitioning = ARRAY['ds']
                )
            """,
            'insert': f"""
                INSERT INTO {schema}.academies
                SELECT academy_id, 
                        academy_name, 
                        academy_domain, 
                        instructor_id, 
                        DATE('{ds}')
                FROM dataexpert_production.bootcamp.academies

                
            """,
        },
        'dataexpert_production.bootcamp.programs': {
            'create': f"""
               CREATE TABLE IF NOT EXISTS {schema}.programs (
                program_id                               INTEGER,
                name                                     VARCHAR,
                description                              VARCHAR,
                start_date                               DATE,
                end_date                                 DATE,
                cohort_id                                INTEGER,
                product_id                               VARCHAR,
                is_active                                BOOLEAN,
                is_test_program                          BOOLEAN,
                is_subscription                          BOOLEAN,
                is_featured                              BOOLEAN,
                required_submissions_for_certification   INTEGER,
                required_minutes_for_certification       INTEGER,
                discord_role                             VARCHAR,
                price                                    REAL,
                academy_id                               INTEGER,
                schedule_image                           VARCHAR,
                homework_image                           VARCHAR,
                guest_speakers_image                     VARCHAR,
                monthly_price                            REAL,
                monthly_price_stripe_id                  VARCHAR,
                annual_price                             REAL,
                annual_price_stripe_id                   VARCHAR,
                one_off_price_stripe_id                  VARCHAR,
                slug                                     VARCHAR,
                discord_invite_link                      VARCHAR,
                stage_channel_id                         VARCHAR,
                ds                                       DATE
            )
            WITH (
            
                partitioning   = ARRAY['ds']  
            )
                """,
            'insert': f"""
                    INSERT INTO {schema}.programs
                    SELECT *, DATE('{ds}')
                    FROM dataexpert_production.bootcamp.programs
                """
        },
        'dataexpert_production.bootcamp.user_programs': {
            'create': f"""
                  CREATE TABLE IF NOT EXISTS {schema}.user_programs (
                    user_id                               integer,
                    program_id                            integer,
                    join_date                             date,
                    is_active                             boolean,
                    most_recent_course_activity           integer,
                    has_lifetime_access                   boolean,
                    most_recent_course_activity_timestamp timestamp,
                    ds DATE
               )
               WITH (
                   partitioning   = ARRAY['ds']  
               )
                   """,
            'insert': f"""
                       INSERT INTO {schema}.user_programs
                       SELECT *, DATE('{ds}')
                       FROM dataexpert_production.bootcamp.user_programs
                   """
        },
        'dataexpert_production.bootcamp.user_sql_queries': {
            'create': f"""
                    CREATE TABLE IF NOT EXISTS {schema}.user_sql_queries (
                        user_id           integer,
                        query             VARCHAR,
                        query_id          VARCHAR,
                        query_status      VARCHAR,
                        start_time        timestamp,
                        query_source_page VARCHAR,
                        query_engine      VARCHAR,
                      ds DATE
                 )
                 WITH (
                     partitioning   = ARRAY['ds']  
                 )
                     """,
            'insert': f"""
                         INSERT INTO {schema}.user_sql_queries
                         SELECT *, DATE('{ds}')
                         FROM dataexpert_production.bootcamp.user_sql_queries
                     """
        },
        'dataexpert_production.bootcamp.video_intervals': {
            'create': f"""
                       CREATE TABLE IF NOT EXISTS {schema}.video_intervals (
                           slug                 VARCHAR,
                        module               VARCHAR,
                        user_id              integer,
                        view_start_timestamp bigint,
                        view_end_timestamp   bigint,
                        event_timestamp      timestamp,
                        course_version       VARCHAR,
                        course_id            integer,
                         ds DATE
                    )
                    WITH (
                        partitioning   = ARRAY['ds']  
                    )
                        """,
            'insert': f"""
                            INSERT INTO {schema}.video_intervals
                            SELECT *, DATE('{ds}')
                            FROM dataexpert_production.bootcamp.video_intervals
                            WHERE DATE(event_timestamp) = DATE('{ds}')
                        """
        },
        'dataexpert_production.bootcamp.modules': {
            'create': f"""
                          CREATE TABLE IF NOT EXISTS {schema}.modules (
                              module_id           integer,
                        title               VARCHAR,
                        slug                VARCHAR,
                        attendance_required boolean,
                        has_homework        boolean,
                        sort_order          integer,
                        name                VARCHAR,
                        instructor_id       integer,
                        academy_id          integer,
                        program_id          integer,
                        start_date          date,
                            ds DATE
                       )
                       WITH (
                           partitioning   = ARRAY['ds']  
                       )
                           """,
            'insert': f"""
                               INSERT INTO {schema}.modules
                               SELECT *, DATE('{ds}')
                               FROM dataexpert_production.bootcamp.modules
                           """
        },
        'dataexpert_production.bootcamp.program_modules': {
            'create': f"""
                              CREATE TABLE IF NOT EXISTS {schema}.program_modules (
                                program_id integer,
                                module_id INTEGER,
                                sort_order INTEGER,
                                ds DATE
                           )
                           WITH (
                               partitioning   = ARRAY['ds']  
                           )
                               """,
            'insert': f"""
                                   INSERT INTO {schema}.program_modules
                                   SELECT *, DATE('{ds}')
                                   FROM dataexpert_production.bootcamp.program_modules
                               """
        },
        'dataexpert_production.bootcamp.module_courses': {
            'create': f"""
                                 CREATE TABLE IF NOT EXISTS {schema}.module_courses (
                                     course_id INTEGER,
                                     module_id INTEGER,
                                     sort_order INTEGER,
                                   ds DATE
                              )
                              WITH (
                                  partitioning   = ARRAY['ds']  
                              )
                                  """,
            'insert': f"""
                                      INSERT INTO {schema}.module_courses
                                      SELECT *, DATE('{ds}')
                                      FROM dataexpert_production.bootcamp.module_courses
                                  """
        },
        'dataexpert_production.bootcamp.course_content': {
            'create': f"""
                                     CREATE TABLE IF NOT EXISTS {schema}.course_content (
                                        title                      varchar,
                                        video_url                  varchar,
                                        github_url                 varchar,
                                        description                varchar,
                                        slides_url                 varchar,
                                        slug                       varchar,
                                        duration_seconds           integer,
                                        version                    varchar,
                                        has_query_editor           boolean,
                                        instructor_id              integer,
                                        date_recorded              date,
                                        is_free                    boolean,
                                        attendance_required        boolean,
                                        upload_time                timestamp,
                                        processing_completion_time timestamp,
                                        event_id                   integer,
                                        course_id                  integer,
                                        raw_video_url              varchar,
                                        academy_id                 integer,
                                        image_url                  varchar,
                                        transcript_url             varchar,
                                       ds DATE
                                  )
                                  WITH (
                                      partitioning   = ARRAY['ds']  
                                  )
                                      """,
            'insert': f"""
                                          INSERT INTO {schema}.course_content
                                          SELECT *, DATE('{ds}')
                                          FROM dataexpert_production.bootcamp.course_content
                                      """
        },
        'dataexpert_production.bootcamp.course_view_time': {
            'create': f"""
                                        CREATE TABLE IF NOT EXISTS {schema}.course_view_time (
                                          user_id INTEGER,
                                          course_id INTEGER,
                                          seconds_watched INTEGER,
                                          ds DATE
                                     )
                                     WITH (
                                         partitioning   = ARRAY['ds']  
                                     )
                                         """,
            'insert': f"""
                                             INSERT INTO {schema}.course_view_time
                                             SELECT *, DATE('{ds}')
                                             FROM dataexpert_production.bootcamp.course_view_time
            """
        }
    }

    starting_point = EmptyOperator(
        task_id='start'
    )

    for table in source_map:
        insert_query = source_map[table]['insert']
        create_query = source_map[table]['create']
        table_name = schema + '.' + table.split('.')[-1]

        source_map[table]['create_step'] = PythonOperator(
            task_id="create_step_" + table_name,
            depends_on_past=True,
            python_callable=execute_trino_query,
            op_kwargs={
                'query': create_query
            }
        )
        source_map[table]['clear_step'] = PythonOperator(
            task_id="clear_step_" + table_name,
            depends_on_past=True,
            python_callable=execute_trino_query,
            op_kwargs={
                'query': f"""
                   DELETE FROM {table_name} 
                   WHERE ds = DATE('{ds}')
                """
            }
        )
        source_map[table]['insert_step'] = PythonOperator(
            task_id="insert_step_" + table_name,
            depends_on_past=True,
            python_callable=execute_trino_query,
            op_kwargs={
                'query': insert_query
            }
        )

        (starting_point >> source_map[table]['create_step'] >>
         source_map[table]['clear_step'] >> source_map[table]['insert_step'])




dataexpert_snapshot_dag()
