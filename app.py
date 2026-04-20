import base64
from datetime import datetime, timezone
import json
import os
import time
from functools import wraps
from pathlib import Path

import mysql.connector
from flask import Flask, flash, redirect, render_template, request, session, url_for
from mysql.connector import errorcode


APP_TITLE = "HeatWave Demo"
ROOT_DIR = Path(__file__).resolve().parent
PROFILE_STORE = ROOT_DIR / "profiles.json"
DEFAULT_PROFILE = {
    "name": "",
    "host": "",
    "port": 3306,
    "database": "performance_schema",
}
NAV_GROUPS = [
    {
        "label": "Home",
        "items": [{"endpoint": "home", "label": "Dashboard"}],
    },
    {
        "label": "Admin",
        "items": [
            {"endpoint": "connection_profile", "label": "Connection Profile"},
            {"endpoint": "setup_configdb_page", "label": "Setup configdb"},
        ],
    },
    {
        "label": "HeatWave",
        "items": [
            {"endpoint": "nlsql_page", "label": "NL_SQL"},
            {"endpoint": "vision_page", "label": "HWVision"},
        ],
    },
]
HEATWAVE_PERFORMANCE_SQL = {
    "innodb": """
SELECT /*+ SET_VAR(use_secondary_engine=off) */ airline.airlinename, SUM(booking.price) as price_tickets, count(*) as nb_tickets
      FROM airportdb.booking booking, airportdb.flight flight, airportdb.airline airline, airportdb.airport_geo airport_geo
      WHERE booking.flight_id=flight.flight_id
      AND airline.airline_id=flight.airline_id
      AND flight.from=airport_geo.airport_id
      AND airport_geo.country = "UNITED STATES"
      GROUP BY airline.airlinename
      ORDER BY nb_tickets desc, airline.airlinename
      LIMIT 10;
""".strip(),
    "rapid": """
SELECT /*+ SET_VAR(use_secondary_engine=on) */ airline.airlinename, SUM(booking.price) as price_tickets, count(*) as nb_tickets
      FROM airportdb.booking booking, airportdb.flight flight, airportdb.airline airline, airportdb.airport_geo airport_geo
      WHERE booking.flight_id=flight.flight_id
      AND airline.airline_id=flight.airline_id
      AND flight.from=airport_geo.airport_id
      AND airport_geo.country = "UNITED STATES"
      GROUP BY airline.airlinename
      ORDER BY nb_tickets desc, airline.airlinename
      LIMIT 10;
""".strip(),
}
HEATWAVE_PERFORMANCE_EXEC_SQL = {
    "innodb": """
SELECT /*+ SET_VAR(use_secondary_engine=off) */ airline.airlinename, SUM(booking.price) as price_tickets, count(*) as nb_tickets
      FROM airportdb.booking booking, airportdb.flight flight, airportdb.airline airline, airportdb.airport_geo airport_geo
      WHERE booking.flight_id=flight.flight_id
      AND airline.airline_id=flight.airline_id
      AND flight.from=airport_geo.airport_id
      AND airport_geo.country = "UNITED STATES"
      GROUP BY airline.airlinename
      ORDER BY nb_tickets desc, airline.airlinename
      LIMIT 10
""".strip(),
    "rapid": """
SELECT /*+ SET_VAR(use_secondary_engine=on) */ airline.airlinename, SUM(booking.price) as price_tickets, count(*) as nb_tickets
      FROM airportdb.booking booking, airportdb.flight flight, airportdb.airline airline, airportdb.airport_geo airport_geo
      WHERE booking.flight_id=flight.flight_id
      AND airline.airline_id=flight.airline_id
      AND flight.from=airport_geo.airport_id
      AND airport_geo.country = "UNITED STATES"
      GROUP BY airline.airlinename
      ORDER BY nb_tickets desc, airline.airlinename
      LIMIT 10
""".strip(),
}
PREFERRED_DEFAULT_MODEL = "meta.llama-3.3-70b-instruct"
app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "change-this-secret-key")
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024


def ensure_profile_store():
    if PROFILE_STORE.exists():
        return
    PROFILE_STORE.write_text(json.dumps({"profiles": []}, indent=2), encoding="utf-8")


def _normalized_port(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return DEFAULT_PROFILE["port"]


def normalize_profile(payload):
    return {
        "name": str(payload.get("name", "")).strip(),
        "host": str(payload.get("host", "")).strip(),
        "port": _normalized_port(payload.get("port")),
        "database": str(payload.get("database", "")).strip(),
    }


def load_profiles():
    ensure_profile_store()
    try:
        data = json.loads(PROFILE_STORE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    rows = data.get("profiles", [])
    profiles = []
    for row in rows:
        profile = normalize_profile(row)
        if profile["name"]:
            profiles.append(profile)
    return sorted(profiles, key=lambda item: item["name"].lower())


def save_profiles(profiles):
    normalized = []
    seen_names = set()
    for row in profiles:
        profile = normalize_profile(row)
        if not profile["name"]:
            continue
        key = profile["name"].lower()
        if key in seen_names:
            continue
        seen_names.add(key)
        normalized.append(profile)
    PROFILE_STORE.write_text(
        json.dumps({"profiles": normalized}, indent=2),
        encoding="utf-8",
    )


def get_profile_by_name(profile_name):
    lookup = str(profile_name or "").strip().lower()
    for profile in load_profiles():
        if profile["name"].lower() == lookup:
            return profile
    return None


def get_session_profile():
    payload = session.get("connection_profile", {})
    if not payload:
        return normalize_profile(DEFAULT_PROFILE)
    return normalize_profile(payload)


def set_session_profile(profile):
    new_profile = normalize_profile(profile)
    old_profile = get_session_profile()
    if old_profile != new_profile:
        clear_login_state(keep_profile=False)
    session["connection_profile"] = new_profile
    session["profile_name"] = new_profile["name"]


def get_selected_profile_name():
    requested = request.args.get("profile", "").strip()
    if requested:
        return requested
    requested = request.values.get("profile_name", "").strip()
    if requested:
        return requested
    return str(session.get("profile_name", "")).strip()


def profile_is_complete(profile=None):
    active = profile or get_session_profile()
    return bool(active["host"] and active["database"] and active["port"])


def get_connection_summary():
    profile = get_session_profile()
    if not profile_is_complete(profile):
        return "Not configured"
    return "{host}:{port}/{database}".format(**profile)


def clear_login_state(keep_profile=True):
    profile = get_session_profile() if keep_profile else None
    session["logged_in"] = False
    session["db_user"] = ""
    session["db_password"] = ""
    if keep_profile and profile:
        session["connection_profile"] = profile
        session["profile_name"] = profile["name"]
    elif not keep_profile:
        session.pop("connection_profile", None)
        session.pop("profile_name", None)


def get_connection_config(user=None, password=None, include_database=True):
    profile = get_session_profile()
    config = {
        "host": profile["host"],
        "port": profile["port"],
    }
    if include_database and profile["database"]:
        config["database"] = profile["database"]
    resolved_user = session.get("db_user", "") if user is None else user
    resolved_password = session.get("db_password", "") if password is None else password
    if resolved_user:
        config["user"] = resolved_user
    if resolved_password:
        config["password"] = resolved_password
    return config


def mysql_connection(config=None, *, autocommit=False):
    cnx = mysql.connector.connect(**(config or get_connection_config()))
    cnx.autocommit = bool(autocommit)
    return cnx


def run_sql(sql_text, params=None, *, include_database=True, autocommit=False):
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(
            get_connection_config(include_database=include_database),
            autocommit=autocommit,
        )
        cursor = cnx.cursor()
        cursor.execute(sql_text, params or ())
        return cursor.fetchall()
    finally:
        if cursor:
            cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()


def exec_sql(sql_text, params=None, *, include_database=True, autocommit=False):
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(
            get_connection_config(include_database=include_database),
            autocommit=autocommit,
        )
        cursor = cnx.cursor()
        cursor.execute(sql_text, params or ())
        cnx.commit()
        return True
    except mysql.connector.Error:
        if cnx:
            cnx.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()


def run_sql_with_columns(sql_text, params=None, *, include_database=True, autocommit=False):
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(
            get_connection_config(include_database=include_database),
            autocommit=autocommit,
        )
        cursor = cnx.cursor()
        cursor.execute(sql_text, params or ())
        return {
            "columns": list(cursor.column_names or ()),
            "rows": [list(row) for row in cursor.fetchall()],
        }
    finally:
        if cursor:
            cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()


def call_proc(proc_name, args):
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection()
        cursor = cnx.cursor()
        result_args = cursor.callproc(proc_name, args)
        datasets = []
        columns = []
        for result in cursor.stored_results():
            datasets.append(result.fetchall())
            columns.append(result.column_names)
        return {
            "output": result_args[1] if len(result_args) > 1 else "",
            "resultset": datasets,
            "columnset": columns,
        }
    finally:
        if cursor:
            cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()


def login_required(route_handler):
    @wraps(route_handler)
    def wrapped(*args, **kwargs):
        if not session.get("logged_in"):
            flash("Log in with a saved connection profile first.", "warning")
            return redirect(url_for("login"))
        return route_handler(*args, **kwargs)

    return wrapped


def validate_user(user, password):
    config = get_connection_config(
        user=user,
        password=password,
        include_database=bool(get_session_profile()["database"]),
    )
    try:
        cnx = mysql_connection(config)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            return False, "Something is wrong with your user name or password."
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            return False, "The configured default database does not exist."
        return False, str(err)
    cnx.close()
    return True, ""


def setup_db():
    exec_sql("create database if not exists nlsql", include_database=False)
    exec_sql(
        """
        create table if not exists nlsql.configdb (
            db_name varchar(64) not null primary key,
            enabled char(1) not null
        )
        """,
        include_database=False,
    )
    exec_sql(
        """
        insert into nlsql.configdb (db_name, enabled)
        select defaults.schema_name_value, 'Y'
        from (
            select 'information_schema' as schema_name_value
            union all
            select 'sys'
            union all
            select 'performance_schema'
        ) defaults
        left join nlsql.configdb existing
            on existing.db_name = defaults.schema_name_value
        where existing.db_name is null
        """,
        include_database=False,
    )


def fetch_enabled_databases():
    rows = run_sql(
        """
        select db_name as db_name_value
        from nlsql.configdb
        where enabled = 'Y'
        order by db_name
        """,
        include_database=False,
    )
    return [row[0] for row in rows]


def fetch_available_databases():
    rows = run_sql(
        """
        select schema_name as schema_name_value
        from information_schema.schemata
        where schema_name <> 'nlsql'
        order by schema_name
        """,
        include_database=False,
    )
    return [row[0] for row in rows]


def save_configdb_databases(database_names):
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(get_connection_config(include_database=False))
        cursor = cnx.cursor()
        cursor.execute("delete from nlsql.configdb")
        if database_names:
            cursor.executemany(
                """
                insert into nlsql.configdb (db_name, enabled)
                values (%s, 'Y')
                """,
                [(name,) for name in sorted(set(database_names))],
            )
        cnx.commit()
    except mysql.connector.Error:
        if cnx:
            cnx.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()


def _get_model_ids(query):
    rows = run_sql(query)
    return [row[0] for row in rows]


def choose_default_model(models):
    if PREFERRED_DEFAULT_MODEL in models:
        return PREFERRED_DEFAULT_MODEL
    return models[0] if models else ""


def get_generation_models():
    return _get_model_ids(
        """
        select model_id as model_id_value
        from sys.ML_SUPPORTED_LLMS
        where capabilities->>'$[0]' = 'GENERATION'
        """
    )


def get_nlsql_models():
    return get_generation_models()


def get_vision_models():
    return get_generation_models()


def _mysql_quote(value):
    text = str(value or "")
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def airportdb_exists(*, autocommit=False):
    rows = run_sql(
        """
        select schema_name as schema_name_value
        from information_schema.schemata
        where schema_name = %s
        """,
        ("airportdb",),
        include_database=False,
        autocommit=autocommit,
    )
    return bool(rows)


def build_nav_groups():
    groups = []
    show_performance = False
    if session.get("logged_in"):
        try:
            show_performance = airportdb_exists()
        except mysql.connector.Error:
            show_performance = False

    for group in NAV_GROUPS:
        items = [dict(item) for item in group["items"]]
        if group["label"] == "HeatWave" and show_performance:
            items.append(
                {
                    "endpoint": "heatwave_performance_page",
                    "label": "HeatWave Performance",
                }
            )
        groups.append({"label": group["label"], "items": items})
    return groups


def call_nlsql(question, model_id, schemas):
    dblist = ", ".join('"{}"'.format(schema_name) for schema_name in schemas)
    options = '{{"execute": true, "model_id": "{llm}", "schemas": [{schemas}]}}'.format(
        llm=model_id,
        schemas=dblist,
    )
    return call_proc("sys.NL_SQL", [question, "", options])


def build_nlsql_call_text(question, model_id, schemas):
    dblist = ", ".join('"{}"'.format(schema_name) for schema_name in schemas)
    options = '{{"execute": true, "model_id": "{llm}", "schemas": [{schemas}]}}'.format(
        llm=model_id,
        schemas=dblist,
    )
    return (
        "CALL sys.NL_SQL(\n"
        "  {question},\n"
        "  '',\n"
        "  {options}\n"
        ");"
    ).format(
        question=_mysql_quote(question),
        options=_mysql_quote(options),
    )


def build_nlsql_tables(result):
    tables = []
    for index, rows in enumerate(result.get("resultset", [])):
        raw_columns = result.get("columnset", [])
        columns = list(raw_columns[index]) if index < len(raw_columns) else []
        tables.append(
            {
                "columns": columns,
                "rows": [list(row) for row in rows],
            }
        )
    return tables


def answer_query_on_image(question, model_id, image_base64):
    rows = run_sql(
        """
        select sys.ML_GENERATE(
            %s,
            JSON_OBJECT('model_id', %s, 'image', %s)
        ) as response_value
        """,
        (question, model_id, image_base64),
    )
    return rows[0][0] if rows else ""


def render_dashboard(template_name, **context):
    return render_template(
        template_name,
        app_title=APP_TITLE,
        nav_groups=build_nav_groups(),
        current_user=session.get("db_user", ""),
        current_profile_name=session.get("profile_name", ""),
        connection_summary=get_connection_summary(),
        logged_in=bool(session.get("logged_in")),
        current_endpoint=request.endpoint or "",
        current_table="",
        **context,
    )


def redirect_for_profile_update(profile_name=""):
    return_to = request.form.get("return_to", "").strip()
    if return_to == "connection_profile" and session.get("logged_in"):
        return redirect(url_for("connection_profile", profile=profile_name))
    return redirect(url_for("login", profile=profile_name))


@app.route("/", methods=["GET"])
def home():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    return render_dashboard("dashboard.html", page_title="Dashboard")


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("logged_in"):
        return redirect(url_for("home"))

    profiles = load_profiles()
    selected_profile_name = get_selected_profile_name()
    selected_profile = get_profile_by_name(selected_profile_name) or normalize_profile(DEFAULT_PROFILE)
    if request.method == "POST":
        selected_profile_name = request.form.get("profile_name", "").strip()
        selected_profile = get_profile_by_name(selected_profile_name)
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if not selected_profile:
            flash("Choose a saved connection profile before logging in.", "warning")
        else:
            set_session_profile(selected_profile)
            ok, message = validate_user(username, password)
            if ok:
                session["db_user"] = username
                session["db_password"] = password
                session["logged_in"] = True
                flash("Login successful.", "success")
                return redirect(url_for("home"))
            clear_login_state()
            flash(message or "Invalid connection profile or database credentials.", "error")
        selected_profile = selected_profile or normalize_profile(DEFAULT_PROFILE)

    return render_template(
        "login.html",
        app_title=APP_TITLE,
        profiles=profiles,
        selected_profile_name=selected_profile_name,
        selected_profile=selected_profile,
        form_profile=selected_profile,
        logged_in=False,
    )


@app.route("/profiles", methods=["POST"])
def save_profile_route():
    existing_profiles = load_profiles()
    action = request.form.get("profile_action", "save")
    profile = normalize_profile(request.form)

    if not profile["name"]:
        flash("Profile name is required.", "warning")
        return redirect_for_profile_update()

    if action == "delete":
        updated = [row for row in existing_profiles if row["name"].lower() != profile["name"].lower()]
        save_profiles(updated)
        if session.get("profile_name", "").lower() == profile["name"].lower():
            clear_login_state(keep_profile=False)
        flash("Profile deleted.", "success")
        return redirect_for_profile_update()

    updated = [row for row in existing_profiles if row["name"].lower() != profile["name"].lower()]
    updated.append(profile)
    save_profiles(updated)

    if session.get("profile_name", "").lower() == profile["name"].lower():
        set_session_profile(profile)
    flash("Profile saved.", "success")
    return redirect_for_profile_update(profile["name"])


@app.route("/logout", methods=["POST"])
def logout():
    clear_login_state()
    flash("Logged out.", "info")
    return redirect(url_for("login"))


@app.route("/connection-profile", methods=["GET"])
@login_required
def connection_profile():
    profiles = load_profiles()
    current_name = get_selected_profile_name() or session.get("profile_name", "")
    current_profile = get_profile_by_name(current_name) or get_session_profile()
    return render_dashboard(
        "connection_profile.html",
        page_title="Connection Profile",
        profiles=profiles,
        selected_profile_name=current_name,
        form_profile=current_profile,
    )


@app.route("/setup-configdb", methods=["GET", "POST"])
@login_required
def setup_configdb_page():
    try:
        setup_db()
        if request.method == "POST":
            selected = request.form.getlist("enabled_databases")
            save_configdb_databases(selected)
            flash("Updated nlsql.configdb.", "success")
            return redirect(url_for("setup_configdb_page"))
        configured = fetch_enabled_databases()
        available = fetch_available_databases()
    except mysql.connector.Error as error:
        flash(str(error), "error")
        configured = []
        available = []

    return render_dashboard(
        "setup_configdb.html",
        page_title="Setup configdb",
        configured_databases=configured,
        available_databases=available,
        unconfigured_databases=[name for name in available if name not in configured],
    )


@app.route("/nlsql", methods=["GET", "POST"])
@login_required
def nlsql_page():
    question = ""
    selected_databases = []
    sql_text = ""
    table_names = ""
    result_tables = []
    models = []
    selected_model = ""
    proc_call_text = ""

    try:
        setup_db()
        available_databases = fetch_enabled_databases()
        models = get_nlsql_models()
        selected_model = choose_default_model(models)
        default_databases = [
            name
            for name in ("information_schema", "performance_schema")
            if name in available_databases
        ]

        if request.method == "POST":
            question = request.form.get("question", "").strip()
            selected_databases = request.form.getlist("databases")
            selected_model = request.form.get("llm", "").strip() or selected_model

            if not question:
                flash("Enter a question.", "warning")
            elif not selected_databases:
                flash("Choose at least one schema.", "warning")
            elif not selected_model:
                flash("No supported generation models were found for this connection.", "error")
            else:
                proc_call_text = build_nlsql_call_text(question, selected_model, selected_databases)
                response = call_nlsql(question, selected_model, selected_databases)
                output = json.loads(response.get("output") or "{}")
                sql_text = output.get("sql_query", "")
                table_names = output.get("tables", "")
                if output.get("is_sql_valid") == 1:
                    result_tables = build_nlsql_tables(response)
        else:
            selected_databases = default_databases
    except mysql.connector.Error as error:
        flash(str(error), "error")
        available_databases = []
    except json.JSONDecodeError:
        flash("The NL_SQL response could not be parsed.", "error")
        available_databases = fetch_enabled_databases() if session.get("logged_in") else []

    return render_dashboard(
        "nlsql.html",
        page_title="HWnlsql",
        available_databases=available_databases,
        selected_databases=selected_databases,
        llm_models=models,
        selected_model=selected_model,
        question=question,
        proc_call_text=proc_call_text,
        sql_text=sql_text,
        table_names=table_names,
        result_tables=result_tables,
        docs_url="https://dev.mysql.com/doc/heatwave/en/mys-hw-genai-nl-sql.html",
    )


@app.route("/vision", methods=["GET", "POST"])
@login_required
def vision_page():
    answer = ""
    question = ""
    preview_data_url = ""
    llm_models = []
    selected_model = ""

    try:
        llm_models = get_vision_models()
        selected_model = choose_default_model(llm_models)

        if request.method == "POST":
            question = request.form.get("question", "").strip()
            selected_model = request.form.get("llm", "").strip() or selected_model
            uploaded_file = request.files.get("image_file")

            if not uploaded_file or not uploaded_file.filename:
                flash("Upload an image file.", "warning")
            elif not question:
                flash("Enter a question.", "warning")
            elif not selected_model:
                flash("No supported generation models were found for this connection.", "error")
            else:
                image_bytes = uploaded_file.read()
                mime_type = uploaded_file.mimetype or "image/png"
                encoded_image = base64.b64encode(image_bytes).decode("utf-8")
                preview_data_url = "data:{mime};base64,{body}".format(
                    mime=mime_type,
                    body=encoded_image,
                )
                raw_response = answer_query_on_image(question, selected_model, encoded_image)
                payload = json.loads(raw_response or "{}")
                answer = payload.get("text", "")
    except mysql.connector.Error as error:
        flash(str(error), "error")
    except json.JSONDecodeError:
        flash("The vision response could not be parsed.", "error")

    return render_dashboard(
        "vision.html",
        page_title="HWVision",
        llm_models=llm_models,
        selected_model=selected_model,
        question=question,
        answer=answer,
        preview_data_url=preview_data_url,
    )


def get_heatwave_performance_table_counts():
    rows = run_sql_with_columns(
        """
        select 'booking' as table_name, count(*) as row_count from airportdb.booking
        union all
        select 'flight' as table_name, count(*) as row_count from airportdb.flight
        union all
        select 'airline' as table_name, count(*) as row_count from airportdb.airline
        union all
        select 'airport_geo' as table_name, count(*) as row_count from airportdb.airport_geo
        """,
        autocommit=True,
    )
    return [
        {"table_name": row[0], "row_count": row[1]}
        for row in rows["rows"]
    ]


def get_session_autocommit_value():
    rows = run_sql(
        "select @@session.autocommit as autocommit_value",
        autocommit=True,
    )
    return rows[0][0] if rows else None


def explain_heatwave_performance_query(sql_text):
    raw_plan = run_sql_with_columns("EXPLAIN " + sql_text, autocommit=True)
    formatted_rows = []
    for row in raw_plan["rows"]:
        formatted_row = []
        for value in row:
            cell_value = value
            is_multiline = False
            if isinstance(value, str):
                try:
                    parsed_json = json.loads(value)
                except json.JSONDecodeError:
                    parsed_json = None
                if isinstance(parsed_json, (dict, list)):
                    cell_value = json.dumps(parsed_json, indent=2)
                    is_multiline = True
            formatted_row.append({"value": cell_value, "is_multiline": is_multiline})
        formatted_rows.append(formatted_row)
    return {
        "columns": raw_plan["columns"],
        "rows": formatted_rows,
    }


def execute_heatwave_performance_query(sql_text):
    cnx = None
    cursor = None
    try:
        cnx = mysql_connection(autocommit=True)
        cursor = cnx.cursor()
        cursor.execute("select @@session.autocommit as autocommit_value")
        autocommit_value = cursor.fetchone()[0]
        started_at = datetime.now(timezone.utc)
        started_counter = time.perf_counter()
        cursor.execute(sql_text)
        rows = [list(row) for row in cursor.fetchall()]
        finished_counter = time.perf_counter()
        finished_at = datetime.now(timezone.utc)
        elapsed_seconds = (finished_at - started_at).total_seconds()
        return {
            "columns": list(cursor.column_names or ()),
            "rows": rows,
        }, {
            "started_at": started_at,
            "finished_at": finished_at,
            "elapsed_seconds": elapsed_seconds,
            "elapsed_perf_seconds": finished_counter - started_counter,
        }, autocommit_value
    finally:
        if cursor:
            cursor.close()
        if cnx and cnx.is_connected():
            cnx.close()


@app.route("/heatwave-performance", methods=["GET", "POST"])
@login_required
def heatwave_performance_page():
    active_tab = request.values.get("tab", "innodb").strip().lower()
    if active_tab not in HEATWAVE_PERFORMANCE_SQL:
        active_tab = "innodb"
    sql_text = HEATWAVE_PERFORMANCE_SQL[active_tab]

    try:
        if not airportdb_exists(autocommit=True):
            flash("The HeatWave Performance page is available only when schema `airportdb` exists.", "warning")
            return redirect(url_for("home"))

        table_counts = get_heatwave_performance_table_counts()
        result_table = {"columns": [], "rows": []}
        execution_timing = None
        query_executed = False
        autocommit_value = get_session_autocommit_value()
        explain_plan = {"columns": [], "rows": []}

        if request.method == "POST":
            sql_text = request.form.get("sql_text", "").strip() or sql_text
            result_table, execution_timing, autocommit_value = execute_heatwave_performance_query(sql_text)
            query_executed = True
            explain_plan = explain_heatwave_performance_query(sql_text)
    except mysql.connector.Error as error:
        flash(str(error), "error")
        table_counts = []
        explain_plan = {"columns": [], "rows": []}
        result_table = {"columns": [], "rows": []}
        execution_timing = None
        query_executed = request.method == "POST"
        autocommit_value = None

    return render_dashboard(
        "heatwave_performance.html",
        page_title="HeatWave Performance",
        active_tab=active_tab,
        active_tab_label="InnoDB" if active_tab == "innodb" else "RAPID engine",
        tabs=[
            {"id": "innodb", "label": "InnoDB"},
            {"id": "rapid", "label": "RAPID engine"},
        ],
        sql_text=sql_text,
        explain_plan=explain_plan,
        table_counts=table_counts,
        result_table=result_table,
        execution_timing=execution_timing,
        query_executed=query_executed,
        autocommit_value=autocommit_value,
    )


if __name__ == "__main__":
    host = os.environ.get("APP_ADDRESS", "0.0.0.0")
    port = _normalized_port(os.environ.get("APP_PORT", "443"))
    cert_file = os.environ.get("APP_SSL_CERT_FILE", "")
    key_file = os.environ.get("APP_SSL_KEY_FILE", "")
    ssl_context = (cert_file, key_file) if cert_file and key_file else None
    app.run(host=host, port=port, debug=False, ssl_context=ssl_context)
