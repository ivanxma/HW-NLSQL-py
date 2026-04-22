import mysql.connector
from flask import flash, redirect, request, url_for

from app_context import (
    ASKME_CONFIG_ROWS,
    app,
    fetch_askme_config,
    login_required,
    render_dashboard,
    save_askme_config,
    setup_askme_db,
)


@app.route("/setup-askme", methods=["GET", "POST"])
@login_required
def setup_askme_page():
    config_values = {}
    try:
        setup_askme_db()
        if request.method == "POST":
            submitted = {
                env_var: request.form.get(env_var, "")
                for env_var, _default_value in ASKME_CONFIG_ROWS
            }
            save_askme_config(submitted)
            flash("Updated askme.config.", "success")
            return redirect(url_for("setup_askme_page"))
        config_values = fetch_askme_config()
    except mysql.connector.Error as error:
        flash(str(error), "error")

    config_items = []
    for env_var, default_value in ASKME_CONFIG_ROWS:
        config_items.append(
            {
                "env_var": env_var,
                "env_value": config_values.get(env_var, default_value),
            }
        )

    return render_dashboard(
        "setup_askme.html",
        page_title="Setup Askme",
        config_items=config_items,
    )
