# HeatWave Demo

This application now uses Flask instead of Streamlit while preserving the same core logic:

- login with a saved MySQL connection profile
- maintain `nlsql.configdb`
- run HeatWave `sys.NL_SQL`
- run HeatWave visual prompts with `sys.ML_GENERATE`
- compare InnoDB and RAPID execution on `airportdb`

## Install

Install `git` first, depending on your operating system:

### Ubuntu

```bash
sudo apt update
sudo apt install -y git python3 python3-pip python3-venv python3-full
```

### Oracle Linux 8

```bash
sudo dnf install -y git python3 python3-pip python3-setuptools python3-wheel
```

### Oracle Linux 9

```bash
sudo dnf install -y git python3 python3-pip python3-setuptools python3-pip-wheel
```

Clone the repository and enter the project directory:

```bash
git clone https://github.com/ivanxma/HW-NLSQL-py
cd HW-NLSQL-py
```

Run the setup script to install the OS packages and create a local Python virtual environment in `.venv`:

```bash
chmod +x setup.sh start_https.sh
sudo ./setup.sh
```

The setup script:

- detects whether the host is Ubuntu, Oracle Linux 8, or Oracle Linux 9 and runs the matching package installation
- installs `python3`, `pip`, and `openssl`
- creates `.venv` in the project directory
- installs the Python packages from `requirements.txt` into that virtual environment
- creates and starts `hw-nlsql-https.service` on port `443` when `systemd` is available
- on Ubuntu, opens `443/tcp` in `ufw` when active, or adds and persists an `iptables` allow rule with `iptables-persistent`
- on Oracle Linux, opens `443/tcp` in `firewalld` when active
- keeps SELinux aligned for HTTPS, and adjusts the SELinux port label if you run on a non-default HTTP/S port
- falls back to direct startup instructions only when `systemd` is not available

## Run

If the service was created, manage the HTTPS service with `systemctl`:

```bash
sudo systemctl status hw-nlsql-https.service
sudo systemctl restart hw-nlsql-https.service
sudo journalctl -u hw-nlsql-https.service -f
```

The launcher still generates a self-signed certificate under `.certs/` if one does not already exist.

If `systemd` is not available, start the app directly:

```bash
/bin/bash ./start_https.sh
```

## AirportDB Loader

The repository also includes helper scripts to install MySQL Shell Innovation and load the MySQL `airportdb` sample dump used by the HeatWave quickstart.

OS-specific MySQL Shell installation scripts:

- `OL8/install_mysql_shell_innovation.sh`
- `OL9/install_mysql_shell_innovation.sh`
- `ubuntu/install_mysql_shell_innovation.sh`

The wrapper script [load_airportdb.sh](load_airportdb.sh) does the following:

- detects the Linux distribution
- installs `mysqlsh` with the matching script if `mysqlsh` is not already present
- downloads `airport-db.tar.gz`
- extracts it with `tar xvzf airport-db.tar.gz`
- runs `util.loadDump("airport-db", {threads: 16, deferTableIndexes: "all", ignoreVersion: true})`
- optionally calls `sys.heatwave_load` for schema `airportdb`

Usage:

```bash
chmod +x load_airportdb.sh
./load_airportdb.sh <mysql_user> <mysql_host_or_ip>
```

Optional environment variables:

```bash
LOAD_THREADS=16
HEATWAVE_LOAD=1
AIRPORTDB_ARCHIVE_PATH=/path/to/airport-db.tar.gz
AIRPORTDB_EXTRACT_DIR=/path/to/airport-db
MYSQLSH_BIN=/usr/bin/mysqlsh
```

Notes:

- `mysqlsh` will prompt for the password interactively.
- The script expects the extracted dump directory to be `airport-db` under the project root unless `AIRPORTDB_EXTRACT_DIR` is set.
- `HEATWAVE_LOAD=0 ./load_airportdb.sh <mysql_user> <mysql_host_or_ip>` skips the final HeatWave load call.

## Kubernetes

Deploy to Kubernetes in a dedicated `nlsql` namespace:

1. Create the namespace:

```bash
kubectl create ns nlsql
```

2. Apply the manifest:

```bash
kubectl apply -n nlsql -f k8s/hw-nlsql.yaml
```

3. Check the deployment and service:

```bash
kubectl get deploy,po,svc -n nlsql
```

4. If the pod is still starting or restarting, check the logs:

```bash
kubectl logs -n nlsql deployment/hw-nlsql
```

5. If you update the manifest and need to restart the pod:

```bash
kubectl rollout restart deployment/hw-nlsql -n nlsql
kubectl rollout status deployment/hw-nlsql -n nlsql
```

The Kubernetes manifest creates the `Secret`, `ConfigMap`, `Deployment`, and `LoadBalancer` `Service` in namespace `nlsql`.

## Configure

### OCI IAM Setup

If this application runs on OCI Compute and needs to call OCI services by using instance principal authentication, create a Dynamic Group and the required IAM policies first.

#### Create Dynamic Group `hw-genai-dg`

In the OCI Console, open `Identity & Security > Domains > Default domain > Dynamic groups`, then create a Dynamic Group with:

- Name: `hw-genai-dg`
- Description: `Dynamic group for HeatWave GenAI application instances`
- Matching rule: `ANY {instance.compartment.id = 'the ocid of the compartment', resource.compartment.id = 'the ocid of the compartment'}`

Replace `the ocid of the compartment` with the actual compartment OCID.

#### Create Policies

Create a policy in the tenancy or in the target parent compartment and attach the following statements for compartment `hw-demo-compartment`:

```text
allow dynamic-group hw-genai-dg to read volume-family in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to read instance-family in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to read objectstorage-namespaces in tenancy
allow dynamic-group hw-genai-dg to read buckets in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to manage objects in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to manage object-family in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to read secret-bundles in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to use generative-ai-chat in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to use generative-ai-text-generation in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to use generative-ai-text-summarization in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to use generative-ai-text-embedding in compartment hw-demo-compartment
allow dynamic-group hw-genai-dg to use generative-ai-model in compartment hw-demo-compartment
```

If you use different OCI names, replace `hw-genai-dg` and `hw-demo-compartment` with your actual Dynamic Group and compartment names. After creating the Dynamic Group and policies, allow a few minutes for IAM propagation before testing the application.

1. Open the app.
2. Create or select a saved connection profile on the login page.
3. Log in with the database user and password for that profile.
4. Open `Admin > Setup configdb` and choose the schemas NL_SQL should use.
5. Use `HeatWave > NL_SQL`, `HeatWave > HWVision`, or `HeatWave > HeatWave Performance`.

Profiles are stored in `profiles.json`. Only non-secret connection details are stored there.

## Page Notes

### NL_SQL

- The `Submit` button is disabled while a request is running.
- The pointer changes to a wait cursor until the response returns.
- The page shows the full `CALL sys.NL_SQL(...)` syntax for the submitted request, followed by the generated SQL and result sets.

### HeatWave Performance

- The menu item appears only when schema `airportdb` exists.
- The page uses two tabs: `InnoDB` and `RAPID engine`.
- Opening or switching tabs only loads the SQL text and metadata. It does not execute the SQL.
- Clicking `Execute` runs the current editable SQL, switches the cursor to wait, and shows `Status : Running` while the request is in flight.
- The page forces the session to `autocommit=1` for this workload and displays the current session autocommit value.
- The page shows row counts for `airportdb.booking`, `airportdb.flight`, `airportdb.airline`, and `airportdb.airport_geo`.
- The `EXPLAIN` output is produced only after `Execute` is clicked and JSON plan values are formatted for readability.
