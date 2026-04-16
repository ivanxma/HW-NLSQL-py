# HeatWave Demo

This application now uses Flask instead of Streamlit while preserving the same core logic:

- login with a saved MySQL connection profile
- maintain `nlsql.configdb`
- run HeatWave `sys.NL_SQL`
- run HeatWave visual prompts with `sys.ML_GENERATE`

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

1. Open the app.
2. Create or select a saved connection profile on the login page.
3. Log in with the database user and password for that profile.
4. Open `Admin > Setup configdb` and choose the schemas NL_SQL should use.
5. Use `HeatWave > HWnlsql` or `HeatWave > HWVision`.

Profiles are stored in `profiles.json`. Only non-secret connection details are stored there.
