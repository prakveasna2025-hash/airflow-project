# 🌬️ Apache Airflow Project

<div align="center">

![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-Cache-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![License](https://img.shields.io/badge/License-Apache%202.0-green?style=for-the-badge)

**🚀 A complete, Docker-based Apache Airflow project for building, scheduling, and monitoring production-grade data pipelines as code.**

[📖 Documentation](https://airflow.apache.org/docs/) • [🐛 Report Bug](https://github.com/prakveasna2025-hash/airflow-project/issues) • [💡 Request Feature](https://github.com/prakveasna2025-hash/airflow-project/issues)

</div>

---

## 📌 Table of Contents

- [🌟 Overview](#-overview)
- [🏗️ Architecture & Core Concepts](#️-architecture--core-concepts)
- [📂 Project Structure](#-project-structure)
- [🚀 Key DAG Examples](#-key-dag-examples)
- [⚙️ Getting Started](#️-getting-started)
- [🐳 Docker Services](#-docker-services)
- [📝 Documenting DAGs (Best Practice)](#-documenting-dags-best-practice)
- [🛠️ Development Workflow](#️-development-workflow)
- [🧪 Testing & Debugging](#-testing--debugging)
- [❓ Troubleshooting](#-troubleshooting)
- [📚 Important Resources](#-important-resources)
- [🤝 Contributing](#-contributing)
- [📄 License](#-license)

---

## 🌟 Overview

This repository contains a **complete, Docker-based Apache Airflow project** designed to help you **develop, schedule, and monitor workflows as code**. It includes a collection of progressively advanced DAG examples covering everything from basic task orchestration to dynamic scheduling, incremental loads, and **asset-aware data pipelines**.

> 💡 **Apache Airflow** is an open-source platform that allows you to programmatically author, schedule, and monitor workflows. Workflows are defined as **Directed Acyclic Graphs (DAGs)** of tasks, making it easy to manage complex data pipelines with clear dependencies and built-in retry logic.

### ✨ Key Features

- 🐳 **Fully containerized** — runs anywhere Docker runs
- 🔁 **CeleryExecutor** for distributed, scalable task execution
- 🐍 **Modern TaskFlow API** using `@dag` and `@task` decorators
- 📦 **Asset-aware pipelines** (formerly Datasets)
- 📈 **Incremental load** patterns for efficient ETL/ELT
- 🔧 **Extensible** via `src/` custom packages and `pyproject.toml`

---

## 🏗️ Architecture & Core Concepts

Before diving into the project, it's important to understand the fundamental building blocks of Airflow reflected in this repository:

| Icon | Concept | Description |
|------|---------|-------------|
| 🕸️ | **DAG** (Directed Acyclic Graph) | The core concept of Airflow. Collects tasks together, organized with dependencies and relationships to define execution order. |
| ✅ | **Tasks & Operators** | A task is a single unit of work. **Operators** are templates (e.g., `PythonOperator`, `BashOperator`); **Sensors** wait for external events. |
| 🐍 | **TaskFlow API** | Modern `@dag` and `@task` decorators let you define DAGs as native Python functions — cleaner, more intuitive pipelines. |
| ⏰ | **Scheduler** | Triggers scheduled workflows and submits tasks to the executor. |
| ⚙️ | **Executor** | Runs tasks. This project uses **CeleryExecutor** (Redis + PostgreSQL) for distributed execution. |
| 🖥️ | **Webserver** | The UI to inspect, trigger, and debug DAGs and tasks. |
| 📦 | **Assets** (Datasets) | Data-aware scheduling — DAGs triggered by asset production rather than time. |

---

## 📂 Project Structure

The repository follows a clean, standard Airflow project layout, separating orchestration logic from configuration and dependencies:
airflow-project/
│
├── 📁 dags/ # All DAG definition files
│ ├── 🐍 1_first_dag.py # Basic DAG with TaskFlow API
│ ├── 🐍 2_dag_versioning.py # DAG versioning concepts
│ ├── 🐍 3_oparator.py # Operator usage examples
│ ├── 🐍 4_Xcoms.py # Cross-task communication with XComs
│ ├── 🐍 10_schedule_dalta.py # Dynamic scheduling
│ ├── 🐍 11_incremental_load.py # Incremental data loading
│ ├── 🐍 12_special_load.py # Special loading patterns
│ ├── 🐍 13_assets.py # Asset-aware data pipelines
│ └── 🐍 14_asset_dependent.py # Asset dependencies
│
├── 📁 src/airflow_project/ # Custom Python packages/utilities
│ └── 🐍 init.py
│
├── 🐳 docker-compose.yaml # Local Airflow env (CeleryExecutor + Redis + Postgres)
├── 📦 pyproject.toml # Python project dependencies & metadata
├── 🔒 uv.lock # Locked dependency versions (for uv)
├── 🐍 .python-version # Python version specification
└── 📖 README.md # This file

text

---

## 🚀 Key DAG Examples

This repository contains progressively advanced DAG examples. Here are the highlights:

### 1️⃣ Basic DAG — `1_first_dag.py`
> 🎯 Demonstrates the modern **TaskFlow API** using `@dag` and `@task` decorators.

Defines three simple Python tasks and uses the bitshift operator (`>>`) to set a linear dependency chain.

```python
@dag(dag_id="first_dag", schedule="@daily")
def first_dag():
    @task
    def extract(): ...

    @task
    def transform(data): ...

    @task
    def load(data): ...

    load(transform(extract()))
2️⃣ DAG Versioning — 2_dag_versioning.py
🎯 Explores how to manage DAG versions safely over time.

3️⃣ Operators — 3_oparator.py
🎯 Shows various operator types (PythonOperator, BashOperator, etc.) and when to use each.

4️⃣ XComs — 4_Xcoms.py
🎯 Demonstrates cross-task communication using XComs (small metadata passing between tasks).

🔟 Dynamic Scheduling — 10_schedule_dalta.py
🎯 Explores dynamic scheduling patterns for pipelines that adapt their run cadence based on external conditions or data availability.

1️⃣1️⃣ Incremental & Special Loads — 11_incremental_load.py, 12_special_load.py
🎯 Best practices for loading data incrementally (only new records) and handling special load scenarios — critical for efficient ETL/ELT pipelines.

1️⃣3️⃣ Asset-Aware Pipelines — 13_assets.py, 14_asset_dependent.py
🎯 Demonstrates the Asset (formerly Dataset) feature for data-aware scheduling.

In 13_assets.py, a task declares an outlet asset (my_asset) and writes a file to a specific URI. Then 14_asset_dependent.py consumes that asset, creating a data-aware dependency between DAGs — no more schedule_interval guessing!

⚙️ Getting Started
📋 Prerequisites
Requirement	Version	Notes
🐳 Docker	Latest	Required
🐳 Docker Compose	v2+	Required
📦 uv (optional)	Latest	For Python dependency management
🚀 Steps to Run
1️⃣ Clone the repository

bash
git clone https://github.com/prakveasna2025-hash/airflow-project.git
cd airflow-project
2️⃣ Configure environment (optional)

Create a .env file to customize the FERNET_KEY and other environment variables:

bash
echo "AIRFLOW_UID=$(id -u)" > .env
3️⃣ Spin up the stack

bash
docker compose up -d
ℹ️ This pulls the official apache/airflow:3.3.2 image and starts the scheduler, webserver, worker, redis, and postgres services.

4️⃣ Open the Airflow UI

🌐 Navigate to → http://localhost:8080

Field	Value
👤 Username	airflow
🔑 Password	airflow (default — change in production!)
5️⃣ Enable your DAGs

🎛️ In the UI, toggle each DAG ON to start scheduling. Watch them run in real time!

🐳 Docker Services
The docker-compose.yaml orchestrates the following services:

Icon	Service	Port	Purpose
🖥️	airflow-webserver	8080	Web UI & REST API
⏰	airflow-scheduler	—	Monitors DAGs, triggers tasks
👷	airflow-worker	—	Executes tasks (Celery)
🗄️	postgres	5432	Metadata database
⚡	redis	6379	Celery broker
🔍 Useful Commands
bash
# Start all services
docker compose up -d

# View logs for a specific service
docker compose logs -f airflow-scheduler

# Stop all services
docker compose down

# Restart just the scheduler
docker compose restart airflow-scheduler

# Access a shell inside the scheduler
docker compose exec airflow-scheduler bash
📝 Documenting DAGs (Best Practice)
📚 A crucial best practice in Airflow: document your DAGs directly in the code.

Airflow lets you attach Markdown documentation to a DAG via the doc_md parameter. This documentation renders directly in the Airflow UI, giving your team context without leaving the browser. 🎨

✍️ Example
python
@dag(
    dag_id="my_documented_dag",
    doc_md=__doc__,  # Uses the module docstring
    # ... other params
)
def my_dag():
    """
    # 📊 My Documented DAG

    This DAG extracts data from **source X** and loads it into **destination Y**.

    ## Steps
    1. Extract
    2. Transform
    3. Load
    """
You can also pass a string or a file path to a Markdown file directly to doc_md — great for keeping DAGs lean while still shipping rich docs. 📁

🛠️ Development Workflow
🆕 Adding a new DAG → Drop your Python file inside dags/. The scheduler will automatically parse it (usually within ~30s).

📦 Managing dependencies → Add Python dependencies to pyproject.toml. For container-level dependencies, create a custom Dockerfile and update the build directive in docker-compose.yaml.

🧩 Project organization → Keep orchestration logic (DAGs) separate from business logic (SQL, complex Python modules) by using src/ or an include/ folder for supporting files.

🔒 Locking versions → Use uv lock to keep reproducible dependency versions.

🎯 Recommended Local Dev Loop
bash
# 1. Edit a DAG
vim dags/my_new_dag.py

# 2. Validate it compiles
docker compose exec airflow-scheduler python -m py_compile /opt/airflow/dags/my_new_dag.py

# 3. Trigger a manual run
docker compose exec airflow-scheduler airflow dags trigger my_new_dag

# 4. Watch logs
docker compose logs -f airflow-scheduler
🧪 Testing & Debugging
Tool	Purpose
🧪 airflow dags test <dag_id>	Run a full DAG locally with a specific logical date
🧪 airflow tasks test <dag_id> <task_id>	Run a single task in isolation
🔍 Airflow UI → Grid View	Visualize task states across runs
📜 Airflow UI → Logs	Inspect task-level logs
🕵️ airflow dags list-import-errors	Find broken DAGs quickly
❓ Troubleshooting
<details> <summary>🔥 <b>Webserver shows 503 or won't load</b></summary>
Wait ~60 seconds after docker compose up — the DB needs to initialize.

Check logs: docker compose logs airflow-webserver

Ensure port 8080 isn't already in use.

</details><details> <summary>🚫 <b>DAG not appearing in the UI</b></summary>
Verify the file is inside dags/ and ends with .py.

Check for import errors: airflow dags list-import-errors

Restart the scheduler: docker compose restart airflow-scheduler

</details><details> <summary>🔐 <b>Permission errors on `dags/`, `logs/`, `plugins/`</b></summary>
Set AIRFLOW_UID in .env to your local user ID:

bash
echo "AIRFLOW_UID=$(id -u)" >> .env
Then: docker compose down && docker compose up -d

</details><details> <summary>🐘 <b>Postgres connection refused</b></summary>
The DB may still be starting. Wait 30–60s and retry.

Check health: docker compose ps

</details>
📚 Important Resources
🔗 Resource	Description
📖 Apache Airflow Docs	Official documentation
🎓 Astronomer: Managing Code	Best practices for organizing Airflow code
🏗️ Airflow Project Structure	Recommended layout
🐍 TaskFlow API Guide	Modern Pythonic DAG authoring
📦 Assets & Data-Aware Scheduling	Asset-driven pipelines
🐳 Docker Compose Reference	Official Airflow compose setup
🤝 Contributing
Contributions, issues, and feature requests are welcome! 🎉

🍴 Fork the repository

🌿 Create a feature branch: git checkout -b feature/amazing-feature

💾 Commit your changes: git commit -m "feat: add amazing feature"

🚀 Push: git push origin feature/amazing-feature

🔃 Open a Pull Request

💡 Please ensure new DAGs include a doc_md string and follow the existing naming conventions.

📄 License
This project is based on Apache Airflow, which is licensed under the Apache License 2.0.

text
Copyright 2016-2025 The Apache Software Foundation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
<div align="center">
🌬️ Happy Orchestrating! 🌬️
Made with ❤️ using Apache Airflow

⭐ If this project helped you, please consider giving it a star! ⭐

</div> ```
📥 How to Use This File
Create the file at the root of your repo:

bash
touch README.md
Paste the entire content above into README.md.

Commit and push:

bash
git add README.md
git commit -m "docs: add comprehensive README with icons"
git push origin main
🎨 What's Included
Section	Icons Used
🌬️ Header	Airflow logo emoji, shields/badges
🌟 Overview	Feature bullets with emojis
🏗️ Architecture	Concept table with icons
📂 Structure	File-tree with inline emoji
🚀 DAG Examples	Numbered emojis (1️⃣–1️⃣4️⃣)
⚙️ Getting Started	Step-by-step with numbered icons
🐳 Docker Services	Port/service table
📝 Doc Best Practice	Markdown DAG doc example
🛠️ Development	Workflow bullets
🧪 Testing	Tool table
❓ Troubleshooting	Collapsible <details> blocks
📚 Resources	Link table
🤝 Contributing	Step list
📄 License	Full Apache 2.0 text
Footer	Center-aligned call-to-action
