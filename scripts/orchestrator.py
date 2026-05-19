"""
Module: orchestrator.py
Author: Senior Data Engineering Mentor (Amazon RBS Portfolio Project)
Description: Production pipeline orchestrator simulating Apache Airflow DAG behaviors.
             Manages task dependency execution, status logging, and automatic file archiving.
"""

import os
import subprocess
import sys
import glob
import shutil
from datetime import datetime

# Define absolute paths relative to project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
INGRESS_DIR = os.path.join(BASE_DIR, "data", "ingress")
ARCHIVE_DIR = os.path.join(BASE_DIR, "data", "archive")

def get_latest_ingress_file():
    """Identifies the file that was just processed so we can archive it."""
    search_path = os.path.join(INGRESS_DIR, "catalog_transactions_*.csv")
    files = glob.glob(search_path)
    if not files:
        return None
    return max(files, key=os.path.getctime)

def archive_processed_file():
    """Task 3: Moves the processed CSV file from ingress to archive to keep pipeline clean."""
    print("\n" + "="*60)
    print(f" 🟢 [DAG TASK START]: Archive_Processed_Raw_Payload | {datetime.now()}")
    print("="*60)
    
    file_to_archive = get_latest_ingress_file()
    if not file_to_archive:
        print("⚠️ No file found in ingress to archive.")
        return False
        
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    filename = os.path.basename(file_to_archive)
    target_path = os.path.join(ARCHIVE_DIR, filename)
    
    try:
        shutil.move(file_to_archive, target_path)
        print(f"📦 Successfully moved raw landing file to Archive Layer:")
        print(f"   From: {file_to_archive}")
        print(f"   To:   {target_path}")
        print("\n" + "="*60)
        print(f" ✅ [DAG TASK SUCCESS]: Archive_Processed_Raw_Payload completed cleanly.")
        print("="*60)
        return True
    except Exception as e:
        print(f" ❌ [DAG TASK CRITICAL FAILURE]: Failed to move file to archive. Error: {e}")
        print("="*60)
        return False

def run_task(task_name: str, script_name: str) -> bool:
    """Simulates an Airflow Task Instance executing a sub-process."""
    print("\n" + "="*60)
    print(f" 🟢 [DAG TASK START]: {task_name} | {datetime.now()}")
    print("="*60)
    
    script_path = os.path.join(SCRIPTS_DIR, script_name)
    python_executable = sys.executable

    try:
        subprocess.run(
            [python_executable, "-u", script_path],
            check=True,
            capture_output=False,
            text=True
        )
        print("\n" + "="*60)
        print(f" ✅ [DAG TASK SUCCESS]: {task_name} completed cleanly.")
        print("="*60)
        return True
    except subprocess.CalledProcessError as e:
        print("\n" + "="*60)
        print(f" ❌ [DAG TASK CRITICAL FAILURE]: {task_name} encountered an exception.")
        print("="*60)
        return False

def execute_pipeline_dag():
    """Defines the DAG sequence: Task_1 >> Task_2 >> Task_3"""
    print(f"\n🚀 [DAG TRIGGER]: Initializing pipeline run sequence at {datetime.now()}")
    
    # Task 1: Ingestion
    if not run_task("Extract_And_Inject_Raw_Payload", "data_generator.py"):
        print("🚨 [DAG ABORTED]: Pipeline halted due to Task 1 failure.")
        return

    # Task 2: Transformation
    if not run_task("PySpark_Enforce_Schema_And_Isolate_DLQ", "transform.py"):
        print("🚨 [DAG ABORTED]: Pipeline halted due to Task 2 failure.")
        return

    # Task 3: Archiving
    if archive_processed_file():
        print(f"\n🏁 [DAG SUCCESS]: End-to-end pipeline run finalized successfully at {datetime.now()}")
    else:
        print(f"\n🚨 [DAG FAILED]: Pipeline finished but archiving task failed.")

if __name__ == "__main__":
    execute_pipeline_dag()