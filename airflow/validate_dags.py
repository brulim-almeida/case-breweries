#!/usr/bin/env python3
"""
Script to validate Airflow DAGs without needing a full Airflow environment.

This script checks:
- DAG syntax and import errors
- Task dependencies
- Configuration validity
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def validate_dag(dag_file_path: str) -> bool:
    """
    Validate a DAG file for syntax and configuration errors.
    
    Args:
        dag_file_path: Path to the DAG file
        
    Returns:
        bool: True if validation succeeds, False otherwise
    """
    print(f"\n{'=' * 80}")
    print(f"VALIDATING DAG: {dag_file_path}")
    print(f"{'=' * 80}\n")
    
    try:
        # Import the DAG file
        import importlib.util
        spec = importlib.util.spec_from_file_location("dag_module", dag_file_path)
        dag_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(dag_module)
        
        # Check if DAG object exists
        if hasattr(dag_module, 'dag'):
            dag = dag_module.dag
            print(f"✅ DAG found: {dag.dag_id}")
            print(f"   Description: {dag.description}")
            print(f"   Schedule: {dag.schedule_interval}")
            print(f"   Tags: {', '.join(dag.tags)}")
            print(f"\n📋 Tasks ({len(dag.tasks)}):")
            
            for task in dag.tasks:
                print(f"   • {task.task_id}")
                if hasattr(task, 'retries'):
                    print(f"     - Retries: {task.retries}")
                if hasattr(task, 'execution_timeout'):
                    print(f"     - Timeout: {task.execution_timeout}")
            
            print(f"\n🔗 Task Dependencies:")
            for task in dag.tasks:
                if task.upstream_task_ids:
                    upstream = ', '.join(task.upstream_task_ids)
                    print(f"   {upstream} → {task.task_id}")
            
            print(f"\n✅ DAG validation successful!")
            return True
        else:
            print("❌ No DAG object found in file")
            return False
            
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        return False
    except SyntaxError as e:
        print(f"❌ Syntax Error: {e}")
        return False
    except Exception as e:
        print(f"❌ Validation Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main validation function."""
    dags_dir = Path(__file__).parent / "dags"
    
    if not dags_dir.exists():
        print(f"❌ DAGs directory not found: {dags_dir}")
        sys.exit(1)
    
    # Find all Python files in dags directory
    dag_files = list(dags_dir.glob("*.py"))
    
    if not dag_files:
        print(f"❌ No DAG files found in {dags_dir}")
        sys.exit(1)
    
    print(f"\nFound {len(dag_files)} DAG file(s) to validate:\n")
    
    results = []
    for dag_file in dag_files:
        if dag_file.name.startswith('_'):
            continue  # Skip private files
        
        success = validate_dag(str(dag_file))
        results.append((dag_file.name, success))
    
    # Summary
    print(f"\n{'=' * 80}")
    print("VALIDATION SUMMARY")
    print(f"{'=' * 80}\n")
    
    success_count = sum(1 for _, success in results if success)
    total_count = len(results)
    
    for dag_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{status}: {dag_name}")
    
    print(f"\nTotal: {success_count}/{total_count} DAGs validated successfully")
    
    if success_count < total_count:
        sys.exit(1)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
