import os
import subprocess
import sys

def run_tests(test_name):
    test_path = os.path.join("tests", test_name)
    print("Ejecutando tests...")
    result = subprocess.run(["pytest", "tests", test_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error ejecutando {test_path}:")
        print(result.stderr)
        exit(1)
    else:
        print("Todos los tests han pasado correctamente.")
        print(result.stdout)

if __name__ == "__main__":
        tests = ["test_spark_session.py", "test_raw.py", "test_staging.py", "test_business.py", "test_lambda_function.py"]
    
        for test in tests:
            run_tests(test)