import os
import subprocess
import sys

def run_script(script_name):
    script_path = os.path.join("src", script_name)
    print(f"Ejecutando {script_path}...")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error ejecutando {script_path}:")
        print(result.stderr)
        exit(1)
    else:
        print(f"Salida de {script_path}:")
        print(result.stdout)


if __name__ == "__main__":
    scripts = ["spark_session.py", "raw.py", "staging.py", "business.py", "lambda_function.py"]
    
    for script in scripts:
        run_script(script)