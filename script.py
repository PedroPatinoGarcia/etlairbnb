import subprocess

def run_script(script_name):
    print(f"Ejecutando {script_name}...")
    result = subprocess.run(["python", script_name], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error ejecutando {script_name}:")
        print(result.stderr)
        exit(1)
    else:
        print(f"Salida de {script_name}:")
        print(result.stdout)


if __name__ == "__main__":
    scripts = ["spark_session.py", "raw.py", "staging.py", "business.py", "lambda_function.py", "lambda_save_function.py", "moto_s3.py"]
    
    for script in scripts:
        run_script(script)