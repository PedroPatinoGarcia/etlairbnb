# testing.py
import os
import subprocess
import sys

def run_tests(test_script_name):
    """
    Ejecuta un archivo de pruebas de Python ubicado en la carpeta 'src/tests' utilizando pytest.

    Args:
        test_script_name (str): Nombre del archivo de pruebas a ejecutar.
    """
    test_script_path = os.path.join("src", "tests", test_script_name)
    print(f"Ejecutando pruebas {test_script_path}...")
    result = subprocess.run([sys.executable, "-m", "pytest", test_script_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error ejecutando pruebas {test_script_path}:")
        print(result.stderr)
        exit(1)
    else:
        print(f"Salida de pruebas {test_script_path}:")
        print(result.stdout)

if __name__ == "__main__":
    test_scripts = ["test_raw.py", "test_staging.py", "test_business.py"]
    for test_script in test_scripts:
        run_tests(test_script)
