import os
import pytest
from unittest.mock import patch, MagicMock
from subprocess import CompletedProcess
from script import run_script

# Mock para subprocess.run que simula el comportamiento de ejecutar un script
@patch('subprocess.run')
def test_run_script_successful(mock_subprocess_run):
    # Configurar el mock para que devuelva un proceso completado exitoso
    mock_subprocess_run.return_value = CompletedProcess(args=['python', 'src/spark_session.py'], returncode=0, stdout='Output', stderr=None)
    
    # Ejecutar el script
    run_script("spark_session.py")
    
    # Verificar que subprocess.run fue llamado correctamente
    mock_subprocess_run.assert_called_once_with(["python", os.path.join("src", "spark_session.py")], capture_output=True, text=True)

@patch('subprocess.run')
def test_run_script_failure(mock_subprocess_run):
    # Configurar el mock para que devuelva un proceso completado con error
    mock_subprocess_run.return_value = CompletedProcess(args=['python', 'src/spark_session.py'], returncode=1, stdout=None, stderr='Error')
    
    # Ejecutar el script
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        run_script("spark_session.py")
    
    # Verificar que subprocess.run fue llamado correctamente
    mock_subprocess_run.assert_called_once_with(["python", os.path.join("src", "spark_session.py")], capture_output=True, text=True)
    
    # Verificar que el script salió con código de error
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 1
