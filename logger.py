# logger.py
import logging
import sys

# Crear logger principal
logger = logging.getLogger("aduanas-dashboard")
logger.setLevel(logging.DEBUG)  # Podés cambiar a INFO en producción

# Formato de logs
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Handler para consola (stdout)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)

# Evitar duplicados si se importa varias veces
if not logger.handlers:
    logger.addHandler(console_handler)
