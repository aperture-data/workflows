import importlib
import pathlib

from shared import logger

# Get current directory (tools/)
package_dir = pathlib.Path(__file__).parent

# Find all .py files in the directory, excluding __init__.py
module_names = [
    f.stem for f in package_dir.glob("*.py")
    if f.name != "__init__.py" and f.name.endswith(".py")
]

# Import each module into the tools namespace
logger.info(f"Importing tools: {module_names}")
for name in module_names:
    logger.info(f"Importing tool: {name}")
    importlib.import_module(f"{__name__}.{name}")
