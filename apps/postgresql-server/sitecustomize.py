import sys
sys.path.insert(0, "/opt/venv/lib/python3.10/site-packages")
sys.path.insert(0, "/app")

with open("/tmp/sitecustomize.log", "w") as f:
    f.write("sitecustomize loaded\n")
    f.write("\n".join(sys.path))
