from pathlib import Path
from os import path
from sys import argv

app_path = Path(path.abspath(argv[0]))

app_fullpathstr = str(app_path)
app_basenamestr = str(app_path.name)
app_dirnamestr = str(app_path.parent)
app_extensionstr = str(app_path.suffix)