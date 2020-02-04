try:
    from pip import main as pipmain
except:
    from pip._internal import main as pipmain

"""
def import_or_install(package):
    try:
        __import__(package)
    except ImportError:
        pip.main(['install', package]
"""

pipmain(['install', 'pyodbc'])