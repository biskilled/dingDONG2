import logging
from dingDong import DingDong
from dingDong import Config

Config.LOGS_DEBUG = logging.DEBUG

# 'sampleSql': {'conn': 'sql',"url": "CPX-VLQ5GA42TW2\SQLEXPRESS;UID=USER;PWD=PWD;"},
Config.CONN_URL = {
    'file':     "C:\\dingDong\\",
    'sqlite': {"url":"C:\\dingDong\\sqlLiteDB.db","create":"createFromCSV"},
    'mongo':    {'url':'mongodb://127.0.0.1:27017','dbname':'test'}}

nodesToLoad = [
    {"source": ["file", "DATAELEMENTDESCRIPTION.csv"],
     "target": ["mongo", "dateElements_Desc"]}
]

nodesToLoad = [
    {"source":  ["file", "createSample.csv"],
     "target": ["sqlite", "createFromCSV"],
     "create": "sqlite"
    }
]


dd = DingDong(dicObj=nodesToLoad, filePath=None, dirData=None,
             includeFiles=None,notIncludeFiles=None,connDict=None, processes=1)

dd.msg.addState("Start Ding")

dd.ding()
dd.dong()

#dd.test()

