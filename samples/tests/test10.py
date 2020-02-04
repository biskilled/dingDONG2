""" import modules -> logging used fr setting log level"""
import logging
from dingDONG import dingDONG
from dingDONG import Config

Config.LOGS_DEBUG = logging.DEBUG
Config.VERSION_DIR = "C:\\dingDong"


Config.CONN_URL = {
    'file': "C:\\dingDong\\",
    'sqlite': {"url":"C:\\dingDong\\sqlLiteDB.db","create":"tableName"}}

nodesToLoad = [
    {"source": ["file", "DATAELEMENTDESCRIPTION.csv"],
     "target": ["sqlite", "dateElements_Desc"]},

    {"source": ["file", "DEMOGRAPHICS.csv"],
     "target": ["sqlite", "demographics"]},

    {"source": ["file", "MEASURESOFBIRTHANDDEATH.csv"],
     "target": ["sqlite", "birthDate"]},

    {"query": ["sqlite", """   Select d.[Number_Counties],  d.[Number_Counties] AS CC, d.[State_FIPS_Code] AS A, d.[County_FIPS_Code] AS B, d.[County_FIPS_Code] AS G,d.[County_FIPS_Code], d.[CHSI_County_Name], d.[CHSI_State_Name],[Population_Size],[Total_Births],[Total_Deaths]
                                    From demographics d INNER JOIN birthDate b ON d.[County_FIPS_Code] = b.[County_FIPS_Code] AND d.[State_FIPS_Code] = b.[State_FIPS_Code]"""],
     "target": ["sqlite", "Final", -1]},

    {"myexec": ["sqlite", "Update dateElements_Desc Set [Data_Type] = 'dingDong';"]},

    {"source": ["sqlite", "Final"],
     "target": ["file", "finall.csv"]}

]

dd = dingDONG(dicObj=nodesToLoad, filePath=None, dirData=None,
             includeFiles=None,notIncludeFiles=None,connDict=None, processes=1)

dd.msg.addState("Start Ding")
dd.ding()

#dd.msg.addState("Start Dong")
dd.dong()
dd.msg.end(msg="FINISHED",pr=True)