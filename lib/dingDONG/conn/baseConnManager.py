
from collections import OrderedDict

from dingDONG.misc.enums import eConn
from dingDONG.conn.connDB       import connDb
from dingDONG.conn.connAccess   import access
from dingDONG.conn.connMongo    import connMongo
from dingDONG.conn.connFile     import connFile
from dingDONG.misc.logger       import p
from dingDONG.config            import config

CLASS_TO_LOAD = {eConn.types.SQLSERVER :connDb,
                 eConn.types.ORACLE:connDb,
                 eConn.types.VERTICA:connDb,
                 eConn.types.ACCESS:access,
                 eConn.types.MYSQL:connDb,
                 eConn.types.LITE:connDb,
                 eConn.types.FILE:connFile,
                 eConn.types.MONGO:connMongo,
                 eConn.types.POSTGESQL:connDb}


def addPropToDict (existsDict, newProp):
    if newProp and isinstance(newProp, (dict, OrderedDict)):
        for k in newProp:
            if k in existsDict and isinstance(newProp[k], dict):
                existsDict = addPropToDict (existsDict, newProp=newProp[k])
            elif k not in existsDict:
                existsDict[k] = newProp[k]
            elif k in existsDict and existsDict[k] is None:
                existsDict[k] = newProp[k]
    elif isinstance(newProp,str):
        existsDict[eConn.props.URL] = newProp
    else:
        p("THERE IS AN ERROR ADDING %s INTO DICTIONARY " %(newProp), "e")

    return existsDict

def mngConnectors(propertyDict, connLoadProp=None):

    connLoadProp = connLoadProp if connLoadProp else config.CONNECTIONS

    ## Merge by CONNECTION
    if eConn.props.TYPE in propertyDict and propertyDict[eConn.props.TYPE] in connLoadProp:
        propertyDict = addPropToDict(existsDict=propertyDict, newProp=connLoadProp[propertyDict[eConn.props.TYPE]])
        # update Connection type
        if eConn.props.TYPE in connLoadProp[propertyDict[eConn.props.TYPE]] and connLoadProp[propertyDict[eConn.props.TYPE]][eConn.props.TYPE] is not None:
            propertyDict[eConn.props.TYPE] = connLoadProp[propertyDict[eConn.props.TYPE]][eConn.props.TYPE]

    if eConn.props.NAME in propertyDict and propertyDict[eConn.props.NAME] in connLoadProp :
        propertyDict = addPropToDict(existsDict=propertyDict, newProp=connLoadProp[propertyDict[eConn.props.NAME]])

    elif eConn.props.TYPE in propertyDict and propertyDict[eConn.props.TYPE] in connLoadProp :
        propertyDict = addPropToDict(existsDict=propertyDict, newProp=connLoadProp[propertyDict[eConn.props.TYPE]])

    if eConn.props.NAME not in propertyDict or propertyDict[eConn.props.NAME] is None:
        propertyDict[eConn.props.NAME] = propertyDict[eConn.props.TYPE]

    # Tal: if ther is no URL, check if conn is Set, search if there is only one conn and update details
    if  eConn.props.URL not in propertyDict or (propertyDict[eConn.props.URL] is None and propertyDict[eConn.props.TYPE] is not None):
        connToLook  = propertyDict[eConn.props.TYPE]

        # will match / add missing values from config_url. based on connection type
        for c in connLoadProp:
            if c == connToLook or (eConn.props.TYPE in connLoadProp[c] and connToLook == connLoadProp[c][eConn.props.TYPE] ):
                connParams = connLoadProp[c]

                for prop in connParams:
                    if prop not in propertyDict:
                        propertyDict[prop] = connParams[prop]
                        p("CONN: %s, ADD PROPERTY %s, VALUE: %s" % (str(connToLook), str(prop), str(connParams[prop])),"ii")
                    elif connParams[prop] is None and connParams[prop] is not None:
                        connParams[prop] = connParams[prop]
                        p("CONN: %s, UPDATE PROPERTY %s, VALUE: %s" % (str(connToLook), str(prop), str(connParams[prop])),"ii")


    if propertyDict and isinstance(propertyDict, dict) and eConn.props.TYPE in propertyDict:
        cType = propertyDict[eConn.props.TYPE]
        if cType in CLASS_TO_LOAD:


            return CLASS_TO_LOAD[cType]( propertyDict=propertyDict )

        else:
            p("CONNECTION %s is NOT DEFINED. PROP: %s" % (str(cType), str(propertyDict)), "e")
    else:
        p ("connectorMng->mngConnectors: must have TYPE prop. prop: %s " %(str(propertyDict)), "e")

def __queryParsetIntoList(self, sqlScript, getPython=True, removeContent=True, dicProp=None, pythonWord="dingDONG"):
        if isinstance(sqlScript, (tuple, list)):
            sqlScript = "".join(sqlScript)
        # return list of sql (splitted by list of params)
        allQueries = self.__getAllQuery(longStr=sqlScript, splitParam=['GO', u';'])

        if getPython:
            allQueries = self.__getPythonParam(allQueries, mWorld=pythonWord)

        if removeContent:
            allQueries = self.__removeComments(allQueries)

        if dicProp:
            allQueries = self._replaceProp(allQueries, dicProp)

        return allQueries
