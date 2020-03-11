# (c) 2017-2019, Tal Shany <tal.shany@biSkilled.com>
#
# This file is part of dingDong
#
# dingDong is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# dingDong is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with dingDong.  If not, see <http://www.gnu.org/licenses/>.

import sys
import shutil
import os
import io
import time
import codecs
from collections import OrderedDict
if sys.version_info[0] == 2:
    import csv23 as csv
else:
    import csv as csv

from dingDONG.conn.baseConnBatch import baseConnBatch
from dingDONG.conn.transformMethods import  *
from dingDONG.misc.enums  import eConn, eJson
from dingDONG.conn.globalMethods import uniocdeStr, setProperty
from dingDONG.config import config
from dingDONG.misc.logger     import p

DEFAULTS = {
    eConn.defaults.FILE_MIN_SIZE :1024,
    eConn.defaults.FILE_DEF_COLUMN_PREF :'col_',
    eConn.defaults.FILE_ENCODING:'windows-1255',
    eConn.defaults.FILE_DELIMITER:',',
    eConn.defaults.FILE_ROW_HEADER:1,
    eConn.defaults.FILE_END_OF_LINE:'\r\n',
    eConn.defaults.FILE_MAX_LINES_PARSE:50000,
    eConn.defaults.FILE_LOAD_WITH_CHAR_ERR:'strict',
    eConn.defaults.FILE_APPEND:False,
    eConn.defaults.FILE_CSV:False,
    eConn.defaults.FILE_REPLACE_TO_NONE:None, #r'\"|\t',
    eConn.defaults.DEFAULT_TYPE:eConn.dataTypes.B_STR

    }

DATA_TYPES = {  }

class connFile (baseConnBatch):
    def __init__ (self, folder=None,fileName=None,
                    fileMinSize=None, colPref=None, encode=None,isCsv=None,
                    delimiter=None,header=None, endOfLine=None,linesToParse=None,
                    withCharErr=None,append=None,replaceToNone=None,
                    isTar=None, isSrc=None, propertyDict=None):

        self.connType = eConn.types.FILE
        baseConnBatch.__init__(self, connType=self.connType, propertyDict=propertyDict, defaults=DEFAULTS, isTar=isTar, isSrc=isSrc)

        """ FILE DEFAULTS PROPERTIES """

        self.fileMinSize= setProperty(k=eConn.defaults.FILE_MIN_SIZE, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_MIN_SIZE], setVal=fileMinSize)
        self.colPref    = setProperty(k=eConn.defaults.FILE_DEF_COLUMN_PREF, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_DEF_COLUMN_PREF], setVal=colPref)
        self.encode     = setProperty(k=eConn.defaults.FILE_ENCODING, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_ENCODING], setVal=encode)
        self.header     = setProperty(k=eConn.defaults.FILE_ROW_HEADER, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_ROW_HEADER], setVal=header)
        self.maxLinesParse = setProperty(k=eConn.defaults.FILE_MAX_LINES_PARSE, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_MAX_LINES_PARSE], setVal=linesToParse)
        self.withCharErr= setProperty(k=eConn.defaults.FILE_LOAD_WITH_CHAR_ERR, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_LOAD_WITH_CHAR_ERR], setVal=withCharErr)
        self.delimiter  = setProperty(k=eConn.defaults.FILE_DELIMITER, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_DELIMITER], setVal=delimiter)
        self.endOfLine  = setProperty(k=eConn.defaults.FILE_END_OF_LINE, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_END_OF_LINE], setVal=endOfLine)
        self.append     = setProperty(k=eConn.defaults.FILE_APPEND, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_APPEND], setVal=append)
        self.replaceToNone= setProperty(k=eConn.defaults.FILE_REPLACE_TO_NONE, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_REPLACE_TO_NONE], setVal=replaceToNone)
        self.isCsv      = setProperty(k=eConn.defaults.FILE_CSV, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_CSV], setVal=isCsv)
        self.defDataType= setProperty(k=eConn.defaults.DEFAULT_TYPE, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.DEFAULT_TYPE], setVal=isCsv)
        self.columnFrame= ('','')

        """ FILE PROPERTIES """
        self.fileFullName = None
        self.folder   = setProperty(k=eConn.props.FOLDER, o=self.propertyDict, setVal=folder)
        if not self.folder:
            self.folder = setProperty(k=eConn.props.URL, o=self.propertyDict, defVal=None)

        self.fileName = setProperty(k=eConn.props.TBL, o=self.propertyDict, setVal=fileName)


        if self.fileName:
            head, tail = os.path.split (self.fileName)
            if head and len(head)>0 and tail and len (tail)>1:
                self.fileFullName   = self.fileName
                self.folder         = head
            elif self.folder:
                self.fileFullName = os.path.join(self.folder, self.fileName)
            else:
                p(u"THERE IS NO FOLDER MAPPING, FILE CONNENTION FAILED, %s" %(self.fileName), "e")
                return
            if (os.path.isfile(self.fileFullName)):
                p (u"FILE EXISTS:%s, DELIMITER %s, HEADER %s " %(self.fileFullName , self.delimiter ,self.header ), "ii")
        elif self.folder:
            head, tail = os.path.split(self.folder)
            if head and len(head)>0 and tail and len (tail)>1:
                self.folder = head
                self.fileFullName = self.folder

    def connect(self, fileName=None):
        if fileName:
            self.fileFullName = fileName
            return True
        elif not self.fileFullName:
            if self.folder and os.path.isdir(self.folder):
                p("CONNETCTED USING FOLDER %s" %self.folder)
                return True
            else:
                err = u"FILE NOT VALID: %s" %(self.fileFullName)
                raise ValueError(err)
        return True

    def close(self):
        pass

    def test(self):
        baseConnBatch.test(self)

    def isExists(self, fullPath=None):
        fullPath = fullPath if fullPath else self.fileFullName
        return os.path.isfile(fullPath)

    def create(self, stt=None, fullPath=None, addIndex=None):
        fullPath = fullPath if fullPath else self.fileFullName
        self.cloneObject(stt=stt, fullPath=fullPath)
        p("NO POINT TO CREATE FILE %s " %(fullPath), "ii")

    """ INTERNAL USED: 
        for create method create new File is file is exist
        If config.TRACK_HISTORY will save old table as tablename_currentDate   """
    def cloneObject(self, stt=None, fullPath=None):
        fullPath = fullPath if fullPath else self.fileFullName
        fileName = os.path.basename(fullPath)
        fileDir = os.path.dirname(fullPath)

        fileNameNoExtenseion = os.path.splitext(fileName)[0]
        fimeNameExtension = os.path.splitext(fileName)[1]
        ### check if table exists - if exists, create new table
        isFileExists = os.path.isfile(fullPath)
        toUpdateFile = False

        if isFileExists:
            actulSize = os.stat(fullPath).st_size
            if actulSize < self.fileMinSize:
                p("FILE %s EXISTS WITH SIZE SMALLER THAN %s --> WONT UPDATE  ..." % (fullPath, str(actulSize)), "ii")
                toUpdateFile = False

            fileStructure = self.getStructure(fullPath=fullPath)
            fileStructureL = [x.lower() for x in fileStructure]
            sttL = [x.lower() for x in stt]

            if set(fileStructureL) != set(sttL):
                toUpdateFile = True
                p("FILE %s EXISTS, SIZE %s STRUCTURE CHANGED !!" % (fullPath, str(actulSize)), "ii")
            else:
                p("FILE %s EXISTS, SIZE %s STRUCURE DID NOT CHANGED !! " % (fullPath, str(actulSize)), "ii")

            if toUpdateFile and config.DING_TRACK_OBJECT_HISTORY:
                oldName = None
                if (os.path.isfile(fullPath)):
                    oldName = fileNameNoExtenseion + "_" + str(time.strftime('%y%m%d')) + fimeNameExtension
                    oldName = os.path.join(fileDir, oldName)
                    if (os.path.isfile(oldName)):
                        num = 1
                        oldName = os.path.splitext(oldName)[0] + "_" + str(num) + os.path.splitext(oldName)[1]
                        oldName = os.path.join(fileDir, oldName)
                        while (os.path.isfile(oldName)):
                            num += 1
                            FileNoExt = os.path.splitext(oldName)[0]
                            FileExt = os.path.splitext(oldName)[1]
                            oldName = FileNoExt[: FileNoExt.rfind('_')] + "_" + str(num) + FileExt
                            oldName = os.path.join(fileDir, oldName)
                if oldName:
                    p("FILE HISTORY, FILE %s EXISTS, COPY FILE TO %s " % (str(self.fileName), str(oldName)), "ii")
                    shutil.copy(fullPath, oldName)

    """ Strucutre Dictinary for file: {Column Name: {ColumnType:XXXXX} .... }
        Types : STR , FLOAD , INT, DATETIME (only if defined)  """
    def getStructure(self, fullPath=None):
        ret     = OrderedDict()
        fullPath= fullPath if fullPath else self.fileFullName

        if self.isExists(fullPath=fullPath):
            with io.open(fullPath, 'r', encoding=self.encode) as f:
                if not self.header:
                    headers = f.readline().strip(self.endOfLine).split(self.delimiter)
                    if headers and len(headers) > 0:
                        for i, col in enumerate(headers):
                            colStr = '%s%s' %(self.colPref, str(i))
                            ret[colStr] = {eJson.stt.TYPE: self.defDataType, eJson.stt.SOURCE: colStr}
                else:
                    for i, line in enumerate(f):
                        if self.header-1 == i:
                            headers = line.strip(self.endOfLine).split(self.delimiter)
                            if headers and len(headers) > 0:
                                for i, col in enumerate(headers):
                                    ret[col] = {eJson.stt.TYPE: self.defDataType, eJson.stt.SOURCE: col}

                            break
        else:
            p ('FILE NOT EXISTS %s >>> ' %( str(fullPath) ), "ii")

        return ret

    def preLoading(self):
        if self.isExists() and self.append:
            p("FILE %s EXISTS WILL APPEND DATA " % (self.fileFullName))

    def extract(self, tar, tarToSrc, batchRows=None, addAsTaret=True):
        batchRows       = batchRows if batchRows else self.batchSize
        fnOnRowsDic     = {}
        execOnRowsDic   = {}

        startFromRow    = 0 if not self.header else self.header
        listOfColumnsH  = {}
        listOfColumnsL  = []
        targetColumnList= []

        fileStructure   = self.getStructure()
        fileStructureL  = OrderedDict()

        for i, col in enumerate(fileStructure):
            fileStructureL[col.lower()] = i
            listOfColumnsH[i] = col

        ## File with header and there is target to source mapping
        if tarToSrc and len(tarToSrc)>0:
            mappingSourceColumnNotExists = u""
            fileSourceColumnNotExists    = u""

            for i, col in enumerate (tarToSrc):
                targetColumnList.append(col)
                if eJson.stt.SOURCE in tarToSrc[col] and tarToSrc[col][eJson.stt.SOURCE]:
                    srcColumnName = tarToSrc[col][eJson.stt.SOURCE]
                    if srcColumnName.lower() in fileStructureL:
                        listOfColumnsL.append(fileStructureL[ srcColumnName.lower() ])
                    else:
                        mappingSourceColumnNotExists+=uniocdeStr (srcColumnName)+u" ; "
                else:
                    listOfColumnsL.append(-1)

                ### ADD FUNCTION
                if eJson.stt.FUNCTION in tarToSrc[col] and tarToSrc[col][eJson.stt.FUNCTION]:
                    fnc = eval(tarToSrc[col][eJson.stt.FUNCTION])
                    fnOnRowsDic[ i ] = fnc if isinstance(fnc, (list, tuple)) else [fnc]

                ### ADD EXECUTION FUNCTIONS
                if eJson.stt.EXECFUNC in tarToSrc[col] and len(tarToSrc[col][eJson.stt.EXECFUNC]) > 0:
                    newExcecFunction = tarToSrc[col][eJson.stt.EXECFUNC]
                    regex = r"(\{.*?\})"
                    matches = re.finditer(regex, tarToSrc[col][eJson.stt.EXECFUNC], re.MULTILINE | re.DOTALL)
                    for matchNum, match in enumerate(matches):
                        for groupNum in range(0, len(match.groups())):
                            colName = match.group(1)
                            if colName and len(colName) > 0 and colName in fileStructureL:
                                colName = colName.replace("{", "").replace("}", "")
                                newExcecFunction.replace(colName, colName)
                    execOnRowsDic[i] = newExcecFunction

            for colNum in listOfColumnsH:
                if colNum not in listOfColumnsL:
                    fileSourceColumnNotExists+= uniocdeStr( listOfColumnsH[colNum] )+u" ; "

            if len(mappingSourceColumnNotExists)>0:
                p("SOURCE COLUMN EXISTS IN SOURCE TO TARGET MAPPING AND NOT FOUND IN SOURCE FILE: %s" %(mappingSourceColumnNotExists),"w")

            if len (fileSourceColumnNotExists)>0:
                p("FILE COLUMN NOT FOUD IN MAPPING: %s" %(fileSourceColumnNotExists),"w")
        ## There is no target to source mapping, load file as is
        else:
            for colNum in listOfColumnsH:
                listOfColumnsL.append(colNum)

        """ EXECUTING LOADING SOURCE FILE DATA """
        rows = []
        try:
            with io.open( self.fileFullName, 'r', encoding=self.encode, errors=self.withCharErr) as textFile:
                if self.isCsv:
                    fFile = csv.reader(textFile, delimiter=self.delimiter)
                    for i, split_line in enumerate(fFile):
                        if i>=startFromRow:
                            if self.replaceToNone:
                                rows.append( [ re.sub(self.replaceToNone,"",split_line[x],re.IGNORECASE|re.MULTILINE|re.UNICODE)  if x>-1 and len(split_line[x])>0 else None for x in listOfColumnsL] )
                            else:
                                rows.append([split_line[x] if x > -1 and len(split_line[x]) > 0 else None for x in listOfColumnsL])

                        if self.maxLinesParse and i>startFromRow and i%self.maxLinesParse == 0:
                            rows = self.dataTransform(data=rows, functionDict=fnOnRowsDic, execDict=execOnRowsDic)
                            tar.load(rows=rows, targetColumn=targetColumnList)
                            rows = list ([])
                else:
                    for i, line in enumerate(textFile):
                        line = re.sub(self.replaceToNone,"",line,re.IGNORECASE|re.MULTILINE|re.UNICODE) if self.replaceToNone else line
                        line = line.strip(self.endOfLine)
                        split_line = line.split(self.delimiter)
                        # Add headers structure
                        if i >= startFromRow:
                            rows.append([split_line[x] if x > -1 and len(split_line[x]) > 0 else None for x in listOfColumnsL])

                        if self.maxLinesParse and i > startFromRow and i % self.maxLinesParse == 0:
                            rows = self.dataTransform(data=rows, functionDict=fnOnRowsDic, execDict=execOnRowsDic)
                            tar.load(rows=rows, targetColumn=targetColumnList)
                            rows = list([])

                if len(rows)>0 : #and split_line:
                    rows = self.dataTransform(data=rows, functionDict=fnOnRowsDic, execDict=execOnRowsDic)
                    tar.load(rows=rows, targetColumn=targetColumnList)
                    rows = list ([])

        except Exception as e:
            p("ERROR LOADING FILE %s  >>>>>>" % (self.fileFullName) , "e")
            p(str(e), "e")

    def load(self, rows, targetColumn):
        totalRows = len(rows) if rows else 0
        if totalRows == 0:
            p("THERE ARE NO ROWS","w")
            return

        if self.append:
            pass

        with codecs.open(filename=self.fileFullName, mode='wb', encoding=self.encode) as f:
            if targetColumn and len(targetColumn) > 0:
                f.write(self.delimiter.join(targetColumn))
                f.write(self.endOfLine)

            for row in rows:
                row = [str(s) for s in row]
                f.write(self.delimiter.join(row))
                f.write(self.endOfLine)

        p('LOAD %s ROWS INTO FILE %s >>>>>> ' % (str(totalRows), self.fileFullName), "ii")
        return

    def execMethod(self, method=None):
        pass

    def merge (self, fileName, hearKeys=None, sourceFile=None):
        raise NotImplementedError("Merge need to be implemented")

    def cntRows (self, objName=None):
        raise NotImplementedError("count rows need to be implemented")

    def createFromDbStrucure(self, stt=None, objName=None, addIndex=None):
        raise NotImplementedError("createFrom need to be implemented")
