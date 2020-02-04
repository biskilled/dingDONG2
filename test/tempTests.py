dd = {
    eNums.dataType.B_STR: {
        eNums.dataType.DB_VARCHAR:None,
        eNums.dataType.DB_NVARCHAR:None,
        eNums.dataType.DB_CHAR:None,
        eNums.dataType.DB_BLOB:None},
    eNums.dataType.B_INT: {eNums.dataType.DB_INT:None,
                           eNums.dataType.DB_BIGINT:None},
    eNums.dataType.B_FLOAT:{eNums.dataType.DB_FLOAT:None,
                            eNums.dataType.DB_DECIMAL:None},
    eNums.dataType.DB_DATE:{eNums.dataType.DB_DATE:None}
}


class bb (baseConn):
    def __init__ (self, propertyDict=None, **args):


        baseConn.__init__(self, propertyDict=propertyDict, dataTypes=dd)
        self.dataTypes = self.setDataTypes(connDataTypes=setProperty(k=eNums.conn.DATA_TYPES, o=args, defVal={}) )

        print ('-------------------------------------------')
        print (self.dataTypes)
        pass

dd1 = { eNums.dataType.DB_VARCHAR:['1','222222222222222'] }

dd2 = { eNums.dataType.DB_VARCHAR:['yyyy','xxxxxxxxxxxxxxxxxxxxx'] }

class bb2 (bb):
    def __init__ (self, **args):
        self.dataTypes = setProperty(k=eNums.conn.DATA_TYPES,o=args, defVal={})
        bb.__init__(self, dataTypes=self.dataTypes)




xx = bb2(dataTypes=dd1)
yy = bb2(dataTypes=dd2)