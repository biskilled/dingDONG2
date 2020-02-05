from lib.dingDONG.conn.connGlobalDB import connGlobalDb
from dingDONG.misc.enums            import eConn

dt = {eConn.dataTypes.DB_DATE:['dddddddddddddd']}

xx = connGlobalDb (connType='sql', connUrl='fdfdfdfdf', dataTypes=dt)