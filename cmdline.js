#!/usr/bin/env node

const MySQLPgImporter = require('./index.js')

let imp
(async function() {
  imp = new MySQLPgImporter()
  //imp.schema = 'myapp'
  //imp.setIgnoreRule('container_cleanup')
  //imp.importFile = process.env.MYSQLDUMP_PATH  // default shown
  imp.nameTranslator = (tname, cname) => {
    let tname2=tname, cname2=cname
    if (tname == 'user') tname2 = 'users'

    if (cname) {
      // rename columns named "id" to "tablename_id"
      //if (cname == 'id') cname2 = tname+'_id'

      // ensure cols referencing fk id field also end with _id
      //if (imp.tableDefs[tname].cols[cname].refcol && imp.tableDefs[tname].cols[cname].refcol == 'id' && ! /\_id$/.test(cname2)) cname2 += '_id'

      if (cname2=='user') cname2='user_id'
    }

    return [tname2, cname2]
  }
  //imp.filterInsertVal = (mysqlTable, mysqlCol, val, meta) => val
  await imp.execute()
})().catch( e => { console.log(e); console.log('imp object: ', imp); process.exit(-1); })
