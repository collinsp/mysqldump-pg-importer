# MySQL Dump Postgresql Importer

```bash
// install
npm install mysqldump-pg-importer

// define postgres environment
export PGUSER=EnterYourUserHere
export PGHOST=127.0.0.1
export PGPASSWORD=EnterYourPasswordHere
export PGDATABASE=EnterDatabaseName
export PGPORT=5432
export MYSQLDUMP_PATH=/path/to/a/mysqldump.sql

// make sure database user and database exists
sudo -u postgres createuser $PGUSER
sudo -u postgres createdb --owner=$PGUSER $PGDATABASE

// start import
mysqldump-pg-importer

// you can also start the importer using code using this example
vim importmysqldb.js
```

```javascript
const MySQLtoPgImporter = require('mysql-pg-importer')

let imp
(async function() {
  imp = new MySQLtoPgImporter()
  //imp.schema = 'myapp'
  //imp.setIgnoreRule('tablename1,tablename2,tablename3')
  //imp.importFile = process.env.MYSQLDUMP_PATH  // default shown

  // example name translator
  imp.nameTranslator = (tname, cname) => {
    let tname2=tname, cname2=cname

    // "user" is a reserved word in postgres
    if (tname == 'user') tname2 = 'users'

    if (cname) {
      // convert all "id" named columns to "tablename_id"
      if (cname == 'id') cname2 = tname+'_id'

      // ensure cols referencing fk id field also end with _id
      if (imp.tableDefs[tname].cols[cname].refcol && imp.tableDefs[tname].cols[cname].refcol == 'id' && ! /\_id$/.test(cname2)) cname2 += '_id'

      if (cname2=='user') cname2='user_id'
    }

    return [tname2, cname2]
  }

  //imp.filterInsertVal = (mysqlTable, mysqlCol, val, meta) => val
  await imp.execute()
})().catch( e => { console.log(e); console.log('imp object: ', imp); process.exit(-1); })
```

