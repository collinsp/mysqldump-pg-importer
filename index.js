"use strict"

// these are used for debugging oom issues
//const heapdump = require('heapdump');
//const v8 = require('v8');

module.exports = class MySQLPgImporter {
  constructor(importFile) {
    // these members can be reconfigured after construction, before execute is called
    this.logger = console.log    // where should we log to
    this.DEBUG = false           // log everything
    this.nameTranslator = (mysqlTable, mysqlCol) => { return [mysqlTable, mysqlCol]; }  // override to convert names
    this.filterInsertVal = (mysqlTable, mysqlCol, val, meta) => { return val; }         // optionally provide filter function
    this.importFile = importFile ? importfile : process.env.MYSQLDUMP_PATH  // set mysql dump file (text document containing mysql SQL commands)
    this.minWorkingBufLen = 80   // the parser will ensure the buffer is filled with this much data by default
    this.schema = process.env.PGSCHEMA // schema we should import tables into
    this.importFileReadStream = undefined
    this.dbh = undefined         // postgres database handle
    this.dbhInTransaction = false // remember if we are in a database transaction (postgres database is built in a single transaction)

    /* parsed mysql table defs format:
      { "tablename": {
           name: "tablename",
           colorder: ["colname", ..], 
           pk: "pkcolname",
           unq: ["colname", "colname,colname", ..],
           indexes: [ "colname", "colname,colname", ..],
           cols: {
             "colname": {
               name: "colname",
               type: "sqltype",
               mysqlColIdx: 5, // insert index position
               reftable: 'tname', refcol: 'colname', refopts:'ON DELETE CASCADE' // defined for fk refs
             }, ..
           }, ..
         }, ..
      }
    */
    this.tableDefs = {}
    this.tableOrder = []         // remember order in which we saw the tables in the mysql dump, we will recreate them in postgres in the same order
    this.parseLineNo = 0         // line we are currently parsing
    this.state = {}              // variable to store parse state
    this.buf = ""                // working parse buffer
    this.bufHistoryIdx = 0       // where to write the next buf in the bufHistory array, cycles back to 0 when equal to maxBufHistoryLen
    this.bufHistory = []         // stores previous buf so we can inspect them if we encounter a parseError
    this.maxBufHistoryLen = 0    // number of previous buffers to keep (useful for debugging parse errors)
    this.maxHistoryEntryLen = 70 // when the buf is saved to history, trim to this size to reduce the size of the history
    this.insertObj = undefined   // remember previous insert prepared statement object so it can be reused for each insert
    this.ignoreRule = {}         // { tname: 1, tname: { cname: 1 } }
    this.dataFixes = {}        // { <tname>: { <keyval>: { <col>: [<oldval>, <newval>, <why>], .. }, .. }, .. }
  }

  // supply optional ignore ruleset; usage: imp.setIgnoreRule("ignoreTable1,ignoreTable2.col1")
  setIgnoreRule(rule='') {
    this.ignoreRule={}
    rule.replace(/\ /g,'').split(/,/).map( x => {
      const [tname, cname] = x.split('.')
      if (cname) {
        if (!(tname in this.parsedIgnoreRule)) {
          this.ignoreRule[tname] = { cname: 1 }
        } else if (typeof this.ignoreRule[tname] == 'object') {
          this.ignoreRule[tname][cname]=1
        }
      } else {
        this.ignoreRule[tname]=1
      }
    })
  }

  isIgnored(tname, cname=undefined) {
    if (!(tname in this.ignoreRule)) return false;
    if (typeof(this.ignoreRule[tname]) == 'object') {
      if (cname == undefined) return false;
      return !!this.ignoreRule[tname][cname];
    }
    return !!this.ignoreRule[tname];
  }

  // emulate perl _quotemeta to escape unsafe chars in a regex
  _quotemeta(regexStr) {
    return regexStr.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, "\\$&")
  }

  // called when the parser encounters unrecognizable data
  // outputs the current state and parse buf history
  // throws exception to end parsing
  _parseError(msg) {
    const m = 'ParseError line: '+this.parseLineNo+'; msg: ' + msg
    let errbuf = m
    errbuf += "\n  buf: ", this.buf.substr(0, 100)  // show current buffer (trimmed down a bit)

    if (this.maxBufHistoryLen > 0) {
      let i = this.bufHistoryIdx
      let seq = 0
      while (true) {
        i = (i==0) ? this.bufHistory.length - 1 : i - 1
        if (i==this.bufHistoryIdx) break
        --seq
        errbuf += "\n  buf("+seq+"): " + this.bufHistory[i]
      }
      errbuf += "\n  parse state: " + JSON.stringify(this.state)
    }
    this.logger(errbuf)
    throw m
  }

  _showLine() {
    if (/^(.*)/.test(this.buf)) this.logger((this.parseLineNo + 1) + ': ' + RegExp.$1.substr(0,70))
  }

  // executes import
  async execute() {
    if (this.schema == '') throw 'missing this.schema value'

    // setup dbh
    if (! this.dbh) {
      const { Client } = require('pg')
      this.dbh = new Client()
      await this.dbh.connect()
    }

    // verify database is working
    const res = await this.dbh.query('SELECT $1::text AS val', ['testconnection'])
    if (res.rows[0].val != 'testconnection') throw 'expected string "testconnection"'
    
    await this.dbh.query('BEGIN')
    this.dbhInTransaction = true

    if (this.schema) {
      await this.dbh.query(`DROP SCHEMA IF EXISTS ${this.schema} CASCADE`)
      await this.dbh.query(`CREATE SCHEMA ${this.schema}`)
      await this.dbh.query(`SET search_path = ${this.schema}`)
    }

    // setup read stream
    if (! this.importFileReadStream) {
      const fs = require('fs')
      this.importFileReadStream = fs.createReadStream(this.importFile)
    }
    this.importFileReadStream.setEncoding('utf8')

    // read chunks from the read stream
    for await (const chunk of this.importFileReadStream) {
      this.buf += chunk
      await this._processData()  // parse, translate, exec in postgres database
    }
    this.importFileReadStream = undefined
    await this._processData()
    await this._enableTableConstraints()
    await this.dbh.query('COMMIT')
    this.dbhInTransaction = false
    await this.dbh.end()
  }

  // not currently used, but left for reference on how to write a heapdump for diagnosing oom issues
  _preventMemoryLeaks() {
    const ms = Date.now()
    const dT = ms - this.lastMemoryLeakCheckMS;
    if (dT < 500) return
    this.lastMemoryLeakCheckMS = ms
    const { space_size, space_used_size } = v8.getHeapSpaceStatistics()[1]; // old space stats
    const usage = ((space_used_size / space_size) * 100).toFixed()

    // if we are running out of memory, generate a heap report which we can review in chrome
    if (usage > 80) {
      this.logger(`FATAL: memory usage is ${usage}; writing heap snapshot and exiting; please wait...`);
      heapdump.writeSnapshot( (err, filename) => {
        this.logger(`         heap snapshot written to: ${filename}`);
      });
      throw 'oom'
    }

    // recreate postgres prepared statement
    this.insertObj = undefined;

    // use some techniques from the internet to dereference memory so it can be
    // garbage collected (not sure these actually work)
    // causes RegExp globals to reset, executing multiple regexs on a large
    // string leaks memory because the RegExp class retains the last matcher as
    // a global. Due to efficiencies in how v8 stores string under the hood
    // (https://www.just-bi.nl/a-tale-of-a-javascript-memory-leak/) memory
    // leaks unless we execute a trivial regex now and then 
    /\s*/g.exec("");

    // https://bugs.chromium.org/p/v8/issues/detail?id=2869
    this.buf = (' ' + this.buf).substr(1);
  }
  
  // parse and process data in buffer
  async _processData() {

    // continue to parse this.buf if:
    // the readstream is still open AND the buffer size is >= minWorkingBufLen 
    // OR the readstream is closed (we read it all) and there is still stuff in the buffer to parse
    while ((! this.importFileReadStream && this.buf.length > 0) || (this.importFileReadStream && this.buf.length >= this.minWorkingBufLen)) {

      // not needed anymore
      //this._preventMemoryLeaks()

      this.DEBUG && this._showLine()
  
      // if not parsing a statement, try to parse the start of an SQL statement
      if (! this.state.parsetype) {
        // blank line
        if (/^ *\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          ++this.parseLineNo 
        }
        else if (/^\-\-.*?\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          ++this.parseLineNo 
        }
        // parse start of multiline comment
        else if (/^ *\/\*/.test(this.buf)) {
          this.buf = RegExp.rightContext
          this.state = { parsetype: 'multilinecomment' }
        }
        else if (/^CREATE TABLE `([^`]+)` *\(\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          const name = RegExp.$1
          this.state = { parsetype: 'createtable', name: name, cols: {}, colorder:[], pk: undefined, unq:[], indexes:[] }
          ++this.parseLineNo 
        }
        else if (/^(DROP TABLE|LOCK TABLES|UNLOCK TABLES).*?\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          this.state = {}
          ++this.parseLineNo 
        }
        else if (/^DELIMITER (\S+)\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          const delimiter = RegExp.$1
          const regexp = new RegExp(this._quotemeta(delimiter))
          this.state = { parsetype: 'delimiter', regexp: regexp }
          ++this.parseLineNo 
        }
        else if (/^INSERT INTO `([^`]+)` VALUES */.test(this.buf)) {
          this.buf = RegExp.rightContext
          const tname = RegExp.$1
          this.state = { parsetype: 'insert', tname: tname, rec: undefined }
        }
        else {
          this._parseError('unknown statement')
        }
      }
  
      else if (this.state.parsetype==='multilinecomment') {
        if (/^.*?\*\/ *\;? */.test(this.buf)) {
          this.buf = RegExp.rightContext
          this.state = {}
        } else {
          this._parseError("could not parse end of multiline comment")
        }
      }
      
      // if in the middle of parsing a statement
      else if (this.state.parsetype==='createtable') {
      
        // parse `colname` coltype
        if (/^`([^`]+)` (\w+) *(.+?)\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          let colopts = RegExp.$3
          let colname = RegExp.$1

          const col = { name: colname, type: RegExp.$2.toUpperCase(), mysqlColIdx: this.state.colorder.length }
          ++this.parseLineNo 
    
          // clone inner data struct, and add info
          this.state.cols[colname]=col
          this.state.colorder.push(colname)

  
          // if ENUM type, turn into VARCHAR with check constraint
          if (col.type==='ENUM' && /^\(/.test(colopts)) {
            colopts = RegExp.rightContext
            col.type = 'VARCHAR'
            col.scale = 1 
            const opts = []
            while (/^'([^']+)',?/.test(colopts)) {
              colopts = RegExp.rightContext
              const opt = RegExp.$1 
              if (opt.length > col.scale) col.scale = opt.length
              opts.push(opt)
            }
            col.ck = 'CHECK ('+col.name+' IN ('+opts.map(x => "'"+x+"'" ).join(',')+')'
          } else {
            // extract scale if exists
            if (/^\((\d+)\) */.test(colopts)) {
              colopts = RegExp.rightContext
              col.scale = parseInt(RegExp.$1,10)
            }
            // translate type to postgres type
            col.type = col.type
               .replace(/ *UNSIGNED */,'')
               .replace(/^BIT$/,'BOOLEAN')
               .replace(/^INT$/,'INTEGER')
               .replace(/^DATETIME$/,'TIMESTAMP WITHOUT TIME ZONE')
               .replace(/^DOUBLE$/,'DOUBLE PRECISION')
               .replace(/^FLOAT$/,'REAL')
               .replace(/^MEDIUMINT$/,'INTEGER')
               .replace(/^\w+BLOB$/,'BYTEA')
               .replace(/^TINYINT$/,'SMALLINT')
               .replace(/^(TINY|MEDIUM|LONG)TEXT$/,'TEXT')
               .replace(/^TIME$/,'TIME WITHOUT TIME ZONE')
               .replace(/^VARBINARY.*$/,'BYTEA')
            if (/INTEGER|BIGINT/.test(col.type)) col.scale=undefined
          }
      
          // change autoincrement fields to serial types
          if (/\bAUTO_INCREMENT\b/.test(colopts)) {
            if (/BIGINT/.test(col.type)) col.type='BIGSERIAL'
            else if (/SMALLINT/.test(col.type)) col.type='SMALLSERIAL'
            else if (/TINYINT/.test(col.type)) col.type='SMALLSERIAL'
            else col.type = 'SERIAL'
            col.scale = undefined
          }
          if (/SMALLINT|INTEGER/.test(col.type)) {
            col.scale = undefined
          }
      
          // extract default value
          if (/\bDEFAULT '([^\']+)'/.test(colopts)) {
            col.def = RegExp.$1
            if (col.def==='NULL') col.def=undefined
          }
      
          // extract "not null" status
          col.notnull = /\bNOT\ NULL\b/.test(colopts)
        }
      
        else if (/^PRIMARY KEY\ \(([^\)]+)\).*?\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          this.state.pk = RegExp.$1.replace(/\`/g,'')
          ++this.parseLineNo 
        }
        else if (/^UNIQUE KEY `[^`]+` \((.*?)\),?\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          const cols = RegExp.$1.replace(/`/g,'').replace(/\(\d+\)/g,'')
          this.state.unq.push(cols)
          ++this.parseLineNo 
        }
        else if (/^KEY `[^`]+` \((.+?)\)\,?\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          const cols = RegExp.$1.replace(/`/g,'').replace(/\(\d+\)/g,'')
          this.state.indexes.push(cols)
          ++this.parseLineNo 
        }
        else if (/^CONSTRAINT `[^`]+` FOREIGN KEY \(`([^`]+)`\) REFERENCES `([^`]+)` \(`([^`]+)`\)(.*?)\,?\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          let tname=this.state.name, col=RegExp.$1, reftable=RegExp.$2, refcol=RegExp.$3, refopts=RegExp.$4.trim()
          const c = this.state.cols[col]
          c.reftable=reftable
          c.refcol=refcol
          c.refopts=refopts
          ++this.parseLineNo 
        }
      
        // parse end of create table
        else if (/^\).*?\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          this.tableDefs[this.state.name]=this.state
          this.tableOrder.push(this.state.name)
          this._createTable(this.state)
          this.state={}
          ++this.parseLineNo 
        }
        else {
          this._parseError('could not parse create table')
        }
      }
  
      // process quoted string value
      else if (this.state.parsetype==='quotedVal') {
        let i=0
        const l=this.buf.length
        while (i < l) {
          const ch = this.buf.charAt(i)
          if (this.state.escMode) {
            this.state.buf += ch
            this.state.escMode = false
          } else if (ch === '\\') {
            this.state.escMode = true
          } else if (ch === "'") {
            this.state.returnState.quotedVal = this.state.buf
            this.state = this.state.returnState
            i++ // do not include '
            break
          } else {
            this.state.buf += ch
          }
          ++i
        }
        this.buf = this.buf.substr(i)
      }
          
      // process insert statement
      else if (this.state.parsetype==='insert') {
  
        // process quotedVal
        if ('quotedVal' in this.state) {
          this.state.rec.push(this.state.quotedVal)
          delete this.state.quotedVal
        }
  
        // match '(' start of record
        else if (/^\(/.test(this.buf)) {
          this.buf = RegExp.rightContext
          this.state.rec = []
        } 
        // match NULL
        else if (/^NULL/.test(this.buf)) {
          this.buf = RegExp.rightContext
          this.state.rec.push(null)
        }
        // match numeric
        else if (/^(\-?\d*\.\d+|\-?\d+)/.test(this.buf)) {
          this.buf = RegExp.rightContext
          this.state.rec.push(RegExp.$1)
        }
  
        // if start of quoted val
        else if (/^'/.test(this.buf)) {
          this.buf = RegExp.rightContext
          this.state = { parsetype: 'quotedVal', returnState: this.state, buf: '', escMode: false  }
        }
  
        // parse field delimiter
        else if (/^\,/.test(this.buf)) {
          this.buf = RegExp.rightContext
        }
  
        // match ')' end of record
        else if (/^\)\,? */.test(this.buf)) {
          this.buf = RegExp.rightContext
          await this._insertRec(this.state)
          this.state.rec = undefined
        }
        // match end of statement
        else if (/^\;\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          ++this.parseLineNo 
          const tableObj = this.tableDefs[this.state.tname]
          if (tableObj.insertObj) {
            this.logger(`total inserted for ${tableObj.insertObj.name}: ${tableObj.insertObj.totalInserted}`)
            tableObj.insertObj.totalInserted=0   // reset
            tableObj.insertObj.values=undefined
          }
          this.state = {}
        }
        else {
          this._parseError('could not parse insert')
        }
      }
      
      // process delimiter statement
      else if (this.state.parsetype==='delimiter') {
        if (/^(.*?)\r?\n */.test(this.buf)) {
          this.buf = RegExp.rightContext
          let checkForDelimiter = RegExp.$1
          ++this.parseLineNo
          if (this.state.regexp.test(checkForDelimiter)) this.state = {}
        }
      }
      
      else {
        this._parseError('unhandled parse')
      }
  
      // add buf to parse history
      if (this.maxBufHistoryLen > 0) {
        this.bufHistory[this.bufHistoryIdx]=this.buf.substr(0, this.maxHistoryEntryLen)
        ++this.bufHistoryIdx 
        if (this.bufHistoryIdx===this.maxBufHistoryLen) this.bufHistoryIdx=0
      }
    }
  }

  // returns a create table query promise
  // state = { parsetype: 'createtable', name: tname, cols: {}, colorder:[], pk: undefined, unq:[], indexes:[], insertObj: undefined }
  async _createTable() {
    if (!(typeof this.state == 'object' && this.state.name && typeof this.state.cols == 'object')) {
      this.logger('invalid state: ', this.state)
      throw 'invalid state'
    }
    if (this.isIgnored(this.state.name)) return;
    const [pgtable] = this.nameTranslator(this.state.name)

    let buf = []
    for (let cname of this.state.colorder) {
      if (! this.isIgnored(this.state.name, cname)) {
        let def = this.state.cols[cname]
        const [pgtable, pgcol] = this.nameTranslator(this.state.name, cname)
        if (! pgtable || ! pgcol) throw `invalid pgtable this.nameTranslator( ${this.state.name}, ${cname})`
        
        buf.push(`\n  ${pgcol} ${def.type}`
          + ((def.scale) ? `(${def.scale})` : '')
          + ((cname == this.state.pk) ? ' PRIMARY KEY' : '')
          + ((def.notnull) ? ` NOT NULL` : ''))
          + ((def.ck) ? ` ${def.ck}`: '')
      }
    }
    const sql = `CREATE TABLE ${pgtable} (${ buf.join(",") })`
    this.logger(sql)
    await this.dbh.query(sql)
  }

  // returns an insert query promise
  // this.state = { tname: "", rec: []}
  async _insertRec() {
    if (this.isIgnored(this.state.tname)) return;
    
    const tableObj = this.tableDefs[this.state.tname]
    if (! tableObj.insertObj) {
      const valsSQL=[]
      for (let col of tableObj.colorder) {
        if (! this.isIgnored(this.state.tname,col)) {
          valsSQL.push('$'+(valsSQL.length + 1));
        }
      }
      const [pgtable] = this.nameTranslator(tableObj.name)
      tableObj.insertObj = {
        name: 'insert_'+pgtable,
        text: `INSERT INTO ${ pgtable } VALUES (${valsSQL.join(',')})`,
        values: undefined,
        totalInserted: 0
      }
    }
    
    // create insert bind value array for pgsql insert
    tableObj.insertObj.values = []
    for (let i=0, l=tableObj.colorder.length; i<l; ++i) {
      const cname = tableObj.colorder[i] 

      if (! this.isIgnored(tableObj.name, cname)) {
        const colDef = tableObj.cols[cname]
        const oldVal = this.state.rec[i]
        let newVal = oldVal

        // mysql allows invalid year "0000" AD which actually is nonsense - change to year 0001 for this bad data
        if (/^(?:TIMESTAMP|DATE)/.test(colDef.type) && /^0000/.test(newVal)) {
          newVal = newVal.replace(/^0000/,'0001');
        }

        newVal = this.filterInsertVal(tableObj.name, cname, newVal, colDef)

        if (newVal != oldVal) this._logDataFix(tableObj.name, this.state.rec, cname, oldVal, newVal);
        tableObj.insertObj.values.push(newVal)
      }
    }

    this.DEBUG && this.logger(tableObj.insertObj.values)
    ++tableObj.insertObj.totalInserted

    await this.dbh.query(tableObj.insertObj)
  }

  // return key string for table and mysql insert array  like: "id=5"  OR  "col1=6,col2=foo"
  _getKeyVal(tname,recAr) {
    const tableDef = this.tableDefs[tname]
    return (tableDef.pk || tableDef.unq[0] || '').split(',').map(col => col+'='+recAr[tableDef.cols[col].mysqlColIdx]).join(',')
  }

  // record a data fix
  _logDataFix(tname, mysqlRecAr, cname, oldVal, newVal) {
    const recKeyVal = this._getKeyVal(tname, mysqlRecAr)
    if (!(tname in this.dataFixes)) this.dataFixes[tname]={}
    if (!(recKeyVal in this.dataFixes[tname])) this.dataFixes[tname][recKeyVal]={}
    this.dataFixes[tname][recKeyVal][cname] = [oldVal, newVal];
    this.logger(`FIXDATA: ${tname}(${recKeyVal}) ${oldVal} => ${newVal}`)
  }

  // returns promise to execute pgsql to enable table constraints for all tables
  async _enableTableConstraints() {
    const idxSQL=[], fkSQL=[]

    for (let tname of this.tableOrder) {
      if (this.isIgnored(tname)) continue;
      const [pgtable] = this.nameTranslator(tname)

      // tableDef = { name: tname, cols: {}, colorder:[], pk: undefined, unq:[], indexes:[] }
      const tableDef = this.tableDefs[tname]
      const fkColIdxFound={}

      for (let cname of tableDef.colorder) {
        const def = tableDef.cols[cname]
        const pgcol = this.nameTranslator(tname, cname)[1]

        if (def.refcol) {
          const [pgreftable, pgrefcol] = this.nameTranslator(def.reftable, def.refcol)
          fkColIdxFound[pgcol]=false
          
          fkSQL.push(`ALTER TABLE ${pgtable} ADD FOREIGN KEY (${pgcol}) REFERENCES ${pgreftable}(${pgrefcol})${ def.refopts ? ' '+def.refopts : ''}`)
        }
      }

      // add unique constraints
      for (let unqcols of tableDef.unq) {
        const pgcolAr = unqcols.split(',').map(col => this.nameTranslator(tname, col)[1])
        
        if (pgcolAr[0] in fkColIdxFound) {
          fkColIdxFound[pgcolAr[0]]=true // we found a suitable index for this fk col
        }

        idxSQL.push(`CREATE UNIQUE INDEX ON ${pgtable} (${pgcolAr.join(',')})`)
      }

      // add indexes
      for (let idxcols of tableDef.indexes)  {
        const pgcolAr = idxcols.split(',').map(col => this.nameTranslator(tname, col)[1])
        
        if (pgcolAr[0] in fkColIdxFound) {
          fkColIdxFound[pgcolAr[0]]=true // we found a suitable index for this fk col
        }

        idxSQL.push(`CREATE INDEX ON ${pgtable} (${pgcolAr.join(',')})`)
      }

      // ensure all fk cols are indexed
      for (let col in fkColIdxFound) {
        if (fkColIdxFound[col]==false) {
          const [pgtable, pgcol] = this.nameTranslator(tname, col)
          idxSQL.push(`CREATE INDEX ON ${pgtable} (${pgcol})`)
        }
      }
    }

    for (let sql of idxSQL) {
      this.logger(sql)
      await this.dbh.query(sql)
    }
    for (let sql of fkSQL) {
      this.logger(sql)
      await this.dbh.query(sql)
    }
  }

  // disconnect from database
  async end() {
    if (this.dbh) {
      if (this.dbhInTransaction) {
        this.logger('WARNING: you have called end within an active database transaction; auto rollback!')
        await this.dbh.query('ROLLBACK')
        this.dbhInTransaction = false
      }
    }
    this.dbh = undefined
  }
}
