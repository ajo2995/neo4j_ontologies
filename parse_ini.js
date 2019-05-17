var fs = require('fs')
  , ini = require('./ini')

var config = ini.parse(fs.readFileSync('./go.obo', 'utf-8'))

console.log(JSON.stringify(config,null,2));
