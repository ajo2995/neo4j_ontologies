const _ = require('lodash');
const argv = require('minimist')(process.argv.slice(2));
const neo4j = require('neo4j-driver');
const uri = argv.uri;
const user = argv.user;
const password = argv.password;
const oboFile = argv.obo;

const driver = neo4j.driver(uri, neo4j.auth.basic(user, password));
const session = driver.session(neo4j.WRITE);

function addNode(tx, label, properties) {
  return tx.run(`CREATE (:${label} $props)`,{props: properties});
}
function addNodes(tx, label, nodes) {
  return tx.run(`
    UNWIND $props AS map
    MERGE (n:${label} { id: map.id})
    ON CREATE SET n = map
    ON MATCH SET n += map`
    ,{props:nodes})
}

function addRelationship(tx, relationship, source, target, properties) {
  return tx.run(`
    MATCH (source:${source.label}),(target:${target.label})
    WHERE source.id = '${source.id}' AND target.id = '${target.id}'
    CREATE (source)-[r:${relationship} $props]->(target)`
    ,{props: properties});
}

function parseOBO(fname) {
  var fs = require('fs')
    , ini = require('./ini')

  return ini.parse(fs.readFileSync(fname, 'utf-8'))
}

async function runAsync() {
  console.error("parsing OBO file");
  let ontology = parseOBO(oboFile);
  let n = ontology.Term.length;
  let x = ontology.xrefs ? ontology.xrefs.length : 0;
  let r = ontology.relationships.Term.length;
  console.error(`Found ${n} terms ${r} relationships and ${x} xrefs`);
  const chunk=100;
  for(let i=0;i<n;i+=chunk) {
    await session.writeTransaction(tx => addNodes(tx, 'Term', ontology.Term.slice(i,i+chunk)));
  }
  console.error(`Added terms`);

  for(let i=0;i<x;i+=chunk) {
    await session.writeTransaction(tx => addNodes(tx, 'Xref', ontology.xrefs.slice(i,i+chunk)));
  }
  console.error(`Added xrefs`);

  for(let i=0;i<r;i++) {
    if (i%100 === 0) {
      process.stderr.write(`Adding relationships ${Math.floor(10000*i/r)/100}% complete\r`);
    }
    const rel = ontology.relationships.Term[i];
    await session.writeTransaction(tx => addRelationship(tx, rel.type, rel.source, rel.target, rel.props || {}));
  }
  process.stderr.write(`\nAdded relationships`);

  console.error("\nClosing session");
  session.close();
  driver.close();
}

runAsync();
