const argv = require('minimist')(process.argv.slice(2));
const neo4j = require('neo4j-driver').v1;
const uri = argv.uri;
const user = argv.user;
const password = argv.password;
const label = argv.label;
const oboFile = argv.obo;

const driver = neo4j.driver(uri, neo4j.auth.basic(user, password));
const session = driver.session(neo4j.WRITE);

function deleteNodes(tx) {
  return tx.run(`MATCH (n:${label}) DETACH DELETE n`);
}

function addTerm(tx, properties) {
  return tx.run(`CREATE (:${label} $props)`,{props: properties});
}

function addRelationship(tx, relationship, source, target, properties) {
  return tx.run(
    `MATCH (source:${label}),(target:${label})
    WHERE source.id = '${source}' AND target.id = '${target}'
    CREATE (source)-[r:${relationship} $props]->(target)
    `,{props: properties});
}

function parseOBO(fname) {
  var fs = require('fs')
    , ini = require('./ini')

  return ini.parse(fs.readFileSync(fname, 'utf-8'))
}

async function runAsync() {
  await session.writeTransaction(tx => deleteNodes(tx, label));
  // let terms = [
  //   {
  //     id: 'GO:0000001',
  //     name: 'mitochondrion inheritance',
  //     namespace: 'biological_process',
  //     def: 'The distribution of mitochondria, including the mitochondrial genome, into daughter cells after mitosis or meiosis, mediated by interactions between mitochondria and the cytoskeleton.'
  //   },
  //   {
  //     id: 'GO:0000002',
  //     name: 'who knows',
  //     namespace: 'biological_process',
  //     def: 'I made this one up just to test my javascript code.'
  //   }
  // ];
  let ontology = parseOBO(oboFile);
  // ontology.terms = terms;
  for(let i=0;i<ontology.Term.length;i++) {
    await session.writeTransaction(tx => addTerm(tx, ontology.Term[i]));
  }
  for(let i=0;i<ontology.relationships.Term.length;i++) {
    const rel = ontology.relationships.Term[i];
    await session.writeTransaction(tx => addRelationship(tx, rel.type, rel.source, rel.target, rel.props || {}));
  }
  // await session.writeTransaction(tx => addRelationship(tx, 'is_a','GO:0000001','GO:0000002',{}))
  session.close();
  driver.close();
}

runAsync();
