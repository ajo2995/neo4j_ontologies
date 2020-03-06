const argv = require('minimist')(process.argv.slice(2));
const neo4j = require('neo4j-driver');
const uri = argv.uri;
const user = argv.user;
const password = argv.password;
const qtlFile = argv.qtl;

const driver = neo4j.driver(uri, neo4j.auth.basic(user, password));
const session = driver.session(neo4j.WRITE);

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

function parseQTLs(fname) {
  let tsv = require('fs').readFileSync(fname, 'utf-8');
  let lines = tsv.split(/[\r\n]+/g).filter(line => !line.match(/^#/));
  let nodes = {
    QTL:[],
    Publication:[]
  };
  let pubs = {};
  let relationships = [];
  lines.forEach(line => {
    const cols = line.split(/\t/);
    if (cols[5]) {
      let loc = cols[5].match(/(\w+):(\d+)-(\d+)/);
      if (loc) {
        nodes.QTL.push({
          id: cols[0],
          map: 'GCA_000003195.3',
          region: loc[1],
          start: loc[2],
          end: loc[3]
        });
        if (!pubs.hasOwnProperty(cols[1])) {
          pubs[cols[1]] = true;
          nodes.Publication.push({
            id: cols[1]
          });
        }
        relationships.push({
          source: {label: 'QTL', id:cols[0]},
          target: {label: 'Publication', id:cols[1]}
        });
        if (cols[2]) {
          relationships.push({
            source: {label: 'QTL', id:cols[0]},
            target: {label: 'Term', id:cols[2]}
          })
        }
        if (cols[4]) {
          relationships.push({
            source: {label: 'QTL', id:cols[0]},
            target: {label: 'Term', id:cols[4]}
          })
        }
      }
    }
  });
  return { nodes, relationships };
}

async function runAsync() {
  console.error('parsing QTLs');
  let qtls = parseQTLs(qtlFile);
  let r = qtls.relationships.length;
  let q = qtls.nodes.QTL.length;
  let p = qtls.nodes.Publication.length;
  console.error(`found ${p} pubs ${q} qtls ${r} relationships`);
  let chunk=100;
  for(let i=0;i<q;i+=chunk) {
    await session.writeTransaction(tx => addNodes(tx, 'QTL', qtls.nodes.QTL.slice(i,i+chunk)));
  }
  for(let i=0;i<p;i+=chunk) {
    await session.writeTransaction(tx => addNodes(tx, 'Publication', qtls.nodes.Publication.slice(i,i+chunk)));
  }
  for(let i=0;i<r;i++) {
    if (i%100 === 0) {
      process.stderr.write(`Adding relationships ${Math.floor(10000*i/r)/100}% complete\r`);
    }
    const rel = qtls.relationships[i];
    await session.writeTransaction(tx => addRelationship(tx, 'ASSOCIATED_WITH', rel.source, rel.target, rel.props || {}));
  }
  console.error("\nclosing session");
  session.close();
  driver.close();
}

runAsync();
