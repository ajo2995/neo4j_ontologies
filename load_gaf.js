const argv = require('minimist')(process.argv.slice(2));
const neo4j = require('neo4j-driver').v1;
const uri = argv.uri;
const user = argv.user;
const password = argv.password;
const gafFile = argv.gaf;

const driver = neo4j.driver(uri, neo4j.auth.basic(user, password));
const session = driver.session(neo4j.WRITE);

function deleteNodes(tx) {
  return tx.run(`MATCH (n:${label}) DETACH DELETE n`);
}

function addGene(tx, properties) {
  return tx.run(`CREATE (:Gene $props)`,{props: properties});
}

function addRelationship(tx, relationship, source, target, properties) {
  return tx.run(
    `MATCH (source:Gene),(target:GO)
    WHERE source.id = '${source}' AND target.id = '${target}'
    CREATE (source)-[r:${relationship} $props]->(target)
    `,{props: properties});
}

function parseGAF(fname) {
  let gotGene = {};
  let genes = [];
  
  let gaf = require('fs').readFileSync(fname, 'utf-8');
  let lines = gaf.split(/[\r\n]+/g).filter(line => !line.match(/^!/));
  let relationships = lines.map(line => {
    const cols = line.split(/\t/);
    const gene = cols[9];
    if (gene.match(/\s/)) return null;
    if (!gotGene.hasOwnProperty(gene)) {
      gotGene[gene] = genes.length;
      genes.push({
        id: gene,
        symbol: cols[2],
        synonyms: cols[10].split('|').filter(s => s !== gene && s!== cols[2]),
        biotype: cols[11],
        taxon: cols[12].replace('taxon:','')
      })
    }
    let term = cols[4];
    let props = {
      evidenceCode: cols[6],
      date: cols[13],
      assignedBy: cols[14]
    };
    if (!!cols[3]) {
      props.qualifier = cols[3]
    }
    if (!!cols[7]) {
      props.withOrFrom = cols[7];
    }
    return {
      source: gene,
      target: term,
      props: props
    }
  });
  return { genes, relationships };
}

async function runAsync() {
  let gaf = parseGAF(gafFile);

  for(let i=0;i<gaf.genes.length;i++) {
    await session.writeTransaction(tx => addGene(tx, gaf.genes[i]));
  }
  for(let i=0;i<gaf.relationships.length;i++) {
    const rel = gaf.relationships[i];
    if (rel) {
      await session.writeTransaction(tx => addRelationship(tx, 'ASSOCIATED_WITH', rel.source, rel.target, rel.props || {}));
    }
  }
  session.close();
  driver.close();
}

runAsync();
