import neatCsv from 'neat-csv';
import xml2js from 'xml2js';
import _ from 'lodash';
import { open, readFile, writeFile } from 'node:fs/promises';
import path from 'path';
// {
//   "name": [
//     "Nature cell biology"
//   ],
//   "issn": [
//     "1465-7392",
//     "1476-4679"
//   ],
//   "medAbbr": [
//     "Nat Cell Biol"
//   ],
//   "isoAbbr": [
//     "Nat Cell Biol"
//   ],
//   "alias": [
//     "Cell biology",
//     "Nature Cell Biol",
//     "Nature cell biology"
//   ]
// }
// {
//   id: "20315",
//   title: "Nature Reviews Molecular Cell Biology",
//   type: "journal",
//   issn: [
//     "1471-0072",
//     "1471-0080",
//   ],
//   h_index: "508",
//   publisher: "Nature Publishing Group",
//   categories: "Cell Biology (Q1); Molecular Biology (Q1)",
// }

/**
 * Extract json fields from xml file
 *
 * @param {string} pathname Path to the file
 * @param {Array} paths Tags to the array of data
 * @param {Map} names The columns to extract, key name to use
 * @param {Object} opts The parser opts  {@link https://www.npmjs.com/package/xml2js}
 * @returns
 */
async function xmlFromCsv( pathname, xmlPath = [], names, opts = {} ) {
  const parser = new xml2js.Parser( opts );
  const rename = o => {
    _.keys( o ).forEach( old_key => {
      const new_key = names.get( old_key );
      if (old_key !== new_key) {
        Object.defineProperty(o, new_key,
            Object.getOwnPropertyDescriptor(o, old_key));
        delete o[old_key];
      }
    });
  };
  const flatten = o => {
    _.keys( o ).forEach( key => {
      const value = o[key].map( a => {
        if( a instanceof Object ){
           return a['_'];
        } else {
          return a;
        }
      });
      o[key] = value;
    });
  }
  let data;
  const pick = ( o, fields )  => _.pick( o, fields );
  const xml = await readFile( pathname, 'utf8' );
  let raw = await parser.parseStringPromise( xml );
  raw = _.get( raw, xmlPath );
  const fields = Array.from( names.keys() );
  data = raw.map( o => pick( o, fields ) );
  data.forEach( rename );
  data.forEach( flatten );
  return data;
}

/**
 * Extract json fields from csv file
 *
 * @param {string} pathname Path to the file
 * @param {Map} names The columns to extract, key name to use
 * @param {Object} opts The parser opts {@link https://www.npmjs.com/package/csv-parser}
 * @returns
 */
async function jsonFromCsv( pathname, names, opts = {} ) {
  const pick = ( o, fields )  => _.pick( o, fields );
  const rename = o => {
    _.keys( o ).forEach( old_key => {
      const new_key = names.get( old_key );
      if (old_key !== new_key) {
        Object.defineProperty(o, new_key,
            Object.getOwnPropertyDescriptor(o, old_key));
        delete o[old_key];
      }
    });
  };
  let data;
  const fd = await open( pathname );
  const stream = fd.createReadStream();
  const raw = await neatCsv( stream, opts );
  const fields = Array.from( names.keys() );
  data = raw.map( o => pick( o, fields ) );
  data.forEach( rename );
  return data;
}

async function combineRaw(){
  // When values are 'NULL' or '' set to null
  const mapValues = ({ value }) => value === 'NULL' || value === '' ? null : value;

  // For issn, set null or empty to []
  const splitIssn = o => {
    const insertHyphen = s => s.replace(/(\w{4})(\w{4})/, '$1-$2');
    let issn = [];
    if( !_.isNull( o.issn ) ){
      issn = o.issn
        .split(',')
        .map( s => s.trim() )
        .map( insertHyphen );
    }
    return _.assign( {}, o, { issn } );
  }
  const DATA_DIR = 'data';
  const DATA_FILES = [
    'List of Journals and Preprints - scimagojr 2023  Subject Area - Biochemistry, Genetics and Molecular Biology.csv',
    'List of Journals and Preprints - scimagojr 2023  Subject Area - Immunology and Microbiology.csv',
    'List of Journals and Preprints - scimagojr 2023  Subject Area - Medicine.csv',
    'List of Journals and Preprints - scimagojr 2023  Subject Area - Multidisciplinary.csv',
    'List of Journals and Preprints - scimagojr 2023  Subject Area - Neuroscience.csv',
    'List of Journals and Preprints - scimagojr 2023  Subject Area - Pharmacology, Toxicology and Pharmaceutics.csv',
    'List of Journals and Preprints - scimagojr 2023  Subject Area - Veterinary.csv',
    'List of Journals and Preprints - Preprints.csv'
  ];
  const DATA_FIELDS = new Map([
    ['Sourceid', 'id'],
    ['Title', 'title'],
    ['Type', 'type'],
    ['Issn', 'issn'],
    ['H index', 'h_index'],
    ['Publisher', 'publisher'],
    ['Categories', 'categories']
  ]);

  let combined = [];

  for ( const filename of DATA_FILES ) {
    const filepath = path.resolve( path.join( DATA_DIR, filename ) );
    let data = await jsonFromCsv( filepath, DATA_FIELDS, { mapValues } );
    data = data.map( splitIssn );
    combined = combined.concat( data );
  }

  combined = _.uniqBy( combined, 'id' );
  return combined;
};


async function addSynonyms( data ){

  async function getMeta(){
    const hasIssn = m => _.has( m, 'issn' ) && m.issn.length;
    const DATA_DIR = 'data';
    const DATA_FILE = 'jourcache.xml';
    const DATA_FIELDS = new Map([
      ['Name', 'name'],
      ['Issn', 'issn'],
      ['MedAbbr', 'medAbbr'],
      ['IsoAbbr', 'isoAbbr'],
      ['Alias', 'alias']
    ]);
    const filepath = path.resolve( path.join( DATA_DIR, DATA_FILE ) );
    let meta = await xmlFromCsv( filepath, ['JournalCache', 'Journal'], DATA_FIELDS );
    meta = meta.filter( hasIssn );
    return meta;
  }

  function getSynonyms( title, meta ){
    const lowerCase = o => o.toLowerCase()
    let synonyms = [];
    const synonymKeys = ['name', 'medAbbr', 'isoAbbr', 'alias'];
    synonymKeys.forEach( k => {
      if( _.has( meta, k ) && _.get( meta, k ).length ){
        synonyms = synonyms.concat( _.get( meta, k ) );
      }
    });
    // include the title as a synonym for uniqueness check
    const unique = _.uniqBy([ title, ...synonyms ], lowerCase );
    return _.pull( unique, title ); // remove title
  }

  function hasCommonElements( a, b ) {
    return _.intersection( a, b ).length > 0;
  }

  const merged = [];
  const meta = await getMeta();
  for( const d of data ){
    let synonyms = [];
    const match = _.find( meta, m => _.has( m, 'issn' ) && _.has( d, 'issn' ) && hasCommonElements( d.issn, m.issn ) );
    if( match ){
      synonyms = getSynonyms( d.title, match );
    }
    merged.push( _.assign( {}, d, { synonyms } ) );
  }
  return merged;
}

async function main(){
  const JOURNALS_PATH = 'data/journal-list.json';
  const data = await combineRaw();
  const journals = await addSynonyms( data );
  const jsonData = JSON.stringify( journals, null, 2 );
  await writeFile( JOURNALS_PATH, jsonData, 'utf8' );
  // const ncb = _.find( journals, o => _.includes( o['issn'], '1465-7392' ) );
  // console.log( JSON.stringify(ncb, null, 2) );
};

//TODOs
// - issn = split 1474175X
// - Nature reviews cancer (1474-1768) synonym is "cancer"???

main();
