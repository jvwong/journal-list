import neatCsv from 'neat-csv';
import _ from 'lodash';
import { open } from 'node:fs/promises';
import path from 'path';


/**
 * Extract json fields from csv file
 *
 * @param {string} csvPath Path to the csv file
 * @param {Map} names The columns to extract, key name to use
 * @param {Object} opts The csv-parser opts {@link https://www.npmjs.com/package/csv-parser}
 * @returns
 */
async function jsonFromCsv( csvPath, names, opts = {}) {
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
  const fd = await open( csvPath );
  const stream = fd.createReadStream();
  const raw = await neatCsv( stream, opts );
  const fields = Array.from( names.keys() );
  data = raw.map( o => pick( o, fields ) );
  data.forEach( rename );
  return data;
}

async function combineRaw(){

  const mapValues = ({ value }) => value === 'NULL' ? null : value;
  const splitIssn = o => {
    const insertHyphen = s => s.replace(/(\d{4})(\d{4})/, '$1-$2');
    let issn = o.issn.split(',');
    issn = issn.map( insertHyphen );
    o.issn = issn;
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
    data.forEach( splitIssn );
    combined = combined.concat( data );
  }

  return combined;
};

async function main(){
  let data = await combineRaw();
  data = _.uniqBy( data, 'id' );
  const example = _.find( data, ['id', '101680187' ]);
  console.log( example );
  console.log( data.length );
};


main();
