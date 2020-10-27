'use strict';

const Storage = require('@google-cloud/storage');
const BigQuery = require('@google-cloud/bigquery');

// Instantiates a client
const storage = Storage();
const bigquery = new BigQuery();


exports.loadFile = (data, context) => {
    const datasetId = 'bq_demo';
    const tableId = 'card_datatransfer';

    const jobMetadata = {
        skipLeadingRows: 1,
        writeDisposition: 'WRITE_APPEND'
    };

    // Loads data from a Google Cloud Storage file into the table
    bigquery
        .dataset(datasetId)
        .table(tableId)
        .load(storage.bucket(data.bucket).file(data.name), jobMetadata)
        .catch(err => {
            console.error('ERROR:', err);
        });

    console.log(`Loading from gs://${data.bucket}/${data.name} into ${datasetId}.${tableId}`);
};
