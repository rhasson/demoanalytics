'use strict';
console.log('Loading function');

let path = require('path')
let AWS = require('aws-sdk')
let firehose = new AWS.Firehose()
let glue = new AWS.Glue()

var PARTITION_SPEC = {
  "StorageDescriptor": {},
  "Values": []
}

process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason);
})

function describe_delivery_stream (deliveryStreamArn) {
    return new Promise((resolve, reject) => {

        let stream_name = path.parse(deliveryStreamArn).name
        let re = new RegExp(/(\d{4})-(\d{2})-(\d{2})T(\d{2})/, 'i')
        let matches = new Date().toISOString().match(re)

        firehose.describeDeliveryStream({
            'DeliveryStreamName': stream_name
        }, (err, body) => {
            if (err) return reject(new Error('Failed to describe firehose detail - ' + err))

            let stream = body.DeliveryStreamDescription.Destinations[0]
            let db_name = stream.ExtendedS3DestinationDescription.DataFormatConversionConfiguration.SchemaConfiguration.DatabaseName
            let table_name = stream.ExtendedS3DestinationDescription.DataFormatConversionConfiguration.SchemaConfiguration.TableName
            let bucket = stream.ExtendedS3DestinationDescription.BucketARN.split(':')
            bucket = bucket[bucket.legnth - 1]
            let prefix = stream.ExtendedS3DestinationDescription.Prefix

            return resolve({
                'db_name': db_name,
                'table_name': table_name,
                'partitions': [ matches[1], matches[2], matches[3], matches[4] ]
            })
        })
    })
}

function get_partition_spec (params) {
    return new Promise((resolve, reject) => {
        glue.getTable({
            'DatabaseName': params.db_name,
            'Name': params.table_name
        }, (err, data) => {
            if (err) return reject(new Error('Failed to describe table - ' + err))
            PARTITION_SPEC.StorageDescriptor = data.Table.StorageDescriptor
            PARTITION_SPEC.StorageDescriptor.Location = data.Table.StorageDescriptor.Location 
              + params.partitions[0] + '/'
              + params.partitions[1] + '/'
              + params.partitions[2] + '/'
              + params.partitions[3] + '/'
            PARTITION_SPEC.Values = params.partitions

            return resolve({
                'db_name': params.db_name,
                'table_name': params.table_name, 
                'partition': PARTITION_SPEC
            })
        })
    })
}

function add_partition (spec) {
    return new Promise((resolve, reject) => {
        glue.createPartition({
            'DatabaseName': spec.db_name,
            'TableName': spec.table_name,
            'PartitionInput': spec.partition
        }, (err, data) => {
            if (err) return reject(new Error('Failed to create partition - ' + err))
            return resolve(data)
        })
    })
}

function prep_records (event) {
    return new Promise((resolve, reject) => {
        /* Process the list of records and transform them */
        const output = event.records.map((record) => {
            return {
                recordId: record.recordId,
                result: 'Ok',
                data: record.data
            };
        })

        return resolve(output)
    })
}

exports.handler = (event, context, callback) => {
    
    describe_delivery_stream(event.deliveryStreamArn)
    .then((params) => { return get_partition_spec(params) })
    .then((spec) => { return add_partition(spec) })
    .catch((err) => { console.log(err) })
    
    const output = event.records.map((record) => {
        return {
            recordId: record.recordId,
            result: 'Ok',
            data: record.data
        };
    })
    
    callback(null, { records: output })
}
