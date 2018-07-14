/* Lambda function for Rekognition */
'use strict';

const DELIVERY_STREAM = 'demoanalyticsapp_fh'

function groupBy(xs, key) {
    return xs.reduce((rv, x) => {
      (rv[x[key]] = rv[x[key]] || []).push(x)
      return rv
    }, {})
}

exports.handler = (event, context, callback) => {
    let files = event.Records.map((item) => {
        return {
            bucket: item.s3.bucket.name,
            key: item.s3.object.key,
            arn: item.s3.bucket.arn
        }
    })

    console.log('Got files - ', files.length)

    let AWS = require('aws-sdk');
    AWS.config.update({region: 'us-east-1'});
    let Rek = new AWS.Rekognition()
    let Firehose = new AWS.Firehose()

    /* Detect faces in image */
    function DetectFaces (file) {
        return new Promise((resolve, reject) => {
            console.log('Detecting Faces for ', file.key)
            let FaceAttributes = ['ALL']
            let params = { Image: { S3Object: { Bucket: file.bucket, Name: file.key } }, Attributes: FaceAttributes }
            Rek.detectFaces(params, (err, data) => {
                if (err) return reject(err)
                return resolve({
                    ts: new Date().toJSON(),
                    source: file.key,
                    type: 'face',
                    data: data
                })
            })
        })
    }

    /* Detect text in image */
    function DetectText (file) {
        return new Promise((resolve, reject) => {
            console.log('Detecting Text for ', file.key)
            let params = { Image: { S3Object: { Bucket: file.bucket, Name: file.key } } }
            Rek.detectText(params, (err, data) => {
                if (err) return reject(err)
                return resolve({
                    ts: new Date().toJSON(),
                    source: file.key,
                    type: 'text',
                    data: data
                })
            })
        })
    }

    function FirehosePut (params) {
        return new Promise((resolve, reject) => {
            console.log('Writing events to Firehose')
            Firehose.putRecordBatch(params, (err, resp) => {
                if (err) return reject(err)
                return resolve(resp)
            })
        })
    }

    let p_faces = files.map((file) => { return DetectFaces(file) })
    let p_texts = files.map((file) => { return DetectText(file) })
    let promises = p_faces.concat(p_texts)

    Promise.all(promises)
    .then((data) => {
        console.log('Promises resolved - Num of data records returns: ', data.length)
        let recs = data.map((i) => { return { Data: JSON.stringify(i) } })
        let params = { DeliveryStreamName: DELIVERY_STREAM, Records: recs }
        console.log(recs)
        return FirehosePut(params)
    })
    .then((fh) => {
        console.log('Finished pushing to Firehose, closing up...')
        callback(null, {  })
    })
    .catch((err) => { 
        console.log('Oops something went wrong - ', err)
        callback(err, undefined)
    })
}