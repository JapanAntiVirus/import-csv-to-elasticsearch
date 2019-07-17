const csv = require('csv-parser');
const fs = require('fs');
const moment = require('moment');
const util = require('util');
var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
    hosts: ['http://localhost:9200']
});
client.index = util.promisify(client.index);
var count = 1;

async function insert(client,row,r){
    try{
        let result = await client.index({
            index: row['_index'],
            // id: row['_id'],
            type: row['_type'],
            body: r
        });
        console.log(result)
        console.log(`insert successfully ${count++} document`)
    }
    catch(err){
        insert(client,row,r);
    }

}

client.ping({
    requestTimeout: 30000,
}, function (error) {
    if (error) {
        console.error('elasticsearch cluster is down!');
    } else {
        console.log('Everything is ok');
        fs.createReadStream('../data2.csv')
            .pipe(csv())
            .on('data', (row) => {
                //   console.log(row);
                let r = { ...row };
                delete r['_id'];
                delete r['_type'];
                delete r['_index'];
                delete r['_score'];
                delete r['@timestamp'];
                for (i in r) {
                    r[i] = r[i].replace(/,/g, '');
                }
                r['scan_id'] = Number(r['scan_id']);
                r['bandwidth_id'] = Number(r['bandwidth_id']);
                r['traffic'] = Number(r['traffic']);
                r['device_id'] = Number(r['device_id']);
                r['highspeed'] = Number(r['highspeed']);
                r['channel_status'] = Number(r['channel_status']);
                r['port'] = Number(r['port']);
                r['createdDate'] = new Date(r['createdDate'].replace('th', '').replace('rd','').replace('st',''));
                
                if(r['trafficAvgOut'])
                    r['trafficAvgOut'] = Number(r['trafficAvgOut']);
                else
                    delete r['trafficAvgOut']
                if(r['trafficAvgIn'])
                    r['trafficAvgIn'] = Number(r['trafficAvgIn']);
                else
                    delete r['trafficAvgIn'];

                if(r['bandwidth_id']!=0)
                    insert(client,row,r);
                // client.index({
                //     index: row['_index'],
                //     // id: row['_id'],
                //     type: row['_type'],
                //     body: r
                // }, function (err, resp, status) {
                //     // console.log(resp);
                //     if(err){
                //         console.log(err);
                //     }
                //     else{
                //         console.log(resp);
                //     }
                // });
                
            })
            .on('end', () => {
                console.log('CSV file successfully processed');
            });
    }
});
