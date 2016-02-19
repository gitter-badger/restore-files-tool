'use strict';
const fs = require('fs-extra');
const path = require('path');
const config = require('config');
const lblReader = require('line-by-line');
const MongoClient = require('mongodb').MongoClient;

const DEST_DIR = config.get('files.dest');
const SRC_DIR = config.get('files.src');

let ops = 0;
let wc = 0;

function getObjectsByFileName(file_name, file_size, db) {
    return db.collection('objects').find({
        '_file.name': file_name,
        '_file.size': parseInt(file_size, 10)
    }).toArray();
}

function createDirectoryByObject(obj) {
    return new Promise((resolve, reject) => {
        fs.mkdir(path.join(DEST_DIR, obj._id.toString()), err => {
            if(err) return reject(err);

            fs.mkdir(path.join(DEST_DIR, obj._id.toString(), obj._file.uploadStartTime.toString()), err => {
                if(err) return reject(err);

                fs.copy(path.join(SRC_DIR, obj._file.name), path.join(DEST_DIR, obj._id.toString(), obj._file.uploadStartTime.toString(), obj._file.name), err => {
                    if(err) return reject(err);
                    resolve(path.join(DEST_DIR, obj._id.toString(), obj._file.uploadStartTime.toString()));
                });
            })
        })
    });
}

MongoClient.connect(config.get('db.url'), (err, db) => {
    if(err) {
        console.error('Cannot connect to DB');
        process.exit(1);
    }

    console.log('INFO: Successfully connected to DB!');
    console.log('INFO: Started restoring directories...');

    const lr = new lblReader(config.get('files.list'));

    lr.on('line', function(line) {
        ++wc;

        lr.pause();

        const info = line.split(/\s/);

        const name = info[0];
        const size = info[1];

        getObjectsByFileName(name, size, db).then(objs => {
            if(!objs.length) {
                console.warn(`WARN: Not found file ${name} with size ${size} in DB`);
            }

            ops += objs.length;

            for(let obj of objs) {
                createDirectoryByObject(obj, size).then(path => {
                    console.log(`INFO: Successfully restored file ${name} for object ${obj._id} in path ${path}`);
                    if(!--ops && !wc) {
                        console.log('INFO: All operations done. Exiting');
                        process.exit(0);
                    }
                }).catch(err => {
                    console.error(`ERR: Problems during recreate directory for file ${name} for object ${obj._id}`);
                    if(!--ops && !wc) {
                        console.log('INFO: All operations done. Exiting');
                        process.exit(0);
                    }
                });
            }

            --wc;
            lr.resume();
        });

    });

    lr.on('end', function() {
        if(!ops) {
            console.log('INFO: Not found operations. Exiting');
            process.exit(0);
        }
        console.log('INFO: File successfully read');
    });
});

