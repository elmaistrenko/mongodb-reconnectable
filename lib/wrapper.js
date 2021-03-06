const {MongoClient, MongoTimeoutError, MongoError} = require('mongodb');
const retry = require('promise-retry');
const {merge, union} = require('lodash');

const defaultToWrap = [
    'countDocuments',
    'find',
    'findOne',
    'insertOne',
    'insertMany',
    'updateOne',
    'updateMany',
    'deleteOne',
    'deleteMany',
    'bulkWrite',
    'aggregate',
    'distinct',
];

module.exports = function (options) {
    const {
        url,
        onError,
        watch = true,
        retryOptions = {
            retries: 100,
            maxTimeout: 30000,
        },
    } = options;
    const clientOptions = merge({
        useUnifiedTopology: true,
    }, options.clientOptions || {});
    const toWrap = union(options.toWrap || [], defaultToWrap);

    let client = connect();
    let destroyed = false;
    let stream = null;
    const changeHandlers = [];

    async function connect() {
        return retry(async retry => {
            if (destroyed)
                return null;
            try {
                const client = new MongoClient(url, clientOptions);
                await client.connect();
                if (watch) {
                    stream = await client.watch();
                    stream.on('error', function (e) {
                        onError(e);
                        if (isReConnectable(e))
                            reconnect();
                    });
                    stream.on('change', change => changeHandlers.forEach(fn => fn(change)));
                }
                return client;
            } catch (e) {
                onError(e);
                if (e instanceof MongoTimeoutError)
                    return retry(e);
                throw e;
            }
        }, retryOptions).catch(onError);
    }

    let reconnecting = false;

    async function reconnect() {
        if (reconnecting)
            return;
        reconnecting = true;
        try {
            await destroy(false);
            client = connect(url, options);
        } catch (e) {
            onError(e);
        }
        reconnecting = false;
    }

    async function wrapped(dbName, collName) {
        if (destroyed)
            return null;
        const coll = (await client).db(dbName).collection(collName);
        const res = {};
        toWrap.forEach(function (key) {
            res[key] = async function (...args) {
                try {
                    return await coll[key](...args);
                } catch (e) {
                    if (isReConnectable(e))
                        reconnect();
                    throw e;
                }
            };
        });
        return res;
    }

    async function destroy(setDestroyed = true) {
        if (setDestroyed)
            destroyed = true;
        if (watch)
            await stream.close();
        await (await client).close(true);
    }

    function isDestroyed() {
        return destroyed;
    }

    function onChange(fn) {
        changeHandlers.push(fn);
    }

    function offChange(fn) {
        const index = changeHandlers.indexOf(fn);
        if (index > -1)
            changeHandlers.splice(index, 1);
    }

    return {wrapped, destroy, isDestroyed, onChange, offChange};
};

function isReConnectable(e) {
    return e instanceof MongoTimeoutError ||
        (e instanceof MongoError && (
            /topology was destroyed/i.test(e.message) ||
            /Topology is closed, please connect/i.test(e.message)
        ));
}
