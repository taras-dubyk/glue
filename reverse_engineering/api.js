'use strict';

const _ = require('lodash');
const async = require('async');
const fs = require('fs');
const thriftService = require('./thriftService/thriftService');
const hiveHelper = require('./thriftService/hiveHelper');
const entityLevelHelper = require('./entityLevelHelper');
const TCLIService = require('./TCLIService/Thrift_0.9.3_Hive_2.1.1/TCLIService');
const TCLIServiceTypes = require('./TCLIService/Thrift_0.9.3_Hive_2.1.1/TCLIService_types');
const logHelper = require('./logHelper');

module.exports = {
	connect: function(connectionInfo, logger, cb, app){
		if (connectionInfo.path && (connectionInfo.path || '').charAt(0) !== '/') {
			connectionInfo.path = '/' + connectionInfo.path;
		}

		const kerberos = app.require('kerberos');
		const MongoAuthProcess = app.require('kerberos').processes.MongoAuthProcess;

		connectionInfo.isHTTPS = Boolean(
			connectionInfo.mode === 'http' && (isSsl(connectionInfo.ssl) || connectionInfo.ssl === 'https')
		); 

		getSslCerts(connectionInfo, app)
		.then((sslCerts) => {
			if (isSsl(connectionInfo.ssl)) {
				logger.log('info', 'SSL certificates got successfully', 'Connection')
			}

			return thriftService.connect({
				host: connectionInfo.host,
				port: connectionInfo.port,
				username: connectionInfo.user,
				password: connectionInfo.password,
				authMech: connectionInfo.authMechanism || 'NOSASL',
				version: connectionInfo.version,
				mode: connectionInfo.mode,
				configuration: {
					krb_host: connectionInfo.krb_host,
					krb_service: connectionInfo.krb_service
				},
				options: Object.assign({}, {
					https: connectionInfo.isHTTPS,
					path: connectionInfo.path,
					ssl: isSsl(connectionInfo.ssl),
				}, sslCerts)
			})()(TCLIService, TCLIServiceTypes, {
				log: (message) => {
					logger.log('info', { message }, 'Query info')
				}
			}, MongoAuthProcess, kerberos);
		})
		.then(({ cursor, session }) => {
			cb(null, session, cursor);
		}).catch(err => {
			setTimeout(() => {
				cb(err);
			}, 1000);
		});
	},

	disconnect: function(connectionInfo, cb){
		cb();
	},

	testConnection: function(connectionInfo, logger, cb, app){
		logInfo('Test connection', connectionInfo, logger);
		this.connect(connectionInfo, logger, (err) => {
			if (err) {
				logger.log('error', { message: err.message, stack: err.stack, error: err }, 'Connection failed');
			}

			return cb(err);
		}, app);
	},

	getDbCollectionsNames: function(connectionInfo, logger, cb, app) {
		logInfo('Retrieving databases and tables information', connectionInfo, logger);
		
		const { includeSystemCollection } = connectionInfo;

		this.connect(connectionInfo, logger, (err, session, cursor) => {
			if (err) {
				logger.log('error', err, 'Connection failed');

				return cb(err);
			}
			const exec = cursor.asyncExecute.bind(null, session.sessionHandle);
			const execWithResult = getExecutorWithResult(cursor, exec);
			const getTables = getExecutorWithResult(cursor, cursor.getTables.bind(null, session.sessionHandle));

			execWithResult('show databases')
				.then(databases => databases.map(d => d.database_name))
				.then(databases => {
					async.mapSeries(databases, (dbName, next) => {
						const tableTypes = [ "TABLE", "VIEW", "GLOBAL TEMPORARY", "TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM" ];
						
						if (includeSystemCollection) {
							tableTypes.push("SYSTEM TABLE");
						}
						getTables(dbName, tableTypes)
							.then((tables) => {
								return tables.map(table => table.TABLE_NAME)
							})
							.then(dbCollections => {
								next(null, {
									isEmpty: !Boolean(dbCollections.length),
									dbName,
									dbCollections
								})
							})
							.catch(err => next(err))
					}, (err, result) => {
						if (err) {
							logger.log('error', { message: err.message, stack: err.stack, error: err }, 'Retrieving databases and tables information');
						}

						setTimeout(() => {
							cb(err, result);
						}, 1000);
					});
				});
		}, app);
	},

	getDbCollectionsData: function(data, logger, cb, app){
		logger.log('info', data, 'Retrieving schema', data.hiddenKeys);

		const tables = data.collectionData.collections;
		const databases = data.collectionData.dataBaseNames;
		const pagination = data.pagination;
		const includeEmptyCollection = data.includeEmptyCollection;
		const recordSamplingSettings = data.recordSamplingSettings;
		const fieldInference = data.fieldInference;
	
		this.connect(data, logger, (err, session, cursor) => {
			if (err) {
				logger.log('error', err, 'Retrieving schema');
				return cb(err);
			}

			async.mapSeries(databases, (dbName, nextDb) => {
				const exec = cursor.asyncExecute.bind(null, session.sessionHandle);
				const query = getExecutorWithResult(cursor, exec);
				const getPrimaryKeys = getExecutorWithResult(
					cursor,
					cursor.getPrimaryKeys.bind(null, session.sessionHandle)
				);
				const tableNames = tables[dbName] || [];

				exec(`use ${dbName}`)
					.then(() => query(`describe database ${dbName}`))
					.then((databaseInfo) => {
						async.mapSeries(tableNames, (tableName, nextTable) => {
							logger.progress({ message: 'Start sampling data', containerName: dbName, entityName: tableName });

							getLimitByCount(recordSamplingSettings, query.bind(null, `select count(*) as count from ${tableName}`))
								.then(countDocuments => {
									logger.progress({ message: 'Start getting data from database', containerName: dbName, entityName: tableName });

									return getDataByPagination(pagination, countDocuments, (limit, offset, next) => {
										retrieveData(query, tableName, limit, offset).then(data => {
												logger.progress({ message: `${limit * (offset + 1)}/${countDocuments}`, containerName: dbName, entityName: tableName });
												next(null, data);
											}, err => next(err));
									});
								})
								.then((documents) => {
									logger.progress({ message: `Data has successfully got`, containerName: dbName, entityName: tableName });									

									const documentPackage = {
										dbName,
										collectionName: tableName,
										documents,
										indexes: [],
										bucketIndexes: [],
										views: [],
										validation: false,
										emptyBucket: false,
										containerLevelKeys: [],
										bucketInfo: {
											comments: _.get(databaseInfo, '[0].comment', '')
										}
									};

									if (fieldInference.active === 'field') {
										documentPackage.documentTemplate = _.cloneDeep(documents[0]);
									}

									return documentPackage;
								})
								.then((documentPackage) => {
									logger.progress({ message: `Start creating schema`, containerName: dbName, entityName: tableName });

									return allChain(
										() => query(`describe formatted ${tableName}`),
										() => query(`describe extended ${tableName}`),
										() => exec(`select * from ${tableName} limit 1`).then(cursor.getSchema)
									).then(([formattedTable, extendedTable, tableSchema]) => {
										const tableInfo = hiveHelper
											.getFormattedTable(
												...cursor.getTCLIService(),
												cursor.getCurrentProtocol()
											)(formattedTable);
										const extendedTableInfo = hiveHelper.getDetailInfoFromExtendedTable(extendedTable);
										const sample = documentPackage.documents[0];
										documentPackage.entityLevel = entityLevelHelper.getEntityLevelData(tableName, tableInfo, extendedTableInfo);

										return {
											jsonSchema: hiveHelper.getJsonSchemaCreator(...cursor.getTCLIService(), tableInfo)(tableSchema, sample),
											relationships: convertForeignKeysToRelationships(dbName, tableName, tableInfo.foreignKeys || [])
										};
									}).then(({ jsonSchema, relationships }) => {
										logger.progress({ message: `Schema has created successfully`, containerName: dbName, entityName: tableName });
										
										return getPrimaryKeys(dbName, tableName)
											.then(keys => {
												keys.forEach(key => {
													jsonSchema.properties[key.COLUMN_NAME].primaryKey = true;
												});

												return jsonSchema;
											})
											.then(jsonSchema => {
												logger.progress({ message: `Primary keys have retrieved successfully`, containerName: dbName, entityName: tableName });

												return ({ jsonSchema, relationships });
											})
											.catch(err => {
												return Promise.resolve({ jsonSchema, relationships });
											});
									}).then(({ jsonSchema, relationships }) => {
										return query(`show indexes on ${tableName}`)
											.then(result => {
												return getIndexes(result);
											})
											.then(indexes => {
												logger.progress({ message: `Indexes have retrieved successfully`, containerName: dbName, entityName: tableName });
												
												documentPackage.entityLevel.SecIndxs = indexes;

												return { jsonSchema, relationships };
											})
											.catch(err => ({ jsonSchema, relationships }));
									}).then(({ jsonSchema, relationships }) => {
										if (jsonSchema) {
											documentPackage.validation = { jsonSchema };
										}

										return {
											documentPackage,
											relationships
										};
									});
								})
								.then((data) => {
									nextTable(null, data);
								})
								.catch(err => {
									nextTable(err)
								});
						}, (err, data) => {
							if (err) {
								nextDb(err);
							} else {
								nextDb(err, expandPackages(data));
							}
						});
					});
			}, (err, data) => {
				if (err) {
					logger.log('error', { message: err.message, stack: err.stack, error: err }, 'Retrieving databases and tables information');

					setTimeout(() => {
						cb(err);
					}, 1000);
				} else {
					cb(err, ...expandFinalPackages(data));
				}
			});
		}, app);
	}
};

const retrieveData = (query, tableName, limit, offset) => {
	return query(`select * from ${tableName} limit ${limit} offset ${offset}`).then(data => data, error => {
		if (typeof error !== 'string') {
			return Promise.reject(error);
		} else if (error.includes('missing EOF at \'offset\'')) {
			return query(`select * from ${tableName} limit ${limit}`);
		}
	});
};

const logInfo = (step, connectionInfo, logger) => {
	logger.clear();
	logger.log('info', logHelper.getSystemInfo(connectionInfo.appVersion), step);
	logger.log('info', connectionInfo, 'connectionInfo', connectionInfo.hiddenKeys);
};

const expandPackages = (packages) => {
	return packages.reduce((result, pack) => {
		result.documentPackage.push(pack.documentPackage);
		result.relationships = result.relationships.concat(pack.relationships);

		return result;
	}, { documentPackage: [], relationships: [] });
};

const expandFinalPackages = (packages) => {
	return packages.reduce((result, pack) => {
		result[0] = [...result[0], ...pack.documentPackage];
		result[2] = [...result[2], ...pack.relationships];

		return result;
	}, [[], null, []])
};

const getLimitByCount = (recordSamplingSettings, getCount) => new Promise((resolve, reject) => {
	if (recordSamplingSettings.active !== 'relative') {
		const absolute = Number(recordSamplingSettings.absolute.value);

		return resolve(absolute);
	}

	getCount().then((data) => {
		const count = data[0].count;
		const limit = Math.ceil((count * Number(recordSamplingSettings.relative.value)) / 100);
	
		resolve(limit);
	}).catch(reject);
});

const getPages = (total, pageSize) => {
	const generate = (size) => size <= 0 ? [0] : [...generate(size - 1), size];

	return generate(Math.ceil(total / pageSize) - 1);
};

const getDataByPagination = (pagination, limit, callback) => new Promise((resolve, reject) => {
	const getResult = (err, data) => err ? reject(err) : resolve(data);
	const pageSize = Number(pagination.value);

	if (!pagination.enabled) {
		return callback(limit, 0, getResult);
	}

	async.reduce(
		getPages(limit, pageSize),
		[],
		(result, page, next) => {
			callback(pageSize, page, (err, data) => {
				if (err) {
					next(err);
				} else {
					next(null, result.concat(data));
				}
			});
		},
		getResult
	);
});

const allChain = (...promises) => {
	let result = [];

	return promises.reduce((promise, next, i) => {
		return promise.then((data) => {
			if (i !== 0) {
				result.push(data);
			}

			return next();
		});
	}, Promise.resolve())
	.then(data => {
		result.push(data);

		return result;
	});
};

const getExecutorWithResult = (cursor, handler) => {
	const resultParser = hiveHelper.getResultParser(...cursor.getTCLIService());
	
	return (...args) => {
		return handler(...args).then(resp => {
			return allChain(
				() => cursor.fetchResult(resp),
				() => cursor.getSchema(resp)
			);
		}).then(([ resultResp, schemaResp ]) => {
			return resultParser(schemaResp, resultResp)
		});
	};
};

const convertForeignKeysToRelationships = (childDbName, childCollection, foreignKeys) => {
	return foreignKeys.map(foreignKey => ({
		relationshipName: foreignKey.name,
		dbName: foreignKey.parentDb,
		parentCollection: foreignKey.parentTable,
		parentField: foreignKey.parentField,
		childDbName: childDbName,
		childCollection: childCollection,
		childField: foreignKey.childField
	}));
};

const getIndexes = (indexesFromDb) => {
	const getValue = (value) => (value || '').trim();
	const getIndexHandler = (idxType) => {
		if (!idxType) {
			return 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler';
		}
		
		if (idxType === 'compact') {
			return 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler';
		}

		return idxType;
	};

	const getInTable = (tableName) => {
		return 'IN TABLE ' + tableName;
	};

	return (indexesFromDb || []).map(indexFromDb => {
		return {
			name: getValue(indexFromDb.idx_name),
			SecIndxKey: getValue(indexFromDb.col_names).split(',').map(name => ({ name: getValue(name) })),
			SecIndxTable: getInTable(getValue(indexFromDb.idx_tab_name)),
			SecIndxHandler: getIndexHandler(getValue(indexFromDb.idx_type)),
			SecIndxComments: getValue(indexFromDb.comment)
		};
	});
};

const getAuthorityCertificates = (options) => {
	const getFile = (filePath) => {
		if (!fs.existsSync(filePath)) {
			return "";
		} else {
			return fs.readFileSync(filePath);
		}
	};

	return {
		ca: getFile(options.sslCaFile),
		cert: getFile(options.sslCertFile),
		key: getFile(options.sslKeyFile),
	};
};

const getKeystoreCertificates = (options, app) => new Promise((resolve, reject) => {
	app.require('java-ssl', (err, Keystore) => {
		if (err) {
			return reject(err);
		}

		const store = Keystore(options.keystore, options.keystorepass);
		const caText = (store.getCert(options.alias) || '').replace(/\s*-----END CERTIFICATE-----$/, '\n-----END CERTIFICATE-----');
		const ca = caText;
		const cert = caText;
		const key = store.getPrivateKey(options.alias);
	
		return resolve({
			cert,
			key,
			ca,
		});
	});
});

const getSslCerts = (options, app) => {
	if (options.ssl === 'jks') {
		return getKeystoreCertificates(options, app);
	} else if (isSsl(options.ssl)) {
		return Promise.resolve(getAuthorityCertificates(options));
	} else {
		return Promise.resolve({
			cert: '',
			key: '',
			ca: ''
		});
	}
};

const isSsl = (ssl) => ssl && ssl !== 'false' && ssl !== 'https';
