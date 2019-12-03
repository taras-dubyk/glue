const thrift = require('thrift');
const createKerberosConnection = require('./hackolade/saslConnectionService').createKerberosConnection;
const createLdapConnection = require('./hackolade/saslConnectionService').createLdapConnection;
const createHttpConnection = require('./hackolade/httpConnection').createHttpConnection;
const createKerberosHttpConnection = require('./hackolade/kerberosHttpConnection').createKerberosHttpConnection;

const getConnectionByMechanism = (authMech, mode) => {
	if (mode === 'http') {
		return {
			protocol: thrift.TBinaryProtocol,
			transport: thrift.TBufferedTransport
		};
	} else if (authMech === 'NOSASL') {
		return {
			transport: thrift.TBufferedTransport,
			protocol: thrift.TBinaryProtocol
		};
	} else if (authMech === 'GSSAPI') {
		return {
			protocol: thrift.TBinaryProtocol,
			transport: thrift.TFramedTransport
		};
	} else if (authMech === 'LDAP') {
		return {
			protocol: thrift.TBinaryProtocol,
			transport: thrift.TFramedTransport
		};
	} else if (authMech === 'PLAIN') {
		return {
			protocol: thrift.TBinaryProtocol,
			transport: thrift.TFramedTransport
		};
	} else {
		throw new Error("The authentication mechanism " + authMech + " is not supported!");
	}
};

const cacheCall = (func) => {
	let cache = null;

	return (...args) => {
		if (!cache) {
			cache = func(...args);

			if (cache instanceof Promise) {
				return cache.then(result => {
					cache = Promise.resolve(result);
					
					return result;
				});
			}
		}

		return cache;
	};
};

const getConnection = cacheCall((connectionData = {}) => {
	const TCLIService = connectionData.TCLIService;
	const kerberosAuthProcess = connectionData.kerberosAuthProcess;
	const kerberos = connectionData.kerberos;
	const parameters = connectionData.parameters;
	const logger = connectionData.logger;
	const host = parameters.host;
	const port = parameters.port;
	const authMech = parameters.authMech;
	const mode = parameters.mode;
	const options = parameters.options;

	let connectionHandler = options.ssl ? thrift.createSSLConnection : thrift.createConnection;

	if (mode === 'http') {
		connectionHandler = createHttpConnection;
	}

	if (authMech === 'GSSAPI') {
		if (mode === 'http') {
			connectionHandler = createKerberosHttpConnection(kerberos, logger);
		} else {
			connectionHandler = createKerberosConnection(kerberosAuthProcess, logger);
		}
	}

	if (authMech === 'LDAP' && mode !== 'http') {
		connectionHandler = createLdapConnection(kerberosAuthProcess);
	}

	if (authMech === 'PLAIN' && mode !== 'http') {
		options.username = options.username || 'anonymous';
		options.password = options.password || 'anonymous';

		connectionHandler = createLdapConnection(kerberosAuthProcess);
	}

	const connection = connectionHandler(host, port, Object.assign({
		https: false,
		debug: true,
		max_attempts: 1,
		retry_max_delay: 2,
		connect_timeout: 1000,
		timeout: 1000
	}, getConnectionByMechanism(authMech, mode), (options || {})));
	
	return (
		connection instanceof Promise 
			? connection
			: Promise.resolve(connection)
	).then(connection => {
		return thrift.createClient(TCLIService, connection);
	});
});

const getProtocolByVersion = cacheCall((TCLIServiceTypes, version) => {
	if (version === '2.x') {
		return TCLIServiceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8;
	} else {
		return TCLIServiceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9;
	}
});

const getConnectionParamsByMode = (mode, data) => {
	if (mode === 'http' && data.authMech === 'GSSAPI') {
		return getKerberosHttpConnectionParams(data);
	} else if (mode === 'http') {
		return getHttpConnectionParams(data);
	} else if (data.authMech === 'GSSAPI') {
		return getKerberosConnectionParams(data);
	} else if (data.authMech === 'LDAP') {
		return getLdapConnectionParams(data);
	} else {
		return getBinaryConnectionParams(data);
	}
};

const getBinaryConnectionParams = ({ host, port, authMech, options }) => {
	return { host, port, authMech, options, mode: 'binary' };
};

const getKerberosHttpConnectionParams = (data) => {
	const httpParameters = getHttpConnectionParams(data);
	const kerberosParameters = (getKerberosConnectionParams(data).options || {}).krb5;

	return {
		host: data.host,
		port: data.port,
		authMech: data.authMech,
		mode: 'http',
		options: Object.assign({}, httpParameters.options, {
			krb5: kerberosParameters
		})
	};
};

const getHttpConnectionParams = ({ host, port, username, password, authMech, options }) => {
	const headers = options.headers || {};

	if (authMech === 'PLAIN' || authMech === 'NOSASL' || authMech === 'LDAP') {
		username = username || 'anonymous';
		password = password || 'anonymous';
	}

	if (username && password) {
		headers['Authorization'] = 'Basic ' + Buffer.from(`${username}:${password}`).toString('base64');
	}

	return {
		host,
		port,
		authMech,
		mode: 'http',
		options: Object.assign(
			{},
			options,
			{
				headers,
				nodeOptions: options.https ? {
					ca: options.ca,
					cert: options.cert,
					key: options.key
				} : {}
			}
		)
	};	
};

const getKerberosConnectionParams = ({ host, port, username, password, authMech, options, configuration }) => {
	return {
		options: Object.assign({}, options, {
			krb5: {
				krb_service: configuration.krb_service,
				krb_host: configuration.krb_host,
				username,
				password
			}
		}),
		mode: 'binary',
		host,
		port,
		authMech,
		
	};
};

const getLdapConnectionParams = ({ host, port, username, password, authMech, options, configuration }) => {
	return {
		options: Object.assign({}, options, {
			username,
			password
		}),
		mode: 'binary',
		authMech,
		host,
		port,
	};
};

const promisifyCallback = (operation) => new Promise((resolve, reject) => {
	operation((err, res) => {
		if (err) {
			reject(err);
		} else {
			resolve(res);
		}
	});
});

const createSessionRequest = cacheCall((TCLIServiceTypes, options) => {
	return new TCLIServiceTypes.TOpenSessionReq(options);
});

const filterConfiguration = (configuration) => {
	return Object.keys(configuration).filter(key => configuration[key] !== undefined).reduce((result, key) => Object.assign({}, result, {
		[key]: configuration[key]
	}), {});
};

const connect = ({ host, port, username, password, authMech, version, options, configuration, mode }) => (handler) => (TCLIService, TCLIServiceTypes, logger, kerberosAuthProcess, kerberos) => {
	const connectionsParams = getConnectionParamsByMode(mode, { host, port, username, password, authMech, version, options, mode, configuration });
	const protocol = getProtocolByVersion(TCLIServiceTypes, version);

	const execute = (sessionHandle, statement, options = {}) => {
		const requestOptions = Object.assign({
			sessionHandle,
			statement,
			confOverlay: undefined,
			runAsync: false,
			queryTimeout: 100000
		}, options);
		const request = new TCLIServiceTypes.TExecuteStatementReq(requestOptions);
	
		return getConnection().then(client => promisifyCallback((callback) => {
			client.ExecuteStatement(request, (err, res) => {
				if (err) {
					callback(err);
				} else if (res.status.statusCode === TCLIServiceTypes.TStatusCode.ERROR_STATUS) {
					callback(res.status.errorMessage);
				} else {
					callback(null, res);
				}
			});
		}));
	};
	
	const asyncExecute = (sessionHandle, statement, options = {}) => {
		return execute(sessionHandle, statement, Object.assign({}, options, { runAsync: true }))
			.then((res) => {
				return waitFinish(res.operationHandle)
					.then(() => {
						return Promise.resolve(res);
					});
			});
	};
	
	const fetchResult = (executeStatementResponse, limit = 100) => {
		if (!executeStatementResponse.operationHandle.hasResultSet) {
			return Promise.resolve([]);
		}
	
		const getResult = (res, next) => {
			if (typeof next === 'function') {
				next()
					.then(getResult)
					.then(prevRes => [...res, ...prevRes]);
			} else {
				return Promise.resolve(res);
			}
		};
	
		return fetchFirstResult(executeStatementResponse.operationHandle, limit)
			.then(res => {
				if (res.hasMoreRows) {
					return Promise.resolve([res], () => {
						return fetchNextResult(executeStatementResponse.operationHandle, limit);
					});
				} else {
					return Promise.resolve([res]);
				}
			})
			.then(getResult);
	};
	
	const getSchema = (executeStatementResponse) => {
		if (!executeStatementResponse.operationHandle) {
			return Promise.reject(new Error('operation handle does not exist'));
		}
	
		const request = new TCLIServiceTypes.TGetResultSetMetadataReq(executeStatementResponse);
	
		return getConnection().then(client => promisifyCallback((callback) => {
			client.GetResultSetMetadata(request, callback);
		}));
	};

	const getSchemas = (sessionHandle) => {
		const request = new TCLIServiceTypes.TGetSchemasReq({ sessionHandle });

		return getConnection().then(client => promisifyCallback((callback) => {
			client.GetSchemas(request, callback);
		}));
	};

	const getPrimaryKeys = (sessionHandle, schemaName, tableName) => {
		const request = new TCLIServiceTypes.TGetPrimaryKeysReq({ sessionHandle, schemaName, tableName });

		return getConnection()
			.then(client => promisifyCallback((callback) => {
				client.GetPrimaryKeys(request, callback);
			}));
	};

	const getCatalogs = (sessionHandle) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TGetCatalogsReq({ sessionHandle });

		getConnection().then(client => client.GetCatalogs(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		}));
	});

	const getTables = (sessionHandle, schemaName, tableTypes) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TGetTablesReq({ sessionHandle, schemaName, tableTypes, tableName: '_%' });

		getConnection().then(client => client.GetTables(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		}));
	});

	const getTableTypes = (sessionHandle) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TGetTableTypesReq({ sessionHandle });

		getConnection().then(client => client.GetTableTypes(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		}));
	});

	const getTypeInfo = (sessionHandle) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TGetTypeInfoReq({ sessionHandle });

		getConnection().then(client => client.GetTypeInfo(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		}));
	});

	const getColumns = (sessionHandle, schemaName, tableName) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TGetColumnsReq({ sessionHandle, schemaName, tableName });

		getConnection().then(client => client.GetColumns(request, (err, res) => {
			if (err) {
				reject(err);
			} else {
				resolve(res);
			}
		}));
	});
	
	const fetchFirstResult = (operationHandle, maxRows) => {
		return fetchResultRequest({
			orientation: TCLIServiceTypes.TFetchOrientation.FETCH_FIRST,
			operationHandle,
			maxRows
		});
	};
	
	const fetchNextResult = (operationHandle, maxRows) => {
		return fetchResultRequest({
			orientation: TCLIServiceTypes.TFetchOrientation.FETCH_NEXT,
			operationHandle,
			maxRows
		});
	};
	
	const fetchResultRequest = (options) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TFetchResultsReq(options);	
	
		getConnection().then(client => {
			return client.FetchResults(request, (err, res) => {
				if (err) {
					reject(err);
				} else {
					resolve(res);
				}
			});
		});	
	});
	
	const isFinish = (status) => {
		return (
			status === TCLIServiceTypes.TOperationState.FINISHED_STATE ||
			status === TCLIServiceTypes.TOperationState.CANCELED_STATE ||
			status === TCLIServiceTypes.TOperationState.CLOSED_STATE ||
			status === TCLIServiceTypes.TOperationState.ERROR_STATE
		);
	};
	
	const getOperationStatus = (operationHandle) => new Promise((resolve, reject) => {
		const request = new TCLIServiceTypes.TGetOperationStatusReq({ operationHandle });
	
		getConnection().then(client => {
			return client.GetOperationStatus(request, (err, res) => {
				if (err) {
					reject(err);
				} else {
					logger.log({
						info: 'Query status',
						status: res.taskStatus
					});
					resolve(res);
				}
			});
		});
	});
	
	const waitFinish = (operationHandle) => new Promise((resolve, reject) => {
		const repeat = (count) => {
			getOperationStatus(operationHandle)
				.then(res => {
					if (isFinish(res.operationState)) {
						resolve(res);
					} else if (count - 1 <= 0) {
						reject(res);
					} else {
						sleep(1000).then(() => {
							repeat(count - 1);
						});
					}
				})
				.catch(reject);
		};
	
		repeat(60);
	});
	
	const sleep = (timeout) => {
		return new Promise((resolve, reject) => {
			setTimeout(resolve, timeout);
		});
	};

	const getTCLIService = () => [TCLIService, TCLIServiceTypes];
	
	const getCurrentProtocol = () => protocol;
	
	const request = createSessionRequest(TCLIServiceTypes, {
		configuration: filterConfiguration(configuration),
		client_protocol: protocol,
		username,
		password
	});
	const cursor = {
		execute,
		asyncExecute,
		fetchResult,
		getSchema,
		getSchemas,
		getPrimaryKeys,
		getCatalogs,
		getColumns,
		getTables,
		getTableTypes,
		getTypeInfo,
		getTCLIService,
		getCurrentProtocol
	};

	return getConnection({
		parameters: connectionsParams,
		TCLIService,
		kerberosAuthProcess,
		logger,
		kerberos
	})
		.then(client => new Promise((resolve, reject) => {
			logger.log('Connection established successfully');
			logger.log('Starting session...');

			client.OpenSession(request, (err, session) => {
				if (err) {
					logger.log('Session was not started');
				} else {
					logger.log('Session started successfully');
				}

				if (typeof handler === 'function') {
					handler(err, session, cursor);
				} else if (err) {
					reject(err);
				} else {
					resolve({ session, cursor });
				}
			});
		}));
};

module.exports = {
	connect
};