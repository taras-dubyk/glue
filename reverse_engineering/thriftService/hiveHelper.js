const _ = require('lodash');
const Big = require('big.js');
const schemaHelper = require('./schemaHelper');

const getInt64 = (buffer, offset) => {
	// This code from Int64 toNumber function. Using Big.js, convert to string.
	const b = buffer;
	const o = offset;

	// Running sum of octets, doing a 2's complement
	const negate = b[o] & 0x80;
	let value = new Big(0);
	let m = new Big(1);
	let carry = 1;

	for (let i = 7; i >= 0; i -= 1) {
	  let v = b[o + i];

	  // 2's complement for negative numbers
	  if (negate) {
		v = (v ^ 0xff) + carry;
		carry = v >> 8;
		v &= 0xff;
	  }

	  value = value.plus((new Big(v)).times(m));
	  m = m.times(256);
	}

	if (negate) {
	  value = value.times(-1);
	}

	return value;
};

const getColumnValueKeyByTypeDescriptor = (TCLIServiceTypes) =>  (typeDescriptor) => {
	switch (typeDescriptor.type) {
		case TCLIServiceTypes.TTypeId.BOOLEAN_TYPE:
			return 'boolVal';
		case TCLIServiceTypes.TTypeId.TINYINT_TYPE:
			return 'byteVal';
		case TCLIServiceTypes.TTypeId.SMALLINT_TYPE:
			return 'i16Val';
		case TCLIServiceTypes.TTypeId.INT_TYPE:
			return 'i32Val';
		case TCLIServiceTypes.TTypeId.BIGINT_TYPE:
		case TCLIServiceTypes.TTypeId.TIMESTAMP_TYPE:
			return 'i64Val';
		case TCLIServiceTypes.TTypeId.FLOAT_TYPE:
		case TCLIServiceTypes.TTypeId.DOUBLE_TYPE:
			return 'doubleVal';
		case TCLIServiceTypes.TTypeId.BINARY_TYPE:
			return 'binaryVal';
		default:
			return 'stringVal';
	}
};

const noConversion = value => value;
const toString = value => value.toString();
const convertBigInt = value => {
	const result = getInt64(value.buffer, value.offset);
	const max = new Big(Number.MAX_SAFE_INTEGER);

	if (result.cmp(max) > 0) {
		return Number.MAX_SAFE_INTEGER;
	} else {
		return parseInt(result.toString());
	}
};
const toNumber = value => Number(value);
const toJSON = defaultValue => value => {
	try {
		return JSON.parse(value);
	} catch (e) {
		return defaultValue;
	}
};

const getDataConverter = (TCLIServiceTypes) => (typeDescriptor) => {
	switch (typeDescriptor.type) {
		case TCLIServiceTypes.TTypeId.NULL_TYPE:
			return noConversion;
		case TCLIServiceTypes.TTypeId.UNION_TYPE:
		case TCLIServiceTypes.TTypeId.USER_DEFINED_TYPE:
			return toString;

		case TCLIServiceTypes.TTypeId.DECIMAL_TYPE:
			return toNumber;
		case TCLIServiceTypes.TTypeId.STRUCT_TYPE:
		case TCLIServiceTypes.TTypeId.MAP_TYPE:
			return toJSON({});
		case TCLIServiceTypes.TTypeId.ARRAY_TYPE:
			return toJSON([]);
		
		case TCLIServiceTypes.TTypeId.BIGINT_TYPE:
			return convertBigInt;
		case TCLIServiceTypes.TTypeId.TIMESTAMP_TYPE:
		case TCLIServiceTypes.TTypeId.DATE_TYPE:
			return toString;
		case TCLIServiceTypes.TTypeId.BINARY_TYPE:
		case TCLIServiceTypes.TTypeId.INTERVAL_YEAR_MONTH_TYPE:
		case TCLIServiceTypes.TTypeId.INTERVAL_DAY_TIME_TYPE:
		case TCLIServiceTypes.TTypeId.FLOAT_TYPE:
		case TCLIServiceTypes.TTypeId.DOUBLE_TYPE:
		case TCLIServiceTypes.TTypeId.INT_TYPE:
		case TCLIServiceTypes.TTypeId.SMALLINT_TYPE:
		case TCLIServiceTypes.TTypeId.TINYINT_TYPE:
		case TCLIServiceTypes.TTypeId.BOOLEAN_TYPE:
		case TCLIServiceTypes.TTypeId.STRING_TYPE:
		case TCLIServiceTypes.TTypeId.CHAR_TYPE:
		case TCLIServiceTypes.TTypeId.VARCHAR_TYPE:
		default:
			return noConversion;
	}
};

const getTypeDescriptorByColumnDescriptor = (columnDescriptor) => {
	return _.get(columnDescriptor, 'typeDesc.types[0].primitiveEntry', null);
};

const getColumnValuesBySchema = (TCLIServiceTypes) => (columnDescriptor, valuesColumn) => {
	const typeDescriptor = getTypeDescriptorByColumnDescriptor(columnDescriptor);
	const valueType = getColumnValueKeyByTypeDescriptor(TCLIServiceTypes)(typeDescriptor);
	const values = _.get(valuesColumn, `${valueType}.values`, []);

	return values.map(getDataConverter(TCLIServiceTypes)(typeDescriptor));
};

const getColumnName = (columnDescriptor) => {
	const name = columnDescriptor.columnName || '';

	return name.split('.').pop();
};

const getResultParser = (TCLIService, TCLIServiceTypes) => {
	return (schemaResponse, fetchResultResponses) => {
		return fetchResultResponses.reduce((result, fetchResultResponse) => {
			const columnValues = _.get(fetchResultResponse, 'results.columns', []);
			const rows = [...schemaResponse.schema.columns]
				.sort((c1, c2) => c1.position > c2.position ? 1 : c1.position < c2.position ? -1 : 0)
				.reduce((rows, columnDescriptor) => {
					return getColumnValuesBySchema(TCLIServiceTypes)(
						columnDescriptor,
						columnValues[columnDescriptor.position - 1]
					).reduce((result, columnValue, i) => {
						if (!result[i]) {
							result[i] = {};
						}

						result[i][getColumnName(columnDescriptor)] = columnValue;

						return result;
					}, rows);
				}, []);

			return result.concat(rows);
		}, []);
	};
};

const getQualifier = (typeDescriptor, qualifierName, defaultValue) => {
	const result = _.get(typeDescriptor, `typeQualifiers.qualifiers.${qualifierName}`, {});

	return result.i32Value || result.stringValue || defaultValue;
};

const getJsonSchemaByTypeDescriptor = (TCLIServiceTypes) => (typeDescriptor) => {
	switch (typeDescriptor.type) {
		case TCLIServiceTypes.TTypeId.NULL_TYPE:		
		case TCLIServiceTypes.TTypeId.STRING_TYPE:
			return {
				type: "text",
				mode: "string"
			};
		case TCLIServiceTypes.TTypeId.VARCHAR_TYPE:
			return {
				type: "text",
				mode: "varchar",
				maxLength: getQualifier(typeDescriptor, "characterMaximumLength", "")
			};
		case TCLIServiceTypes.TTypeId.CHAR_TYPE:
			return {
				type: "text",
				mode: "char",
				maxLength: getQualifier(typeDescriptor, "characterMaximumLength", "")
			};
		case TCLIServiceTypes.TTypeId.INT_TYPE:
			return {
				type: "numeric",
				mode: "int"
			};
		case TCLIServiceTypes.TTypeId.TINYINT_TYPE:
			return {
				type: "numeric",
				mode: "tinyint"
			};
		case TCLIServiceTypes.TTypeId.SMALLINT_TYPE:
			return {
				type: "numeric",
				mode: "smallint"
			};
		case TCLIServiceTypes.TTypeId.BIGINT_TYPE:
			return {
				type: "numeric",
				mode: "bigint"
			};
		case TCLIServiceTypes.TTypeId.FLOAT_TYPE:
			return {
				type: "numeric",
				mode: "float"
			};
		case TCLIServiceTypes.TTypeId.DOUBLE_TYPE:
			return {
				type: "numeric",
				mode: "double"
			};
		case TCLIServiceTypes.TTypeId.DECIMAL_TYPE:
			return {
				type: "numeric",
				mode: "decimal",
				precision: getQualifier(typeDescriptor, "precision", ""),
				scale: getQualifier(typeDescriptor, "scale", "")
			};
		case TCLIServiceTypes.TTypeId.BOOLEAN_TYPE:
			return {
				type: "bool"
			};
		case TCLIServiceTypes.TTypeId.BINARY_TYPE:
			return {
				type: "binary"
			};
		case TCLIServiceTypes.TTypeId.TIMESTAMP_TYPE:
			return {
				type: "timestamp"
			};
		case TCLIServiceTypes.TTypeId.DATE_TYPE:
			return {
				type: "date"
			};
		case TCLIServiceTypes.TTypeId.ARRAY_TYPE:
			return {
				type: "array",
				subtype: "array<txt>",
				items: []
			};
		case TCLIServiceTypes.TTypeId.MAP_TYPE:
			return {
				type: "map",
				keySubtype: "string",
				subtype: "map<txt>",
				properties: {}
			};
		case TCLIServiceTypes.TTypeId.STRUCT_TYPE:
			return {
				type: "struct",
				properties: {}
			};
		case TCLIServiceTypes.TTypeId.INTERVAL_YEAR_MONTH_TYPE:
		case TCLIServiceTypes.TTypeId.INTERVAL_DAY_TIME_TYPE:
			return {
				type: "interval"
			};
		case TCLIServiceTypes.TTypeId.UNION_TYPE:
		case TCLIServiceTypes.TTypeId.USER_DEFINED_TYPE:
		default:
			return {
				type: "string"
			};
	}
};

const getJsonSchemaCreator = (TCLIService, TCLIServiceTypes, tableInfo) => (schemaResp, sample) => {
	const columnDescriptors = _.get(schemaResp, 'schema.columns', []);

	const jsonSchema = columnDescriptors.reduce((jsonSchema, columnDescriptor) => {
		const typeDescriptor = getTypeDescriptorByColumnDescriptor(columnDescriptor);
		const columnName = getColumnName(columnDescriptor);
		const schema = Object.assign(
			{},
			getJsonSchemaByTypeDescriptor(TCLIServiceTypes)(typeDescriptor),
			{ comments: columnDescriptor.comment || "" }
		);
		const jsonSchemaFromInfo = tableInfo.table[columnName] 
			? schemaHelper.getJsonSchema(tableInfo.table[columnName], _.get(sample, columnName))
			: {};

		if (jsonSchemaFromInfo.type === 'union') {
			return schemaHelper.getChoice(jsonSchema, jsonSchemaFromInfo.subSchemas, columnName);
		} else {
			jsonSchema.properties[columnName] = Object.assign(
				{},
				schema,
				jsonSchemaFromInfo
			);
		}

		return jsonSchema;
	}, {
		$schema: "http://json-schema.org/draft-04/schema#",
		type: "object",
		additionalProperties: false,
		properties: {}
	});

	return jsonSchema;
};

const getIterator = (hiveResult) => {
	let i = 0;
	return () => hiveResult[i++];
};

const isDivider = (column) => {
	return !column || Object.keys(column).every(item => !column[item]);
};

const getColumn = (column, next) => {
	if (isDivider(column)) {
		return {};
	}

	return Object.assign(
		{},
		{ [column.col_name]: column.data_type },
		getColumn(next(), next)
	);
};

const getTable = (next, skipColumn) => {
	const header = next();

	if (skipColumn) {
		next();
	}

	return getColumn(next(), next);
};

const getPartitionInfo = (next, skipColumn) => {
	return getTable(next, skipColumn);
};

const isTableParameter = (column) => {
	return column.col_name === "Table Parameters:";
};

const getTableParameters = (next) => {
	const column = next();
	if (isDivider(column)) {
		return {};
	} else {
		return Object.assign(getTableParameters(next), { [column.data_type.trim()]: column.comment });
	}
};

const getDetailedInfo = (next) => {
	const column = next();
	if (isDivider(column)) {
		return {};
	}
	if (isTableParameter(column)) {
		return {
			[column.col_name.trim().slice(0, -1)]: getTableParameters(next)
		};
	}

	return Object.assign(
		{[column.col_name.trim().slice(0, -1)]: column.data_type},
		getDetailedInfo(next)
	);
};

const getStorageInfo = (next) => {
	const column = next();
	if (isDivider(column)) {
		return {};
	}
	if (isStorageDesc(column)) {
		return {
			[column.col_name.trim().slice(0, -1)]: getTableParameters(next)
		};
	}

	return Object.assign(
		{[column.col_name.trim().slice(0, -1)]: column.data_type},
		getStorageInfo(next)
	);
};

const getForeignKeys = (next) => {
	const header = next();
	const getForeignKey = (next) => {
		const column = next();

		if (isDivider(column)) {
			return [];
		}

		const parentItem = column.col_name.split(":").pop() || "";
		const [ dbName, tableName, fieldName ] = parentItem.split(".");
		const childField = (column.data_type.split(":").pop() || "").trim();

		return [{
			parentDb: dbName,
			parentTable: tableName,
			parentField: fieldName,
			childField
		}, ...getForeignKey(next)];
	};
	const getConstraint = (next) => {
		const column = next();

		if (isDivider(column)) {
			return [];
		}

		if (!isConstraint(column)) {
			return [];
		}

		const constraintName = (column.data_type || "").trim();

		return [...getForeignKey(next).map(foreignKey => {
			return Object.assign(foreignKey, {
				name: constraintName
			});
		}), ...getConstraint(next)];
	};
	
	return getConstraint(next);
};

const isConstraint = (column) => (column.col_name || "").trim() === "Constraint Name:";

const isStorageDesc = (column) => column.col_name === "Storage Desc Params:"

const isPartitionInfo = (column) => {
	return (column.col_name === "# Partition Information");
};

const isDetailedTableInformation = (column) => {
	return (column.col_name === "# Detailed Table Information");
};

const isStorageInfo = (column) => {
	return (column.col_name === "# Storage Information");
};

const isForeignKey = (column) => {
	return (column.col_name === "# Foreign Keys");
};

const handleRow = (column, next, skipColumn) => {
	if (isPartitionInfo(column)) {
		return { partitionInfo: getPartitionInfo(next, skipColumn) };
	} else if (isDetailedTableInformation(column)) {
		return { detailedInfo: getDetailedInfo(next) };
	} else if (isStorageInfo(column)) {
		return { storageInfo: getStorageInfo(next) };
	} else if (isForeignKey(column)) {
		return { foreignKeys: getForeignKeys(next) };
	}
};

const getFormattedTable = (TCLIService, TCLIServiceTypes, currentProtocol) => (hiveResult) => {
	const next = getIterator(hiveResult);
	const skipColumn = (currentProtocol < TCLIServiceTypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9);

	const table = getTable(next, skipColumn);
	let currentColumn = next();
	let result = { table }

	while (currentColumn) {
		result = Object.assign(result, handleRow(currentColumn, next, skipColumn));
		currentColumn = next();
	}
	
	return result;
};

const getDetailInfoFromExtendedTable = (extendedTable) => {
	const isDetailExtendedTableInformation = (column) => column.col_name === "Detailed Table Information";
	const next = getIterator(extendedTable);
	let column;

	while (column = next()) {
		if (isDetailExtendedTableInformation(column)) {
			return column.data_type;
		}
	}
};

module.exports = {
	getResultParser,
	getJsonSchemaCreator,
	getFormattedTable,
	getDetailInfoFromExtendedTable
};