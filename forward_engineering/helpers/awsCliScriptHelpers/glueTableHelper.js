const { CLI, CREATE_TABLE } = require('./cliConstants');
const { getGlueTableColumns, getGluePartitionKeyTableColumns, getGlueTableClusteringKeyColumns, getGlueTableSortingColumns } = require('./glueColumnHelper');

const getGlueTableCreateStatement = (tableSchema, databaseName) => {
	const tableParameters = {
		DatabaseName: databaseName,
		TableInput: {
			Name: tableSchema.title,
			Description: tableSchema.description,
			StorageDescriptor: {
				Columns: getGlueTableColumns(tableSchema.properties),
				Location: tableSchema.location,
				InputFormat: tableSchema.inputFormatClassname,
				OutputFormat: tableSchema.outputFormatClassname,
				Compressed: tableSchema.compressed,
				NumberOfBuckets: tableSchema.numBuckets,
				SerdeInfo: mapSerdeInfo(tableSchema),
				BucketColumns: getGlueTableClusteringKeyColumns(tableSchema.properties),
				SortColumns: getGlueTableSortingColumns(tableSchema.sortedByKey, tableSchema.properties),
				// Parameters: {
				// 	KeyName: ''
				// },
				// SkewedInfo: {
				// 	SkewedColumnNames: [''],
				// 	SkewedColumnValues: [''],
				// 	SkewedColumnValueLocationMaps: {
				// 		KeyName: ''
				// 	}
				// },
				StoredAsSubDirectories: tableSchema.StoredAsSubDirectories
			},
			PartitionKeys: getGluePartitionKeyTableColumns(tableSchema.properties),
			TableType: tableSchema.externalTable ? 'EXTERNAL_TABLE' : ''
		}
	};

	const cliStatement = `${CLI} ${CREATE_TABLE} '${JSON.stringify(tableParameters, null, 2)}'`;
	return cliStatement;
};

const mapSerdeInfo = (tableSchema) => {
	return {
		SerializationLibrary: tableSchema.serDeLibrary,
		Parameters: {
			paths: getSerdePathParams(tableSchema.parameterPaths, tableSchema.properties)
		}
	};
}

const getSerdePathParams = (parameterPaths = [], properties) => {
	return parameterPaths.map(({ keyId }) => {
		const property = Object.entries(properties).find(([key, value]) => value.GUID === keyId);
		const propertyName = property && property[0];
		return propertyName;
	}).join(',');
}

module.exports = {
	getGlueTableCreateStatement
};
