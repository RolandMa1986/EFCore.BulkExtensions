using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EFCore.BulkExtensions.SqlAdapters.SQLite;
/// <inheritdoc/>
public class SqliteOperationsAdapter : ISqlOperationsAdapter
{
    /// <inheritdoc/>
    #region Methods
    // Insert
    public void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }


    /// <inheritdoc/>
    public async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken)
    {
        await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public static async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
    {
        SqliteConnection? connection = (SqliteConnection?)SqlAdaptersMapping.DbServer!.DbConnection;
        if (connection == null)
        {
            connection = isAsync ? await OpenAndGetSqliteConnectionAsync(context, cancellationToken).ConfigureAwait(false)
                                 : OpenAndGetSqliteConnection(context);
        }
        bool doExplicitCommit = false;

        try
        {
            if (context.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }

            SqliteTransaction? transaction = (SqliteTransaction?)SqlAdaptersMapping.DbServer!.DbTransaction;
            if (transaction == null)
            {
                var dbTransaction = doExplicitCommit ? connection.BeginTransaction()
                                                     : context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);

                transaction = (SqliteTransaction?)dbTransaction;
            }
            else
            {
                doExplicitCommit = false;
            }

            var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

            var (properties, entityPropertiesDict, entityShadowFkPropertiesDict, entityShadowFkPropertyColumnNamesDict, shadowPropertyColumnNamesDict) = GetInsertPropterties(context, type, tableInfo);

            var propertiesToLoad = properties
                .Where(a => !tableInfo.AllNavigationsDictionary.ContainsKey(a.Name)
                            || entityShadowFkPropertiesDict.ContainsKey(a.Name)
                            || tableInfo.OwnedTypesDict.ContainsKey(a.Name)).ToArray();

            int rowsCopied = 0;

            foreach (var item in entities)
            {
                LoadSqliteValues(tableInfo, item, command, context, propertiesToLoad, entityPropertiesDict, entityShadowFkPropertiesDict, entityShadowFkPropertyColumnNamesDict, shadowPropertyColumnNamesDict);

                if (isAsync)
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    command.ExecuteNonQuery();
                }
                ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
            }
            if (doExplicitCommit)
            {
                transaction?.Commit();
            }
        }
        finally
        {
            if (doExplicitCommit)
            {
                if (isAsync)
                {
                    await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
                else
                {
                    context.Database.CloseConnection();
                }
            }
        }
    }

    // Merge
    /// <inheritdoc/>
    public void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress) where T : class
    {
        MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: true, cancellationToken);
    }

    /// <inheritdoc/>
    protected static async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        SqliteConnection connection = isAsync ? await OpenAndGetSqliteConnectionAsync(context, cancellationToken).ConfigureAwait(false)
                                                    : OpenAndGetSqliteConnection(context);
        bool doExplicitCommit = false;

        try
        {
            if (context.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }
            var dbTransaction = doExplicitCommit ? connection.BeginTransaction()
                                                 : context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);
            var transaction = (SqliteTransaction?)dbTransaction;

            var command = GetSqliteCommand(context, type, entities, tableInfo, connection, transaction);

            var (properties, entityPropertiesDict, entityShadowFkPropertiesDict, entityShadowFkPropertyColumnNamesDict, shadowPropertyColumnNamesDict) = GetInsertPropterties(context, type, tableInfo);

            var propertiesToLoad = properties
                .Where(a => !tableInfo.AllNavigationsDictionary.ContainsKey(a.Name)
                            || entityShadowFkPropertiesDict.ContainsKey(a.Name)
                            || tableInfo.OwnedTypesDict.ContainsKey(a.Name)).ToArray();

            int rowsCopied = 0;

            foreach (var item in entities)
            {
                LoadSqliteValues(tableInfo, item, command, context, propertiesToLoad, entityPropertiesDict, entityShadowFkPropertiesDict, entityShadowFkPropertyColumnNamesDict, shadowPropertyColumnNamesDict);
                if (isAsync)
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    command.ExecuteNonQuery();
                }
                ProgressHelper.SetProgress(ref rowsCopied, entities.Count, tableInfo.BulkConfig, progress);
            }

            if (operationType == OperationType.Insert && tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null) // For Sqlite Identity can be set by Db only with pure Insert method
            {
                command.CommandText = SqlQueryBuilderSqlite.SelectLastInsertRowId();

                object? lastRowIdScalar = isAsync ? await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)
                                                       : command.ExecuteScalar();

                SetIdentityForOutput(entities, tableInfo, lastRowIdScalar);
            }

            if (doExplicitCommit)
            {
                transaction?.Commit();
            }
        }
        finally
        {
            if (isAsync)
            {
                await context.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
            else
            {
                context.Database.CloseConnection();
            }
        }
    }

    // Read
    /// <inheritdoc/>
    public void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        ReadAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await ReadAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    protected static async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        SqliteConnection connection = isAsync ? await OpenAndGetSqliteConnectionAsync(context, cancellationToken).ConfigureAwait(false)
                                                    : OpenAndGetSqliteConnection(context);
        bool doExplicitCommit = false;
        SqliteTransaction? transaction = null;

        try
        {
            if (context.Database.CurrentTransaction == null)
            {
                //context.Database.UseTransaction(connection.BeginTransaction());
                doExplicitCommit = true;
            }

            transaction = doExplicitCommit ? connection.BeginTransaction()
                                           : (SqliteTransaction?)context.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);

            SqliteCommand command = connection.CreateCommand();
            command.Transaction = transaction;

            // CREATE
            command.CommandText = SqlQueryBuilderSqlite.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName);
            if (isAsync)
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                command.ExecuteNonQuery();
            }

            tableInfo.BulkConfig.OperationType = OperationType.Insert;
            tableInfo.InsertToTempTable = true;
            SqlAdaptersMapping.DbServer!.DbConnection = connection;
            SqlAdaptersMapping.DbServer!.DbTransaction = transaction;
            // INSERT
            if (isAsync)
            {
                await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, cancellationToken).GetAwaiter().GetResult();
            }

            // JOIN
            List<T> existingEntities;
            var sqlSelectJoinTable = SqlQueryBuilder.SelectJoinTable(tableInfo);
            Expression<Func<DbContext, IQueryable<T>>> expression = tableInfo.GetQueryExpression<T>(sqlSelectJoinTable, false);
            var compiled = EF.CompileQuery(expression); // instead using Compiled queries
            existingEntities = compiled(context).ToList();

            tableInfo.UpdateReadEntities(entities, existingEntities, context);

            // DROP
            command.CommandText = SqlQueryBuilderSqlite.DropTable(tableInfo.FullTempTableName);
            if (isAsync)
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                command.ExecuteNonQuery();
            }

            if (doExplicitCommit)
            {
                transaction?.Commit();
            }
        }
        finally
        {
            if (doExplicitCommit)
            {
                if (isAsync)
                {
                    if (transaction is not null)
                    {
                        await transaction.DisposeAsync();
                    }

                    await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
                else
                {
                    transaction?.Dispose();
                    context.Database.CloseConnection();
                }
            }
        }
    }

    /// <inheritdoc/>
    public void Truncate(DbContext context, TableInfo tableInfo)
    {
        string sql = SqlQueryBuilder.DeleteTable(tableInfo.FullTableName);
        context.Database.ExecuteSqlRaw(sql);
    }

    /// <inheritdoc/>
    public async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        string sql = SqlQueryBuilder.DeleteTable(tableInfo.FullTableName);
        await context.Database.ExecuteSqlRawAsync(sql, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    #region Connection
    internal static async Task<SqliteConnection> OpenAndGetSqliteConnectionAsync(DbContext context, CancellationToken cancellationToken)
    {
        await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return (SqliteConnection)context.Database.GetDbConnection();
    }

    internal static SqliteConnection OpenAndGetSqliteConnection(DbContext context)
    {
        context.Database.OpenConnection();

        return (SqliteConnection)context.Database.GetDbConnection();
    }
    #endregion

    #region SqliteData
    internal static SqliteCommand GetSqliteCommand<T>(DbContext context, Type? type, IList<T> entities, TableInfo tableInfo, SqliteConnection connection, SqliteTransaction? transaction)
    {
        SqliteCommand command = connection.CreateCommand();
        command.Transaction = transaction;

        var operationType = tableInfo.BulkConfig.OperationType;

        switch (operationType)
        {
            case OperationType.Insert:
                command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.Insert);
                break;
            case OperationType.InsertOrUpdate:
                command.CommandText = SqlQueryBuilderSqlite.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);
                break;
            case OperationType.InsertOrUpdateOrDelete:
                throw new NotSupportedException("'BulkInsertOrUpdateDelete' not supported for Sqlite. Sqlite has only UPSERT statement (analog for MERGE WHEN MATCHED) but no functionality for: 'WHEN NOT MATCHED BY SOURCE THEN DELETE'." +
                                                " Another way to achieve this is to BulkRead existing data from DB, split list into sublists and call separately Bulk methods for Insert, Update, Delete.");
            case OperationType.Update:
                command.CommandText = SqlQueryBuilderSqlite.UpdateSetTable(tableInfo);
                break;
            case OperationType.Delete:
                command.CommandText = SqlQueryBuilderSqlite.DeleteFromTable(tableInfo);
                break;
        }

        var objectIdentifier = tableInfo.ObjectIdentifier;
        type = tableInfo.HasAbstractList ? entities[0]?.GetType() : type;
        if (type is null)
        {
            throw new ArgumentException("Unable to determine entity type");
        }

        var (properties, entityPropertiesDict, entityShadowFkPropertiesDict, entityShadowFkPropertyColumnNamesDict, shadowPropertyColumnNamesDict) = GetInsertPropterties(context, type, tableInfo);

        foreach (var property in properties)
        {
            var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                && !tableInfo.BulkConfig.SetOutputIdentity
                && tableInfo.DefaultValueProperties.Contains(property.Name);

            if (entityPropertiesDict.ContainsKey(property.Name))
            {
                var propertyEntityType = entityPropertiesDict[property.Name];
                string columnName = propertyEntityType.GetColumnName(objectIdentifier) ?? string.Empty;
                var propertyType = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;

                //SqliteType(CpropertyType.Name): Text(String, Decimal, DateTime); Integer(Int16, Int32, Int64) Real(Float, Double) Blob(Guid)
                var parameter = new SqliteParameter($"@{columnName}", propertyType); // ,sqliteType // ,null //()
                command.Parameters.Add(parameter);
            }
            else if (entityShadowFkPropertiesDict.ContainsKey(property.Name))
            {
                var fk = entityShadowFkPropertiesDict[property.Name];

                entityPropertiesDict.TryGetValue(fk.GetColumnName(objectIdentifier) ?? string.Empty, out var entityProperty);
                if (entityProperty == null) // BulkRead
                    continue;

                var columnName = entityProperty.GetColumnName(objectIdentifier);

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName ?? string.Empty);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName ?? string.Empty].ProviderClrType : entityProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (columnName is not null && !hasDefaultVauleOnInsert)
                {
                    var parameter = new SqliteParameter($"@{columnName}", propertyType); // ,sqliteType // ,null //()
                    command.Parameters.Add(parameter);
                }
            }
        }

        var shadowProperties = tableInfo.ShadowProperties;
        foreach (var shadowProperty in shadowProperties)
        {
            var parameter = new SqliteParameter($"@{shadowProperty}", typeof(string));
            command.Parameters.Add(parameter);
        }

        command.Prepare(); // Not Required but called for efficiency (prepared should be little faster)
        return command;
    }

    private static (PropertyInfo[], Dictionary<string, IProperty>, Dictionary<string, IProperty>, Dictionary<string, string?>, Dictionary<string, string?>) GetInsertPropterties(DbContext context, Type type, TableInfo tableInfo )
    {
        var entityType = context.Model.FindEntityType(type) ?? throw new ArgumentException($"Unable to determine entity type from given type - {type.Name}");
        var entityTypeProperties = entityType.GetProperties();
        var entityPropertiesDict = entityTypeProperties.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

        var entityShadowFkPropertiesDict = entityTypeProperties.Where(a => a.IsShadowProperty() &&
                                                                   a.IsForeignKey() &&
                                                                   a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
                                                             .ToDictionary(x => x.GetContainingForeignKeys()?.First()?.DependentToPrincipal?.Name ?? string.Empty, a => a);
        var entityShadowFkPropertyColumnNamesDict = entityShadowFkPropertiesDict
            .ToDictionary(a => a.Key, a => a.Value.GetColumnName(tableInfo.ObjectIdentifier));
        var shadowPropertyColumnNamesDict = entityPropertiesDict
            .Where(a => a.Value.IsShadowProperty()).ToDictionary(a => a.Key, a => a.Value.GetColumnName(tableInfo.ObjectIdentifier));

        return (properties, entityPropertiesDict, entityShadowFkPropertiesDict, entityShadowFkPropertyColumnNamesDict, shadowPropertyColumnNamesDict);
    }

    internal static void LoadSqliteValues<T>(TableInfo tableInfo, T? entity, SqliteCommand command, DbContext dbContext, IEnumerable<PropertyInfo> properties, Dictionary<string, IProperty> entityPropertiesDict, Dictionary<string, IProperty> entityShadowFkPropertiesDict, Dictionary<string, string?> entityShadowFkPropertyColumnNamesDict, Dictionary<string, string?> shadowPropertyColumnNamesDict)
    { 
        foreach (var propertyColumn in properties)
        {
            string parameterName = propertyColumn.Name;
            object? value;
            if (parameterName.Contains('_')) // ToDo: change IF clause to check for NavigationProperties, optimise, integrate with same code segment from LoadData method
            {
                var ownedPropertyNameList = parameterName.Split('_');
                var ownedPropertyName = ownedPropertyNameList[0];
                var subPropertyName = ownedPropertyNameList[1];
                var ownedFastProperty = tableInfo.FastPropertyDict[ownedPropertyName];
                var ownedProperty = ownedFastProperty.Property;

                var propertyType = Nullable.GetUnderlyingType(ownedProperty.GetType()) ?? ownedProperty.GetType();
                if (!command.Parameters.Contains("@" + parameterName))
                {
                    var parameter = new SqliteParameter($"@{parameterName}", propertyType);
                    command.Parameters.Add(parameter);
                }

                if (ownedProperty == null)
                {
                    value = null;
                }
                else
                {
                    var ownedPropertyValue = entity == null ? null : tableInfo.FastPropertyDict[ownedPropertyName].Get(entity);
                    var subPropertyFullName = $"{ownedPropertyName}_{subPropertyName}";
                    value = ownedPropertyValue == null ? null : tableInfo.FastPropertyDict[subPropertyFullName]?.Get(ownedPropertyValue);
                }
            }
            else
            {
                if (entityPropertiesDict.ContainsKey(parameterName))
                {
                    value = tableInfo.FastPropertyDict.ContainsKey(parameterName)
                        ? tableInfo.FastPropertyDict[parameterName].Get(entity!)
                        : null;
                }
                else if (entityShadowFkPropertiesDict.ContainsKey(parameterName))
                {
                    var foreignKeyShadowProperty = entityShadowFkPropertiesDict[parameterName];
                    var columnName = entityShadowFkPropertyColumnNamesDict[parameterName] ?? string.Empty;
                    if (!entityPropertiesDict.TryGetValue(columnName, out var entityProperty) || entityProperty is null)
                    {
                        continue; // BulkRead
                    };
                    value = tableInfo.FastPropertyDict.ContainsKey(parameterName)
                        ? tableInfo.FastPropertyDict[parameterName].Get(entity!)
                        : null;
                    parameterName = columnName;
                    value = value == null
                        ? null
                        : foreignKeyShadowProperty.FindFirstPrincipal()?.PropertyInfo?.GetValue(value); // TODO Check if can be optimized
                }
                else {
                    value = null;
                }
            }


            if (value is not null) {
                if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(parameterName) && value != DBNull.Value)
                {
                    value = tableInfo.ConvertibleColumnConverterDict[parameterName].ConvertToProvider.Invoke(value);
                }

                command.Parameters[$"@{parameterName}"].Value = value ?? DBNull.Value;
            }
        }

        if (tableInfo.BulkConfig.EnableShadowProperties)
        {
            foreach (var shadowPropertyName in shadowPropertyColumnNamesDict.Keys)
            {
                var shadowProperty = entityPropertiesDict[shadowPropertyName];
                var columnName = shadowPropertyColumnNamesDict[shadowPropertyName] ?? string.Empty;

                var propertyValue = default(object);

                if (tableInfo.BulkConfig.ShadowPropertyValue == null)
                {
                    propertyValue = dbContext.Entry(entity!).Property(shadowPropertyName).CurrentValue;
                }
                else
                {
                    propertyValue = tableInfo.BulkConfig.ShadowPropertyValue(entity!, shadowPropertyName);
                }

                if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName))
                {
                    propertyValue = tableInfo.ConvertibleColumnConverterDict[columnName].ConvertToProvider.Invoke(propertyValue);
                }
                command.Parameters[$"@{shadowPropertyName}"].Value = propertyValue ?? DBNull.Value;
            }
        }
        //else
        //{
        //    value = entity is null ? null : dbContext.Entry(entity).Metadata.GetDiscriminatorValue(); // Set the value for the discriminator column
        //}
    }

    /// <inheritdoc/>
    public static void SetIdentityForOutput<T>(IList<T> entities, TableInfo tableInfo, object? lastRowIdScalar)
    {
        long counter = (long?)lastRowIdScalar ?? 0;

        string identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
        FastProperty identityFastProperty = tableInfo.FastPropertyDict[identityPropertyName];

        string idTypeName = identityFastProperty.Property.PropertyType.Name;
        object? idValue = null;
        for (int i = entities.Count - 1; i >= 0; i--)
        {
            idValue = idTypeName switch
            {
                "Int64" => counter, // long is default
                "UInt64" => (ulong)counter,
                "Int32" => (int)counter,
                "UInt32" => (uint)counter,
                "Int16" => (short)counter,
                "UInt16" => (ushort)counter,
                "Byte" => (byte)counter,
                "SByte" => (sbyte)counter,
                _ => counter,
            };
            if (entities[i] is not null)
            {
                identityFastProperty.Set(entities[i]!, idValue);
            }

            counter--;
        }
    }
    #endregion
}
