using CommonLibrary;
using DTO;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MSTest
{
    public class SqlBulkCopyHelper
    {
        //加载appsetting.json
        static IConfiguration configuration = new ConfigurationBuilder()
      .SetBasePath(Directory.GetCurrentDirectory())
     .AddJsonFile("appsettings.json").Build();
        /// <summary>
        /// 数据库连接字符串，配置文件在appsettings.json文件中
        /// </summary>
        private static readonly string connectionString = configuration["DBSetting:ConnectString"];
        private const string tempTablePre = "#Temp";

        /// <summary>
        /// 执行SqlBulkCopy批量插入，执行事务。
        /// </summary>
        /// <param name="connectionString">数据连接</param>
        /// <param name="TableName">表名</param>
        /// <param name="dt">要插入的数据</param>
        /// <returns></returns>
        public async static Task<string> BulkInsertTables(List<DataTable> insertTables)
        {
            var sqlBulkCopyList = new List<SqlBulkCopy>();
            try
            {
                var insertTasks = insertTables.Select(async o =>
                {
                    var sqlbulkcopy = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.CheckConstraints) { BulkCopyTimeout = 600 };
                    sqlBulkCopyList.Add(sqlbulkcopy);
                    sqlbulkcopy.DestinationTableName = o.TableName;
                    foreach (DataColumn item in o.Columns)
                        sqlbulkcopy.ColumnMappings.Add(item.ColumnName, item.ColumnName);
                    await sqlbulkcopy.WriteToServerAsync(o);
                });
                await Task.WhenAll(insertTasks);
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
            finally
            {
                foreach (var item in sqlBulkCopyList)
                    item.Close();
            }
            return string.Empty;
        }

        /// <summary>
        /// 执行SqlBulkCopy批量更新，执行事务。
        /// </summary>
        /// <param name="bulkTables"></param>
        /// <returns></returns>
        public async static Task<string> BulkUpdateTables(List<BulkTable> bulkTables)
        {
            var updateTables = bulkTables.Select(o => o.Table).ToList();

            foreach (var item in bulkTables)
                if (string.IsNullOrEmpty(item.TableName))
                    item.TableName = item.Table.TableName;

            foreach (var item in bulkTables)
                item.UpdateFields = item.UpdateFields.Except(item.RemoveFields).ToList();

            var tempTableSuf = DateTime.Now.ToString("yyyyMMddHHmmss");
            Func<DataTable, BulkTable, bool> func = (o, p) => o.TableName == p.TableName;
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(string.Empty, conn) { CommandTimeout = 600 })
                {
                    var sqlBulkCopyList = new List<SqlBulkCopy>();
                    try
                    {
                        updateTables.ForEach(o =>
                        {
                            var bulkTable = bulkTables.FirstOrDefault(p => func(o, p));
                            cmd.CommandText += string.Format(@"SELECT A.* into {0} from {1} A WHERE 1=2;", tempTablePre + bulkTable.TableName + tempTableSuf, bulkTable.TableName);
                            foreach (DataColumn item in o.Columns) bulkTable.TableFields.Add(item.ColumnName);
                        });
                        await cmd.ExecuteNonQueryAsync();

                        cmd.CommandText = default;
                        var updateTasks = updateTables.Select(async o =>
                        {
                            var bulkTable = bulkTables.FirstOrDefault(p => func(o, p));
                            var sqlBulkcopy = new SqlBulkCopy(conn) { BulkCopyTimeout = 600 };
                            sqlBulkCopyList.Add(sqlBulkcopy);
                            sqlBulkcopy.DestinationTableName = tempTablePre + bulkTable.TableName + tempTableSuf;
                            foreach (var item in bulkTable.TableFields)
                                sqlBulkcopy.ColumnMappings.Add(item, item);
                            await sqlBulkcopy.WriteToServerAsync(o);

                            var tempSql = new StringBuilder();
                            if (bulkTable.UpdateFields.Count > 0)
                                foreach (var column in bulkTable.UpdateFields) tempSql.Append(string.Format(@"A.{0} = B.{0},", column));
                            else
                            {
                                foreach (var column in bulkTable.TableFields) tempSql.Append(string.Format(@"A.{0} = B.{0},", column));
                            }
                            cmd.CommandText += string.Format(@"UPDATE A SET {0} FROM dbo.{1} A INNER JOIN {2} B ON A.{3}=B.{3};", tempSql.ToString().Trim(','), bulkTable.TableName, tempTablePre + bulkTable.TableName + tempTableSuf, bulkTable.Primary);
                        });
                        await Task.WhenAll(updateTasks);
                        await cmd.ExecuteNonQueryAsync();
                    }
                    catch (Exception ex)
                    {
                        return ex.Message;
                    }
                    finally
                    {
                        cmd.CommandText = default;
                        foreach (var item in bulkTables)
                            cmd.CommandText += string.Format(@"if object_id('tempdb..{0}') is not null Begin drop table {0} End;", tempTablePre + item.TableName + tempTableSuf);
                        await cmd.ExecuteNonQueryAsync();

                        foreach (var item in sqlBulkCopyList)
                            item.Close();
                    }
                }
            }
            return string.Empty;
        }

        /// <summary>
        /// 执行SqlBulkCopy批量新增+更新，执行事务。
        /// </summary>
        /// <param name="bulkTables"></param>
        /// <returns></returns>
        public async static Task<string> BulkEditTables(List<DataTable> insertTables, List<BulkTable> bulkTables)
        {
            var updateTables = bulkTables.Select(o => o.Table).ToList();
            foreach (var item in bulkTables)
                if (string.IsNullOrEmpty(item.TableName))
                    item.TableName = item.Table.TableName;

            foreach (var item in bulkTables)
                item.UpdateFields = item.UpdateFields.Except(item.RemoveFields).ToList();

            var tempTableSuf = DateTime.Now.ToString("yyyyMMddHHmmss");
            Func<DataTable, BulkTable, bool> func = (o, p) => o.TableName == p.TableName;
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand(string.Empty, conn) { CommandTimeout = 600 })
                {
                    var sqlBulkCopyList = new List<SqlBulkCopy>();
                    try
                    {
                        updateTables.ForEach(o =>
                        {
                            var bulkTable = bulkTables.FirstOrDefault(p => func(o, p));
                            cmd.CommandText += string.Format(@"SELECT A.* into {0} from {1} A WHERE 1=2;", tempTablePre + bulkTable.TableName + tempTableSuf, bulkTable.TableName);
                            foreach (DataColumn item in o.Columns) bulkTable.TableFields.Add(item.ColumnName);
                        });
                        await cmd.ExecuteNonQueryAsync();

                        var insertTasks = insertTables.Select(async o =>
                        {
                            var sqlbulkcopy = new SqlBulkCopy(conn) { BulkCopyTimeout = 600 };
                            sqlBulkCopyList.Add(sqlbulkcopy);
                            sqlbulkcopy.DestinationTableName = o.TableName;
                            foreach (DataColumn item in o.Columns) sqlbulkcopy.ColumnMappings.Add(item.ColumnName, item.ColumnName);
                            await sqlbulkcopy.WriteToServerAsync(o);
                        });

                        cmd.CommandText = default;
                        var updateTasks = updateTables.Select(async o =>
                        {
                            var bulkTable = bulkTables.FirstOrDefault(p => func(o, p));
                            var sqlBulkcopy = new SqlBulkCopy(conn) { BulkCopyTimeout = 600 };
                            sqlBulkCopyList.Add(sqlBulkcopy);
                            sqlBulkcopy.DestinationTableName = tempTablePre + bulkTable.TableName + tempTableSuf;
                            foreach (var item in bulkTable.TableFields)
                                sqlBulkcopy.ColumnMappings.Add(item, item);
                            await sqlBulkcopy.WriteToServerAsync(o);
                            var tempSql = new StringBuilder();
                            if (bulkTable.UpdateFields.Count > 0)
                                foreach (var column in bulkTable.UpdateFields) tempSql.Append(string.Format(@"A.{0} = B.{0},", column));
                            else
                            {
                                foreach (var column in bulkTable.TableFields) tempSql.Append(string.Format(@"A.{0} = B.{0},", column));
                            }
                            cmd.CommandText += string.Format(@"UPDATE A SET {0} FROM dbo.{1} A INNER JOIN {2} B ON A.{3}=B.{3};", tempSql.ToString().Trim(','), bulkTable.TableName, tempTablePre + bulkTable.TableName + tempTableSuf, bulkTable.Primary);
                        });
                        await Task.WhenAll(updateTasks.Concat(insertTasks));
                        await cmd.ExecuteNonQueryAsync();
                    }
                    catch (Exception ex)
                    {
                        return ex.Message;
                    }
                    finally
                    {
                        cmd.CommandText = default;
                        foreach (var item in bulkTables)
                            cmd.CommandText += string.Format(@"if object_id('tempdb..{0}') is not null Begin drop table {0} End;", tempTablePre + item.TableName + tempTableSuf);
                        await cmd.ExecuteNonQueryAsync();

                        foreach (var item in sqlBulkCopyList)
                            item.Close();
                    }
                }
            }
            return string.Empty;
        }

        #region SqlBulkCopy
        /// <summary>
        /// 数据列表转成DataTable
        /// </summary>
        /// <typeparam name="TModel"></typeparam>
        /// <param name="modelList"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static DataTable ListToTable<TModel>(List<TModel> modelList, string tableName = "")
        {
            Type modelType = typeof(TModel);
            if (string.IsNullOrEmpty(tableName))
                tableName = modelType.Name;
            DataTable dt = new DataTable(tableName);
            var columns = GetTableColumns(tableName);
            var mappingProps = new List<PropertyInfo>();

            var props = modelType.GetProperties();
            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];
                PropertyInfo mappingProp = props.Where(a => a.Name == column.Name).FirstOrDefault();
                Type dataType = default;
                if (mappingProp == default)
                    dataType = SqlBulkCopyHelper.SqlTypeString2CsharpType(column.Type);
                else
                {
                    mappingProps.Add(mappingProp);
                    if (column.IsNull == 0)
                        dataType = mappingProp.PropertyType;
                    else
                        dataType = Nullable.GetUnderlyingType(mappingProp.PropertyType) ?? mappingProp.PropertyType;
                }
                if (dataType.IsEnum)
                    dataType = typeof(int);
                var dataColumn = new DataColumn(column.Name, dataType);
                if (column.IsNull == 0)
                {
                    if (dataType == typeof(string))
                        dataColumn.DefaultValue = string.IsNullOrEmpty(column.Default) ? string.Empty : column.Default;
                    if (dataType == typeof(int) || dataType == typeof(int?))
                        dataColumn.DefaultValue = string.IsNullOrEmpty(column.Default) ? 0 : int.Parse(column.Default);
                    if (dataType == typeof(decimal) || dataType == typeof(decimal?))
                        dataColumn.DefaultValue = string.IsNullOrEmpty(column.Default) ? 0 : decimal.Parse(column.Default);
                    if (dataType == typeof(double) || dataType == typeof(double?))
                        dataColumn.DefaultValue = string.IsNullOrEmpty(column.Default) ? 0 : double.Parse(column.Default);
                    if (dataType == typeof(bool) || dataType == typeof(bool?))
                        dataColumn.DefaultValue = string.IsNullOrEmpty(column.Default) ? false : bool.Parse(column.Default);
                    if (dataType == typeof(DateTime) || dataType == typeof(DateTime?))
                        dataColumn.DefaultValue = DateTime.Now;
                    if (dataType == typeof(Guid) || dataType == typeof(Guid?))
                        dataColumn.DefaultValue = Guid.Empty;
                }
                dt.Columns.Add(dataColumn);
            }

            var mappingPropsNames = mappingProps.Select(o => o.Name).ToList();
            columns.RemoveAll(o => mappingPropsNames.Contains(o.Name));

            foreach (var model in modelList)
            {
                DataRow dr = dt.NewRow();
                for (int i = 0; i < mappingProps.Count; i++)
                {
                    PropertyInfo prop = mappingProps[i];
                    object value = prop.GetValue(model);
                    if (prop.PropertyType.IsEnum)
                        if (value != null)
                            value = (int)value;
                    dr[prop.Name] = value ?? DBNull.Value;
                }
                dt.Rows.Add(dr);
            }
            return dt;
        }
        /// <summary>
        /// 返回指定可以为null的类型的基础类型参数
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static Type GetUnderlyingType(Type type)
        {
            return Nullable.GetUnderlyingType(type) ?? type;
        }
        /// <summary>
        /// sql server中的数据类型，转换为C#中的类型类型
        /// </summary>
        /// <param name="sqlTypeString"></param>
        /// <returns></returns>
        public static Type SqlTypeString2CsharpType(string sqlTypeString)
        {
            SqlDbType dbTpe = SqlTypeToSqlDbType(sqlTypeString);
            return SqlType2CsharpType(dbTpe);
        }

        /// <summary>
        /// 将sql server中的数据类型，转化为C#中的类型的字符串
        /// </summary>
        /// <param name="sqlTypeString"></param>
        /// <returns></returns>
        public static string SqlTypeString2CsharpTypeString(string sqlTypeString)
        {
            Type type = SqlTypeString2CsharpType(sqlTypeString);
            return type.Name;
        }
        /// <summary>
        /// SqlDbType转换为C#数据类型
        /// </summary>
        /// <param name="sqlType"></param>
        /// <returns></returns>
        public static Type SqlType2CsharpType(SqlDbType sqlType)
        {
            switch (sqlType)
            {
                case SqlDbType.BigInt:
                    return typeof(Int64);
                case SqlDbType.Binary:
                    return typeof(Object);
                case SqlDbType.Bit:
                    return typeof(Boolean);
                case SqlDbType.Char:
                    return typeof(String);
                case SqlDbType.DateTime:
                    return typeof(DateTime);
                case SqlDbType.Decimal:
                    return typeof(Decimal);
                case SqlDbType.Float:
                    return typeof(Double);
                case SqlDbType.Image:
                    return typeof(Object);
                case SqlDbType.Int:
                    return typeof(Int32);
                case SqlDbType.Money:
                    return typeof(Decimal);
                case SqlDbType.NChar:
                    return typeof(String);
                case SqlDbType.NText:
                    return typeof(String);
                case SqlDbType.NVarChar:
                    return typeof(String);
                case SqlDbType.Real:
                    return typeof(Single);
                case SqlDbType.SmallDateTime:
                    return typeof(DateTime);
                case SqlDbType.SmallInt:
                    return typeof(Int16);
                case SqlDbType.SmallMoney:
                    return typeof(Decimal);
                case SqlDbType.Text:
                    return typeof(String);
                case SqlDbType.Timestamp:
                    return typeof(Object);
                case SqlDbType.TinyInt:
                    return typeof(Byte);
                case SqlDbType.Udt://自定义的数据类型
                    return typeof(Object);
                case SqlDbType.UniqueIdentifier:
                    return typeof(Object);
                case SqlDbType.VarBinary:
                    return typeof(Object);
                case SqlDbType.VarChar:
                    return typeof(String);
                case SqlDbType.Variant:
                    return typeof(Object);
                case SqlDbType.Xml:
                    return typeof(Object);
                default:
                    return null;
            }
        }
        /// <summary>
        /// sql server数据类型（如：varchar）转换为SqlDbType类型
        /// </summary>
        /// <param name="sqlTypeString"></param>
        /// <returns></returns>
        public static SqlDbType SqlTypeToSqlDbType(string sqlTypeString)
        {
            SqlDbType dbType = SqlDbType.Variant;//默认为Object

            switch (sqlTypeString)
            {
                case "int":
                    dbType = SqlDbType.Int;
                    break;
                case "varchar":
                    dbType = SqlDbType.VarChar;
                    break;
                case "bit":
                    dbType = SqlDbType.Bit;
                    break;
                case "datetime":
                    dbType = SqlDbType.DateTime;
                    break;
                case "decimal":
                    dbType = SqlDbType.Decimal;
                    break;
                case "float":
                    dbType = SqlDbType.Float;
                    break;
                case "image":
                    dbType = SqlDbType.Image;
                    break;
                case "money":
                    dbType = SqlDbType.Money;
                    break;
                case "ntext":
                    dbType = SqlDbType.NText;
                    break;
                case "nvarchar":
                    dbType = SqlDbType.NVarChar;
                    break;
                case "smalldatetime":
                    dbType = SqlDbType.SmallDateTime;
                    break;
                case "smallint":
                    dbType = SqlDbType.SmallInt;
                    break;
                case "text":
                    dbType = SqlDbType.Text;
                    break;
                case "bigint":
                    dbType = SqlDbType.BigInt;
                    break;
                case "binary":
                    dbType = SqlDbType.Binary;
                    break;
                case "char":
                    dbType = SqlDbType.Char;
                    break;
                case "nchar":
                    dbType = SqlDbType.NChar;
                    break;
                case "numeric":
                    dbType = SqlDbType.Decimal;
                    break;
                case "real":
                    dbType = SqlDbType.Real;
                    break;
                case "smallmoney":
                    dbType = SqlDbType.SmallMoney;
                    break;
                case "sql_variant":
                    dbType = SqlDbType.Variant;
                    break;
                case "timestamp":
                    dbType = SqlDbType.Timestamp;
                    break;
                case "tinyint":
                    dbType = SqlDbType.TinyInt;
                    break;
                case "uniqueidentifier":
                    dbType = SqlDbType.UniqueIdentifier;
                    break;
                case "varbinary":
                    dbType = SqlDbType.VarBinary;
                    break;
                case "xml":
                    dbType = SqlDbType.Xml;
                    break;
            }
            return dbType;
        }
        /// <summary>
        /// 获取数据库表的所有列
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static List<SysColumn> GetTableColumns(string tableName)
        {
            string sql = string.Format(@"select a.name,a.colorder,c.DATA_TYPE,a.isnullable,SUBSTRING(d.text,3,1) defaultValue from 
                syscolumns a inner join sysobjects b on a.id=b.id 
                LEFT JOIN dbo.syscomments d ON a.cdefault  = d.id
                inner join information_schema.columns c  on b.name=c.TABLE_NAME and c.COLUMN_NAME=a.name 
                where b.xtype='U' and b.name='{0}' order by a.colid asc", tableName);

            List<SysColumn> columns = new List<SysColumn>();
            DataTable dt = SqlCoreHelper.ExecuteDataSetText(sql, null).Tables[0];
            foreach (DataRow reader in dt.Rows)
            {
                SysColumn column = new SysColumn();
                column.Name = reader[0].ToString();
                column.ColOrder = Convert.ToInt16(reader[1]);
                column.Type = reader[2].ToString();
                column.IsNull = Convert.ToInt32(reader[3]);
                column.Default = reader[4].ToString();
                columns.Add(column);
            }
            return columns;
        }
        #endregion

        #region old不带事务
        /// <summary>
        /// SqlBulkCopy 批量插入数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="tableName"></param>
        public async static Task BulkInsertList<T>(List<T> list, string tableName)
        {
            var dataTable = ConvertListToTable(list);
            ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.PerUserRoamingAndLocal);
            using (var bulkCopy = new SqlBulkCopy(connectionString))
            {
                foreach (DataColumn dcPrepped in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(dcPrepped.ColumnName, dcPrepped.ColumnName);
                }
                bulkCopy.BulkCopyTimeout = 660;
                bulkCopy.DestinationTableName = tableName;
                await bulkCopy.WriteToServerAsync(dataTable);
            }
        }

        /// <summary>
        /// 本地认证评估表建表SQL
        /// </summary>
        private const string CreateTemplateSql = @"[Id] [int] NOT NULL,[DisabilityCardId] [nvarchar](50) NOT NULL,[PartId] [nvarchar](32) NULL,[ProvinceCode] [nvarchar](4) NULL,[DisabilityLevel] [int] NULL,[DisabilityTypes] [nvarchar](16) NULL,[VisualDisabilityLevel] [int] NULL";

        /// <summary>
        /// 本地认证评估更新SQL 这里采用的merge语言更新语句 你也可以使用 sql update 语句
        /// </summary>
        private const string UpdateSql = @"Merge into DisabilityAssessmentInfo AS T 
Using #TmpTable AS S 
ON T.Id = S.Id
WHEN MATCHED 
THEN UPDATE SET T.[DisabilityCardId]=S.[DisabilityCardId],T.[PartId]=S.[PartId],T.[ProvinceCode]=S.[ProvinceCode],T.[DisabilityLevel]=S.[DisabilityLevel],T.[DisabilityTypes]=S.[DisabilityTypes],T.[VisualDisabilityLevel]=S.[VisualDisabilityLevel];";
        /// <summary>
        /// SqlBulkCopy 批量更新数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="crateTemplateSql"></param>
        /// <param name="updateSql"></param>
        public static void BulkUpdateData<T>(List<T> list, string crateTemplateSql, string updateSql)
        {
            var dataTable = ConvertListToTable(list);
            ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.PerUserRoamingAndLocal);
            using (var conn = new SqlConnection(connectionString))
            {
                using (var command = new SqlCommand("", conn))
                {
                    try
                    {
                        conn.Open();
                        //数据库并创建一个临时表来保存数据表的数据
                        command.CommandText = $"  CREATE TABLE #TmpTable ({crateTemplateSql})";
                        command.ExecuteNonQuery();

                        //使用SqlBulkCopy 加载数据到临时表中
                        using (var bulkCopy = new SqlBulkCopy(conn))
                        {
                            foreach (DataColumn dcPrepped in dataTable.Columns)
                            {
                                bulkCopy.ColumnMappings.Add(dcPrepped.ColumnName, dcPrepped.ColumnName);
                            }

                            bulkCopy.BulkCopyTimeout = 660;
                            bulkCopy.DestinationTableName = "#TmpTable";
                            bulkCopy.WriteToServer(dataTable);
                            bulkCopy.Close();
                        }

                        // 执行Command命令 使用临时表的数据去更新目标表中的数据  然后删除临时表
                        command.CommandTimeout = 600;
                        command.CommandText = updateSql;
                        command.ExecuteNonQuery();
                    }
                    finally
                    {
                        conn.Close();
                    }
                }
            }
        }
        public static DataTable ConvertListToTable<T>(IList<T> data)
        {
            var properties = TypeDescriptor.GetProperties(typeof(T));
            var table = new DataTable();

            foreach (PropertyDescriptor prop in properties)
                table.Columns.Add(prop.Name, Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType);
            foreach (T item in data)
            {
                var row = table.NewRow();

                foreach (PropertyDescriptor prop in properties)
                {
                    row[prop.Name] = prop.GetValue(item) ?? DBNull.Value;
                }

                table.Rows.Add(row);
            }
            return table;
        }
        #endregion

    }
}
