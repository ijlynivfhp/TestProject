using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.IO;
using System.Reflection.PortableExecutable;
using System.Dynamic;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.Collections;
using System.IO.Pipes;
using System.Drawing;
using Microsoft.EntityFrameworkCore.Metadata;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;
using Newtonsoft.Json.Linq;
using System.ComponentModel;
using System.Collections.Concurrent;
using DTO;
using System.Xml.Linq;
using MSTest;
using System.Data.Common;
using System.Threading;
using System.Transactions;

namespace CommonLibrary
{
    /// <summary>
    /// SqlHelper操作类
    /// </summary>
    public sealed partial class SqlHelper
    {
        /// <summary>
        /// 临时表前缀
        /// </summary>
        private const string tempTablePre = "#Temp";

        /// <summary>
        /// appsetting.json加载
        /// </summary>
        private readonly static IConfiguration configuration = new ConfigurationBuilder() .SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json").Build();

        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        private readonly static string connectionString = configuration["DBSetting:ConnectString"];

        /// <summary>
        /// 表字段明细字典集合
        /// </summary>
        private static ConcurrentDictionary<string, List<SysColumn>> tableDic = new ConcurrentDictionary<string, List<SysColumn>>();

        /// <summary>
        /// 批量操作每批次记录数
        /// </summary>
        private const int BatchSize = 2000;

        /// <summary>
        /// 超时时间
        /// </summary>
        private const int CommandTimeOut = 60 * 60 * 1;

        #region 常用增删改查
        /// <summary>
        /// 插入对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        /// <param name="tableName"></param>
        /// <param name="idName"></param>
        /// <returns></returns>
        public static int Add<T>(object model, string tableName = default)
        {
            Type addType = typeof(T);
            var addProperties = addType.GetProperties();

            if (string.IsNullOrEmpty(tableName))
                tableName = addType.Name;

            var idName = GetIdName(tableName, addProperties);

            var dic = ObjToDic(model);
            dic.Remove(idName);

            string columnString = string.Join(",", dic.Select(p => string.Format("{0}", p.Key)));
            string valueString = string.Join(",", dic.Select(p => string.Format("@{0}", p.Key)));
            string sqlStr = $@"insert {tableName} ({columnString}) values ({valueString})
                Select @@IDENTITY AS '{idName}'";

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var command = new SqlCommand(sqlStr, conn);
                SqlParameter[] sqlParameter = dic.Select(p => new SqlParameter(string.Format("@{0}", p.Key), p.Value ?? DBNull.Value)).ToArray();
                command.Parameters.AddRange(sqlParameter);
                var idValue = Convert.ToInt32(command.ExecuteScalar());
                if (addType == model.GetType())
                    addType.GetProperty(idName).SetValue(model, idValue);
                else
                {
                    if (model.GetType().Name == "ExpandoObject")
                    {
                        var dicModel = (IDictionary<string, object>)model;
                        if (dicModel.ContainsKey(idName))
                            dicModel[idName] = idValue;
                        else
                            dicModel.Add(idName, idValue);
                    }
                }
                return idValue;
            }
        }

        /// <summary>
        /// 插入对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="model"></param>
        /// <param name="tableName"></param>
        /// <param name="idName"></param>
        /// <returns></returns>
        public static int Set<T>(object model, object where = default, string tableName = default)
        {
            Type addType = typeof(T);
            var addProperties = addType.GetProperties();

            if (string.IsNullOrEmpty(tableName))
                tableName = addType.Name;

            var dic = ObjToDic(model);
            var whereDic = ObjToDic(where);

            var idName = GetIdName(tableName, addProperties);

            var sqlStr = string.Empty;
            if (dic.ContainsKey(idName))
            {
                var idValue = dic[idName] ?? string.Empty;
                dic.Remove(idName);
                string setStr = string.Join(",", dic.Select(p => string.Format("{0}=@{0}", p.Key)));
                sqlStr = $@"update {tableName} set {setStr} where {idName}={idValue}";
            }
            else
            {
                var whereSql = string.Empty;
                if (whereDic.ContainsKey("WhereSql"))
                {
                    whereSql = whereDic.FirstOrDefault(o => o.Key == "WhereSql").Value.ToString() ?? string.Empty;
                    whereSql = whereSql.TrimStart().ToLower().StartsWith("and") ? whereSql : $"AND {whereSql}";
                    whereDic.Remove("WhereSql");
                }
                else
                {
                    return default;
                }
                string setStr = string.Join(",", dic.Select(p => string.Format("{0}=@{0}", p.Key)));
                sqlStr = $@"update {tableName} set {setStr} where 1=1 {string.Join("", whereDic.Select(o => $" AND {o.Key}={(o.Value.GetType() == typeof(int) || o.Value.GetType() == typeof(long) ? o.Value : $"'{o.Value}'")} "))} {whereSql}";
            }

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var command = new SqlCommand(sqlStr, conn);
                SqlParameter[] sqlParameter = dic.Select(p => new SqlParameter(string.Format("@{0}", p.Key), p.Value ?? DBNull.Value)).ToArray();
                command.Parameters.AddRange(sqlParameter);
                return command.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// 批量新增
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="objList"></param>
        /// <param name="tableName"></param>
        /// <param name="idName"></param>
        /// <returns></returns>
        public static List<int> AddList<T>(List<object> objList, string tableName = default)
        {
            var list = new List<int>();
            try
            {
                foreach (object obj in objList)
                {
                    list.Add(Add<T>(obj, tableName));
                }
            }
            catch { }
            return list;
        }

        /// <summary>
        /// 批量新增
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="objList"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static bool BulkAdd<T>(List<object> objList, string tableName = default)
        {
            try
            {
                Type addType = typeof(T);
                var addProperties = addType.GetProperties();

                if (string.IsNullOrEmpty(tableName))
                    tableName = addType.Name;

                var dt = ObjectToTable(objList, tableName);
                dt.TableName = tableName;
                BulkInsert(dt);
            }
            catch
            {
                return default;
            }
            return true;
        }

        /// <summary>
        /// 批量更新
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="objList"></param>
        /// <param name="where"></param>
        /// <param name="tableName"></param>
        /// <param name="idName"></param>
        /// <returns></returns>
        public static int SetList<T>(List<Model> objList, object where = default, string tableName = default)
        {
            try
            {
                foreach (object obj in objList) Set<T>(obj, where, tableName);
                return objList.Count;
            }
            catch { }
            return default;
        }

        /// <summary>
        /// 批量新增
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="objList"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static bool BulkSet<T>(List<object> objList, string tableName = default)
        {
            try
            {
                Type addType = typeof(T);
                var addProperties = addType.GetProperties();

                if (string.IsNullOrEmpty(tableName))
                    tableName = addType.Name;
                var dt = ObjectToTable(objList, tableName);
                dt.TableName = tableName;
                dt.ExtendedProperties.Add("SQL", $"SELECT TOP(0) * FROM {tableName}");
                BatchUpdate(dt);
            }
            catch
            {
                return default;
            }
            return true;
        }

        /// <summary>
        /// 根据主键，或唯一键（默认取类第一个字段）查询数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="I"></typeparam>
        /// <param name="id"></param>
        /// <param name="tableName"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        public static T GetById<T>(object id, string tableName = default, string fields = "*")
        {
            Type type = typeof(T); var typeName = type.Name;
            var properties = type.GetProperties();

            if (string.IsNullOrEmpty(tableName))
                tableName = type.Name;

            var idName = GetIdName(tableName, properties);

            var idType = id.GetType();

            string sqlStr = string.Format("select {0} from {1} where {2}={3}", fields, tableName, idName, (idType == typeof(int) || idType == typeof(long) ? id : $"'{id}'"));

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                SqlCommand command = new SqlCommand(sqlStr, conn);
                var reader = command.ExecuteReader(CommandBehavior.CloseConnection);//指明了CommandReader然后调用CommandBehavior.CloseConnetion方法来关闭链接
                reader.Read();
                try
                {
                    //动态对象 dynamic,object
                    if (type == typeof(object))
                    {
                        var RowInstance = (IDictionary<string, object>)new ExpandoObject();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string fieldName = reader.GetName(i);
                            var value = reader[fieldName];
                            RowInstance.Add(fieldName, value == DBNull.Value ? null : value);
                        }

                        return (T)RowInstance;
                    }
                    //匿名类 anonymous
                    else if (typeName.Contains("<>") && typeName.Contains("__") && typeName.Contains("AnonymousType")) { }
                    //普通类 class
                    else
                    {
                        T RowInstance = Activator.CreateInstance<T>();//动态创建数据实体对象  
                                                                      //通过反射取得对象所有的Property  
                        foreach (PropertyInfo Property in typeof(T).GetProperties())
                        {
                            //取得当前数据库字段的顺序  
                            int Ordinal = reader.GetOrdinal(Property.Name);
                            if (reader.GetValue(Ordinal) != DBNull.Value)
                            {
                                //将DataReader读取出来的数据填充到对象实体的属性里  
                                Property.SetValue(RowInstance, ChangeType(reader.GetValue(Ordinal), Property.PropertyType), default);
                            }
                        }
                        return RowInstance;
                    }
                }
                catch { }
            }
            return default;
        }

        /// <summary>
        /// 查询泛型实体
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <param name="parms"></param>
        /// <returns></returns>
        public static T Get<T>(string sql, params SqlParameter[] parms)
        {
            var type = typeof(T); var typeName = type.Name;

            using (IDataReader reader = ExecuteDataReader(sql, parms))
            {
                try
                {
                    reader.Read();
                    //动态对象 dynamic,object
                    if (type == typeof(object))
                    {
                        var RowInstance = (IDictionary<string, object>)new ExpandoObject();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string fieldName = reader.GetName(i);
                            var value = reader[fieldName];
                            RowInstance.Add(fieldName, value == DBNull.Value ? null : value);
                        }

                        return (T)RowInstance;
                    }
                    //匿名类 anonymous
                    else if (typeName.Contains("<>") && typeName.Contains("__") && typeName.Contains("AnonymousType")) { }
                    //普通类 class
                    else
                    {
                        T RowInstance = Activator.CreateInstance<T>();//动态创建数据实体对象  
                                                                      //通过反射取得对象所有的Property  
                        foreach (PropertyInfo Property in typeof(T).GetProperties())
                        {
                            //取得当前数据库字段的顺序  
                            int Ordinal = reader.GetOrdinal(Property.Name);
                            if (reader.GetValue(Ordinal) != DBNull.Value)
                            {
                                //将DataReader读取出来的数据填充到对象实体的属性里  
                                Property.SetValue(RowInstance, ChangeType(reader.GetValue(Ordinal), Property.PropertyType), default);
                            }
                        }
                        return RowInstance;
                    }
                }
                catch { }
                finally
                {
                    reader.Close();
                }
                return default;
            }

        }

        /// <summary>
        /// 查询泛型类型集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static List<T> GetList<T>(string sql, params SqlParameter[] parms)
        {
            //实例化一个List<>泛型集合  
            var dataList = new List<T>();

            using (IDataReader reader = ExecuteDataReader(sql, parms))
            {
                try
                {
                    var type = typeof(T);
                    var typeName = type.Name;
                    while (reader.Read())
                    {
                        //动态对象 dynamic,object
                        if (type == typeof(object))
                        {
                            var RowInstance = (IDictionary<string, object>)new ExpandoObject();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                string fieldName = reader.GetName(i);
                                var value = reader[fieldName];
                                RowInstance.Add(fieldName, value == DBNull.Value ? null : value);
                            }
                            dataList.Add((T)RowInstance);
                        }
                        //匿名类 anonymous
                        else if (typeName.Contains("<>") && typeName.Contains("__") && typeName.Contains("AnonymousType")) { }
                        //普通类 class
                        else
                        {
                            T RowInstance = Activator.CreateInstance<T>();//动态创建数据实体对象  
                                                                          //通过反射取得对象所有的Property  
                            foreach (PropertyInfo Property in typeof(T).GetProperties())
                            {
                                try
                                {
                                    //取得当前数据库字段的顺序  
                                    int Ordinal = reader.GetOrdinal(Property.Name);
                                    if (reader.GetValue(Ordinal) != DBNull.Value)
                                    {
                                        //将DataReader读取出来的数据填充到对象实体的属性里  
                                        //Property.SetValue(RowInstance, ChangeType(reader.GetValue(Ordinal), Property.PropertyType), default);
                                        Property.SetValue(RowInstance, ChangeType(reader.GetValue(Ordinal), Property.PropertyType), default);
                                    }
                                }
                                catch
                                {
                                    break;
                                }
                            }
                            dataList.Add(RowInstance);
                        }
                    }
                }
                catch { }
                finally
                {
                    reader.Close();
                }
            }

            return dataList;
        }

        /// <summary>
        /// 获取分页数据（单表分页）
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columns">要取的列名（逗号分开）</param>
        /// <param name="order">排序</param>
        /// <param name="pageSize">每页大小</param>
        /// <param name="pageIndex">当前页(默认从1开始)</param>
        /// <param name="where">查询条件</param>
        /// <param name="totalCount">总记录数</param>
        public static List<T> GetPager<T>(string tableName, string columns, string order, int pageSize, int pageIndex, string where, out int totalCount)
        {
            if (string.IsNullOrEmpty(columns)) columns = "*";
            if (string.IsNullOrEmpty(where)) where = "1=1";
            if (string.IsNullOrEmpty(order)) order = "Id";
            if (pageIndex == 0) pageIndex = 1;
            if (pageSize == 0) pageSize = 20;

            //实例化一个List<>泛型集合  
            var dataList = new List<T>();

            SqlParameter[] paras = {
                                       new SqlParameter("@tablename",SqlDbType.VarChar,100),
                                       new SqlParameter("@columns",SqlDbType.VarChar,1000),
                                       new SqlParameter("@order",SqlDbType.VarChar,100),
                                       new SqlParameter("@pageSize",SqlDbType.Int),
                                       new SqlParameter("@pageIndex",SqlDbType.Int),
                                       new SqlParameter("@where",SqlDbType.VarChar,2000),
                                       new SqlParameter("@totalCount",SqlDbType.Int)
                                   };
            paras[0].Value = tableName;
            paras[1].Value = columns;
            paras[2].Value = order;
            paras[3].Value = pageSize;
            paras[4].Value = pageIndex;
            paras[5].Value = where;
            paras[6].Direction = ParameterDirection.Output;   //输出参数

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();//打开数据库链接

                var command = new SqlCommand("sp_Pager", connection);//创建sqlCommand对象
                command.CommandType = CommandType.StoredProcedure;//声明以存储过程的方式执行。
                command.Parameters.AddRange(paras);//将cmd中的参数和将要执行的存储过程中的参数相对应
                var reader = command.ExecuteReader(CommandBehavior.CloseConnection);//指明了CommandReader然后调用CommandBehavior.CloseConnetion方法来关闭链接

                var type = typeof(T);
                var typeName = type.Name;

                try
                {
                    while (reader.Read())
                    {
                        //动态对象 dynamic,object
                        if (type == typeof(object))
                        {
                            var RowInstance = (IDictionary<string, object>)new ExpandoObject();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                string fieldName = reader.GetName(i);
                                var value = reader[fieldName];
                                RowInstance.Add(fieldName, value == DBNull.Value ? null : value);
                            }
                            dataList.Add((T)RowInstance);
                        }
                        //匿名类 anonymous
                        else if (typeName.Contains("<>") && typeName.Contains("__") && typeName.Contains("AnonymousType")) { }
                        //普通类 class
                        else
                        {
                            T RowInstance = Activator.CreateInstance<T>();//动态创建数据实体对象  
                                                                          //通过反射取得对象所有的Property  
                            foreach (PropertyInfo Property in typeof(T).GetProperties())
                            {
                                try
                                {
                                    //取得当前数据库字段的顺序  
                                    int Ordinal = reader.GetOrdinal(Property.Name);
                                    if (reader.GetValue(Ordinal) != DBNull.Value)
                                    {
                                        //将DataReader读取出来的数据填充到对象实体的属性里  
                                        Property.SetValue(RowInstance, ChangeType(reader.GetValue(Ordinal), Property.PropertyType), default);
                                    }
                                }
                                catch
                                {
                                    break;
                                }
                            }
                            dataList.Add(RowInstance);
                        }
                    }
                    reader.NextResult();
                }
                catch { }

                totalCount = Convert.ToInt32(paras[6].Value);//获取存储过程输出参数的值 即当前记录总数
            }
            return dataList;
        }

        /// <summary>
        /// 查询匿名对象集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static IList GetAnonymousList(Type anonymousType, string sql, params SqlParameter[] parms)
        {
            Type typeMaster = typeof(List<>);
            Type listType = typeMaster.MakeGenericType(anonymousType);
            var list = Activator.CreateInstance(listType) as IList;

            var constructor = anonymousType.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                           .OrderBy(c => c.GetParameters().Length).First();
            var parameters = constructor.GetParameters();
            var values = new object[parameters.Length];

            using (IDataReader reader = ExecuteDataReader(sql, parms))
            {
                try
                {
                    while (reader.Read())
                    {
                        for (int i = 0; i < parameters.Length; i++)
                        {
                            var parameter = parameters[i];
                            object itemValue = default;
                            int Ordinal = reader.GetOrdinal(parameter.Name);
                            var fieldValue = reader.GetValue(Ordinal);
                            if (fieldValue != DBNull.Value)
                            {
                                if (!parameter.ParameterType.IsGenericType)
                                {
                                    itemValue = ChangeType(fieldValue, parameter.ParameterType);
                                }
                                else
                                {
                                    Type genericTypeDefinition = parameter.ParameterType.GetGenericTypeDefinition();
                                    if (genericTypeDefinition == typeof(Nullable<>))
                                    {
                                        itemValue = ChangeType(fieldValue, Nullable.GetUnderlyingType(parameter.ParameterType));
                                    }
                                }
                            }
                            values[i] = itemValue;
                        }
                        list.Add(constructor.Invoke(values));
                    }
                }
                catch { }
                finally
                {
                    reader.Close();
                }
            }

            return list;
        }

        /// <summary>
        /// 获取分页数据（单表分页）
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="columns">要取的列名（逗号分开）</param>
        /// <param name="order">排序</param>
        /// <param name="pageSize">每页大小</param>
        /// <param name="pageIndex">当前页(默认从1开始)</param>
        /// <param name="where">查询条件</param>
        /// <param name="totalCount">总记录数</param>
        public static IList GetAnonymousPager(Type anonymousType, string tableName, string columns, string order, int pageSize, int pageIndex, string where, out int totalCount)
        {
            if (string.IsNullOrEmpty(columns)) columns = "*";
            if (string.IsNullOrEmpty(where)) where = "1=1";
            if (string.IsNullOrEmpty(order)) order = "Id";
            if (pageIndex == 0) pageIndex = 1;
            if (pageSize == 0) pageSize = 20;

            Type typeMaster = typeof(List<>);
            Type listType = typeMaster.MakeGenericType(anonymousType);
            var list = Activator.CreateInstance(listType) as IList;

            var constructor = anonymousType.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                           .OrderBy(c => c.GetParameters().Length).First();
            var parameters = constructor.GetParameters();
            var values = new object[parameters.Length];

            SqlParameter[] paras = {
                                       new SqlParameter("@tablename",SqlDbType.VarChar,100),
                                       new SqlParameter("@columns",SqlDbType.VarChar,1000),
                                       new SqlParameter("@order",SqlDbType.VarChar,100),
                                       new SqlParameter("@pageSize",SqlDbType.Int),
                                       new SqlParameter("@pageIndex",SqlDbType.Int),
                                       new SqlParameter("@where",SqlDbType.VarChar,2000),
                                       new SqlParameter("@totalCount",SqlDbType.Int)
                                   };
            paras[0].Value = tableName;
            paras[1].Value = columns;
            paras[2].Value = order;
            paras[3].Value = pageSize;
            paras[4].Value = pageIndex;
            paras[5].Value = where;
            paras[6].Direction = ParameterDirection.Output;   //输出参数

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();//打开数据库链接

                var command = new SqlCommand("sp_Pager", connection);//创建sqlCommand对象
                command.CommandType = CommandType.StoredProcedure;//声明以存储过程的方式执行。
                command.Parameters.AddRange(paras);//将cmd中的参数和将要执行的存储过程中的参数相对应
                var reader = command.ExecuteReader(CommandBehavior.CloseConnection);//指明了CommandReader然后调用CommandBehavior.CloseConnetion方法来关闭链接

                try
                {
                    while (reader.Read())
                    {
                        for (int i = 0; i < parameters.Length; i++)
                        {
                            var parameter = parameters[i];
                            object itemValue = default;
                            int Ordinal = reader.GetOrdinal(parameter.Name);
                            var fieldValue = reader.GetValue(Ordinal);
                            if (fieldValue != DBNull.Value)
                            {
                                if (!parameter.ParameterType.IsGenericType)
                                {
                                    itemValue = ChangeType(fieldValue, parameter.ParameterType);
                                }
                                else
                                {
                                    Type genericTypeDefinition = parameter.ParameterType.GetGenericTypeDefinition();
                                    if (genericTypeDefinition == typeof(Nullable<>))
                                    {
                                        itemValue = ChangeType(fieldValue, Nullable.GetUnderlyingType(parameter.ParameterType));
                                    }
                                }
                            }
                            values[i] = itemValue;
                        }
                        list.Add(constructor.Invoke(values));
                    }
                    reader.NextResult();
                }
                catch { }

                totalCount = Convert.ToInt32(paras[6].Value);//获取存储过程输出参数的值 即当前记录总数
            }

            return list;
        }
        #endregion

        #region 批量操作(非事务)
        /// <summary>
        /// 执行SqlBulkCopy批量插入(支持对象集合)
        /// </summary>
        /// <param name="tables"></param>
        /// <param name="addList"></param>
        /// <returns></returns>
        public static bool BulkCopyAdd(string tables, params List<object>[] addList)
        {
            if (string.IsNullOrEmpty(tables) || addList.Length == 0) return default;
            var tableNameArr = tables.Split(',');
            if (tableNameArr.Length != addList.Length) return default;
            var tableList = new List<DataTable>();
            for (int i = 0; i < tableNameArr.Length; i++)
            {
                var tableName = tableNameArr[i];
                var table = ObjectToTable(addList[i], tableName);
                table.TableName = tableName;
                tableList.Add(table);
            }
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var sqlbulkcopy = new SqlBulkCopy(conn) { BulkCopyTimeout = CommandTimeOut };

                try
                {
                    foreach (var item in tableList)
                    {
                        sqlbulkcopy.ColumnMappings.Clear();
                        sqlbulkcopy.DestinationTableName = item.TableName;
                        for (int i = 0; i < item.Columns.Count; i++)
                        {
                            sqlbulkcopy.ColumnMappings.Add(item.Columns[i].ColumnName, item.Columns[i].ColumnName);
                        }
                        sqlbulkcopy.WriteToServerAsync(item);
                    }
                }
                catch
                {
                    return default;
                }
                finally
                {
                    sqlbulkcopy.Close();
                    conn.Close();
                }
            }
            return true;
        }

        /// <summary>
        /// 执行SqlBulkCopy批量插入(支持对象集合)
        /// </summary>
        /// <param name="addDic"></param>
        /// <returns></returns>
        public static bool BulkCopyAdd(Dictionary<string, List<object>> addDic)
        {
            var addTbList = new List<DataTable>();
            foreach (var item in addDic)
            {
                var tableName = item.Key;
                var table = ObjectToTable(item.Value, tableName);
                table.TableName = tableName;
                addTbList.Add(table);
            }
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var sqlbulkcopy = new SqlBulkCopy(conn) { BulkCopyTimeout = CommandTimeOut };

                try
                {
                    addTbList.ForEach(o =>
                    {
                        sqlbulkcopy.ColumnMappings.Clear();
                        sqlbulkcopy.DestinationTableName = o.TableName;
                        foreach (DataColumn item in o.Columns) sqlbulkcopy.ColumnMappings.Add(item.ColumnName, item.ColumnName);
                        sqlbulkcopy.WriteToServer(o);
                    });
                }
                catch
                {
                    return default;
                }
                finally
                {
                    sqlbulkcopy.Close();
                    conn.Close();
                }
            }
            return true;
        }

        /// <summary>
        /// 执行SqlBulkCopy批量更新(支持对象集合)
        /// </summary>
        /// <param name="tables"></param>
        /// <param name="setList"></param>
        /// <returns></returns>
        public static bool BulkCopySet(string tables, params List<object>[] setList)
        {
            if (string.IsNullOrEmpty(tables) || setList.Length == 0) return default;
            var tableNameArr = tables.Split(',');
            if (tableNameArr.Length != setList.Length) return default;
            var tableList = new List<DataTable>();
            for (int i = 0; i < tableNameArr.Length; i++)
            {
                var tableName = tableNameArr[i];
                var table = ObjectToTable(setList[i], tableName);
                table.PrimaryKey = new DataColumn[] { table.Columns[GetIdName(tableName)] };
                table.TableName = tableName;
                tableList.Add(table);
            }

            var tempTableSuf = DateTime.Now.ToString("yyyyMMddHHmmss");
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var sqlbulkcopy = new SqlBulkCopy(conn) { BulkCopyTimeout = CommandTimeOut };

                try
                {
                    using (SqlCommand cmd = new SqlCommand(string.Empty, conn) { CommandTimeout = CommandTimeOut })
                    {
                        tableList.ForEach(o =>
                        {
                            var primaryKeyName = o.PrimaryKey.First().ColumnName;
                            var addOrSetFields = new List<string>();
                            foreach (DataColumn column in o.Columns) addOrSetFields.Add(column.ColumnName);
                            cmd.CommandText = string.Format(@"SELECT {0} into {1} from {2} A WHERE 1=2;", string.Join(',', addOrSetFields.Select(p => "A." + p)), tempTablePre + o.TableName + tempTableSuf, o.TableName);
                            cmd.ExecuteNonQuery();

                            sqlbulkcopy.ColumnMappings.Clear();
                            sqlbulkcopy.DestinationTableName = tempTablePre + o.TableName + tempTableSuf;
                            foreach (var item in addOrSetFields) sqlbulkcopy.ColumnMappings.Add(item, item);
                            sqlbulkcopy.WriteToServer(o);

                            StringBuilder updateSql = new StringBuilder(), onSql = new StringBuilder();
                            addOrSetFields.Remove(primaryKeyName);
                            foreach (var column in addOrSetFields) updateSql.Append(string.Format(@"A.{0} = B.{0},", column));
                            onSql.Append(string.Format(@"A.{0} = B.{0}", primaryKeyName));
                            cmd.CommandText = string.Format(@"UPDATE A SET {0} FROM {1} A INNER JOIN {2} B ON {3};drop table {2};",
                                updateSql.ToString().Trim(','), o.TableName, tempTablePre + o.TableName + tempTableSuf, onSql.ToString());
                            cmd.ExecuteNonQuery();
                        });
                    }
                }
                catch
                {
                    return default;
                }
                finally
                {
                    sqlbulkcopy.Close();
                    conn.Close();
                }
            }
            return true;
        }

        /// <summary>
        /// 执行SqlBulkCopy批量更新(支持对象集合)
        /// </summary>
        /// <param name="setDic"></param>
        /// <returns></returns>
        public static bool BulkCopySet(Dictionary<string, List<object>> setDic)
        {
            var setTbList = new List<DataTable>();
            foreach (var item in setDic)
            {
                var tableName = item.Key;
                var table = ObjectToTable(item.Value, tableName);
                table.PrimaryKey = new DataColumn[] { table.Columns[GetIdName(tableName)] };
                table.TableName = tableName;
                setTbList.Add(table);
            }

            var tempTableSuf = DateTime.Now.ToString("yyyyMMddHHmmss");
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var sqlbulkcopy = new SqlBulkCopy(conn) { BulkCopyTimeout = CommandTimeOut };

                try
                {
                    using (SqlCommand cmd = new SqlCommand(string.Empty, conn) { CommandTimeout = CommandTimeOut })
                    {
                        setTbList.ForEach(o =>
                        {
                            var primaryKeyName = o.PrimaryKey.First().ColumnName;
                            var addOrSetFields = new List<string>();
                            foreach (DataColumn column in o.Columns) addOrSetFields.Add(column.ColumnName);
                            cmd.CommandText = string.Format(@"SELECT {0} into {1} from {2} A WHERE 1=2;", string.Join(',', addOrSetFields.Select(p => "A." + p)), tempTablePre + o.TableName + tempTableSuf, o.TableName);
                            cmd.ExecuteNonQuery();

                            sqlbulkcopy.ColumnMappings.Clear();
                            sqlbulkcopy.DestinationTableName = tempTablePre + o.TableName + tempTableSuf;
                            foreach (var item in addOrSetFields) sqlbulkcopy.ColumnMappings.Add(item, item);
                            sqlbulkcopy.WriteToServer(o);

                            StringBuilder updateSql = new StringBuilder(), onSql = new StringBuilder();
                            addOrSetFields.Remove(primaryKeyName);
                            foreach (var column in addOrSetFields) updateSql.Append(string.Format(@"A.{0} = B.{0},", column));
                            onSql.Append(string.Format(@"A.{0} = B.{0}", primaryKeyName));
                            cmd.CommandText = string.Format(@"UPDATE A SET {0} FROM {1} A INNER JOIN {2} B ON {3};drop table {2};",
                                updateSql.ToString().Trim(','), o.TableName, tempTablePre + o.TableName + tempTableSuf, onSql.ToString());
                            cmd.ExecuteNonQuery();
                        });
                    }
                }
                catch
                {
                    return default;
                }
                finally
                {
                    sqlbulkcopy.Close();
                    conn.Close();
                }
            }
            return true;
        }

        /// <summary>
        /// 执行SqlBulkCopy批量新增+更新(支持对象集合)。
        /// </summary>
        /// <param name="addDic"></param>
        /// <param name="setDic"></param>
        /// <returns></returns>
        public static bool BulkCopyAddAndSet(Dictionary<string, List<object>> addDic = default, Dictionary<string, List<object>> setDic = default)
        {
            if (addDic is null && setDic is null) return false;
            addDic = addDic ?? new Dictionary<string, List<object>>();
            setDic = setDic ?? new Dictionary<string, List<object>>();
            var addTbList = new List<DataTable>();
            foreach (var item in addDic)
            {
                var tableName = item.Key;
                var table = ObjectToTable(item.Value, tableName);
                table.TableName = tableName;
                addTbList.Add(table);
            }

            var setTbList = new List<DataTable>();
            foreach (var item in setDic)
            {
                var tableName = item.Key;
                var table = ObjectToTable(item.Value, tableName);
                table.PrimaryKey = new DataColumn[] { table.Columns[GetIdName(tableName)] };
                table.TableName = tableName;
                setTbList.Add(table);
            }

            var tempTableSuf = DateTime.Now.ToString("yyyyMMddHHmmss");
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var sqlbulkcopy = new SqlBulkCopy(conn) { BulkCopyTimeout = CommandTimeOut };

                try
                {
                    using (SqlCommand cmd = new SqlCommand(string.Empty, conn) { CommandTimeout = CommandTimeOut })
                    {
                        addTbList.ForEach(o =>
                        {
                            sqlbulkcopy.ColumnMappings.Clear();
                            sqlbulkcopy.DestinationTableName = o.TableName;
                            foreach (DataColumn item in o.Columns) sqlbulkcopy.ColumnMappings.Add(item.ColumnName, item.ColumnName);
                            sqlbulkcopy.WriteToServer(o);
                        });

                        setTbList.ForEach(o =>
                        {
                            var primaryKeyName = o.PrimaryKey.First().ColumnName;
                            var addOrSetFields = new List<string>();
                            foreach (DataColumn column in o.Columns) addOrSetFields.Add(column.ColumnName);
                            cmd.CommandText = string.Format(@"SELECT {0} into {1} from {2} A WHERE 1=2;", string.Join(',', addOrSetFields.Select(p => "A." + p)), tempTablePre + o.TableName + tempTableSuf, o.TableName);
                            cmd.ExecuteNonQuery();

                            sqlbulkcopy.ColumnMappings.Clear();
                            sqlbulkcopy.DestinationTableName = tempTablePre + o.TableName + tempTableSuf;
                            foreach (var item in addOrSetFields) sqlbulkcopy.ColumnMappings.Add(item, item);
                            sqlbulkcopy.WriteToServer(o);

                            StringBuilder updateSql = new StringBuilder(), onSql = new StringBuilder();
                            addOrSetFields.Remove(primaryKeyName);
                            foreach (var column in addOrSetFields) updateSql.Append(string.Format(@"A.{0} = B.{0},", column));
                            onSql.Append(string.Format(@"A.{0} = B.{0}", primaryKeyName));
                            cmd.CommandText = string.Format(@"UPDATE A SET {0} FROM {1} A INNER JOIN {2} B ON {3};drop table {2};",
                                updateSql.ToString().Trim(','), o.TableName, tempTablePre + o.TableName + tempTableSuf, onSql.ToString());
                            cmd.ExecuteNonQuery();
                        });
                    }
                }
                catch
                {
                    return default;
                }
                finally
                {
                    sqlbulkcopy.Close();
                    conn.Close();
                }
            }
            return true;
        }
        #endregion

        #region 批量操作(事务)
        /// <summary>
        /// 执行SqlBulkCopy批量插入(支持多对象集合)(事务)
        /// </summary>
        /// <param name="tables"></param>
        /// <param name="addList"></param>
        /// <returns></returns>
        public static bool BulkCopyAddTran(string tables, params List<object>[] addList)
        {
            if (string.IsNullOrEmpty(tables) || addList.Length == 0) return default;
            var tableNameArr = tables.Split(',');
            if (tableNameArr.Length != addList.Length) return default;
            var tableList = new List<DataTable>();
            for (int i = 0; i < tableNameArr.Length; i++)
            {
                var tableName = tableNameArr[i];
                var table = ObjectToTable(addList[i], tableName);
                table.TableName = tableName;
                tableList.Add(table);
            }
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var tran = conn.BeginTransaction();//开启事务
                var sqlbulkcopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.CheckConstraints, tran) { BulkCopyTimeout = CommandTimeOut };

                try
                {
                    foreach (var item in tableList)
                    {
                        sqlbulkcopy.ColumnMappings.Clear();
                        sqlbulkcopy.DestinationTableName = item.TableName;
                        for (int i = 0; i < item.Columns.Count; i++)
                        {
                            sqlbulkcopy.ColumnMappings.Add(item.Columns[i].ColumnName, item.Columns[i].ColumnName);
                        }
                        sqlbulkcopy.WriteToServerAsync(item);
                    }
                    tran.Commit();
                }
                catch
                {
                    return default;
                }
                finally
                {
                    sqlbulkcopy.Close();
                    tran.Dispose();
                    conn.Close();
                }
            }
            return true;
        }

        /// <summary>
        /// 执行SqlBulkCopy批量插入(支持多对象集合)(事务)
        /// </summary>
        /// <param name="addDic"></param>
        /// <returns></returns>
        public static bool BulkCopyAddTran(Dictionary<string, List<object>> addDic)
        {
            var addTbList = new List<DataTable>();
            foreach (var item in addDic)
            {
                var tableName = item.Key;
                var table = ObjectToTable(item.Value, tableName);
                table.TableName = tableName;
                addTbList.Add(table);
            }
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var tran = conn.BeginTransaction();//开启事务
                var sqlbulkcopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.CheckConstraints, tran) { BulkCopyTimeout = CommandTimeOut };

                try
                {
                    addTbList.ForEach(o =>
                    {
                        sqlbulkcopy.ColumnMappings.Clear();
                        sqlbulkcopy.DestinationTableName = o.TableName;
                        foreach (DataColumn item in o.Columns) sqlbulkcopy.ColumnMappings.Add(item.ColumnName, item.ColumnName);
                        sqlbulkcopy.WriteToServer(o);
                    });
                    tran.Commit();
                }
                catch
                {
                    return default;
                }
                finally
                {
                    sqlbulkcopy.Close();
                    tran.Dispose();
                    conn.Close();
                }
            }
            return true;
        }

        /// <summary>
        /// 执行SqlBulkCopy批量更新(支持对象集合)(事务)
        /// </summary>
        /// <param name="tables"></param>
        /// <param name="setList"></param>
        /// <returns></returns>
        public static bool BulkCopySetTran(string tables, params List<object>[] setList)
        {
            if (string.IsNullOrEmpty(tables) || setList.Length == 0) return default;
            var tableNameArr = tables.Split(',');
            if (tableNameArr.Length != setList.Length) return default;
            var tableList = new List<DataTable>();
            for (int i = 0; i < tableNameArr.Length; i++)
            {
                var tableName = tableNameArr[i];
                var table = ObjectToTable(setList[i], tableName);
                table.PrimaryKey = new DataColumn[] { table.Columns[GetIdName(tableName)] };
                table.TableName = tableName;
                tableList.Add(table);
            }

            var tempTableSuf = DateTime.Now.ToString("yyyyMMddHHmmss");
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var tran = conn.BeginTransaction();//开启事务
                var sqlbulkcopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.CheckConstraints, tran) { BulkCopyTimeout = CommandTimeOut };

                try
                {
                    using (SqlCommand cmd = new SqlCommand(string.Empty, conn, tran) { CommandTimeout = CommandTimeOut })
                    {
                        tableList.ForEach(o =>
                        {
                            var primaryKeyName = o.PrimaryKey.First().ColumnName;
                            var addOrSetFields = new List<string>();
                            foreach (DataColumn column in o.Columns) addOrSetFields.Add(column.ColumnName);
                            cmd.CommandText = string.Format(@"SELECT {0} into {1} from {2} A WHERE 1=2;", string.Join(',', addOrSetFields.Select(p => "A." + p)), tempTablePre + o.TableName + tempTableSuf, o.TableName);
                            cmd.ExecuteNonQuery();

                            sqlbulkcopy.ColumnMappings.Clear();
                            sqlbulkcopy.DestinationTableName = tempTablePre + o.TableName + tempTableSuf;
                            foreach (var item in addOrSetFields) sqlbulkcopy.ColumnMappings.Add(item, item);
                            sqlbulkcopy.WriteToServer(o);

                            StringBuilder updateSql = new StringBuilder(), onSql = new StringBuilder();
                            addOrSetFields.Remove(primaryKeyName);
                            foreach (var column in addOrSetFields) updateSql.Append(string.Format(@"A.{0} = B.{0},", column));
                            onSql.Append(string.Format(@"A.{0} = B.{0}", primaryKeyName));
                            cmd.CommandText = string.Format(@"UPDATE A SET {0} FROM {1} A INNER JOIN {2} B ON {3};drop table {2};",
                                updateSql.ToString().Trim(','), o.TableName, tempTablePre + o.TableName + tempTableSuf, onSql.ToString());
                            cmd.ExecuteNonQuery();
                        });

                        tran.Commit();
                    }
                }
                catch
                {
                    return default;
                }
                finally
                {
                    sqlbulkcopy.Close();
                    tran.Dispose();
                    conn.Close();
                }
            }
            return true;
        }

        /// <summary>
        /// 执行SqlBulkCopy批量更新(支持对象集合)(事务)
        /// </summary>
        /// <param name="setDic"></param>
        /// <returns></returns>
        public static bool BulkCopySetTran(Dictionary<string, List<object>> setDic)
        {
            var setTbList = new List<DataTable>();
            foreach (var item in setDic)
            {
                var tableName = item.Key;
                var table = ObjectToTable(item.Value, tableName);
                table.PrimaryKey = new DataColumn[] { table.Columns[GetIdName(tableName)] };
                table.TableName = tableName;
                setTbList.Add(table);
            }

            var tempTableSuf = DateTime.Now.ToString("yyyyMMddHHmmss");
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var tran = conn.BeginTransaction();//开启事务
                var sqlbulkcopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.CheckConstraints, tran) { BulkCopyTimeout = CommandTimeOut };

                try
                {
                    using (SqlCommand cmd = new SqlCommand(string.Empty, conn, tran) { CommandTimeout = CommandTimeOut })
                    {
                        setTbList.ForEach(o =>
                        {
                            var primaryKeyName = o.PrimaryKey.First().ColumnName;
                            var addOrSetFields = new List<string>();
                            foreach (DataColumn column in o.Columns) addOrSetFields.Add(column.ColumnName);
                            cmd.CommandText = string.Format(@"SELECT {0} into {1} from {2} A WHERE 1=2;", string.Join(',', addOrSetFields.Select(p => "A." + p)), tempTablePre + o.TableName + tempTableSuf, o.TableName);
                            cmd.ExecuteNonQuery();

                            sqlbulkcopy.ColumnMappings.Clear();
                            sqlbulkcopy.DestinationTableName = tempTablePre + o.TableName + tempTableSuf;
                            foreach (var item in addOrSetFields) sqlbulkcopy.ColumnMappings.Add(item, item);
                            sqlbulkcopy.WriteToServer(o);

                            StringBuilder updateSql = new StringBuilder(), onSql = new StringBuilder();
                            addOrSetFields.Remove(primaryKeyName);
                            foreach (var column in addOrSetFields) updateSql.Append(string.Format(@"A.{0} = B.{0},", column));
                            onSql.Append(string.Format(@"A.{0} = B.{0}", primaryKeyName));
                            cmd.CommandText = string.Format(@"UPDATE A SET {0} FROM {1} A INNER JOIN {2} B ON {3};drop table {2};",
                                updateSql.ToString().Trim(','), o.TableName, tempTablePre + o.TableName + tempTableSuf, onSql.ToString());
                            cmd.ExecuteNonQuery();
                        });

                        tran.Commit();
                    }
                }
                catch
                {
                    return default;
                }
                finally
                {
                    sqlbulkcopy.Close();
                    tran.Dispose();
                    conn.Close();
                }
            }
            return true;
        }

        /// <summary>
        /// 执行SqlBulkCopy批量新增+更新(支持对象集合)(事务)
        /// </summary>
        /// <param name="addDic"></param>
        /// <param name="setDic"></param>
        /// <returns></returns>
        public static bool BulkCopyAddAndSetTran(Dictionary<string, List<object>> addDic = default, Dictionary<string, List<object>> setDic = default)
        {
            if (addDic is null && setDic is null) return false;
            addDic = addDic ?? new Dictionary<string, List<object>>();
            setDic = setDic ?? new Dictionary<string, List<object>>();
            var addTbList = new List<DataTable>();
            foreach (var item in addDic)
            {
                var tableName = item.Key;
                var table = ObjectToTable(item.Value, tableName);
                table.TableName = tableName;
                addTbList.Add(table);
            }

            var setTbList = new List<DataTable>();
            foreach (var item in setDic)
            {
                var tableName = item.Key;
                var table = ObjectToTable(item.Value, tableName);
                table.PrimaryKey = new DataColumn[] { table.Columns[GetIdName(tableName)] };
                table.TableName = tableName;
                setTbList.Add(table);
            }

            var tempTableSuf = DateTime.Now.ToString("yyyyMMddHHmmss");
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var tran = conn.BeginTransaction();//开启事务
                var sqlbulkcopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.CheckConstraints, tran) { BulkCopyTimeout = CommandTimeOut };

                try
                {
                    using (SqlCommand cmd = new SqlCommand(string.Empty, conn, tran) { CommandTimeout = CommandTimeOut })
                    {
                        addTbList.ForEach(o =>
                        {
                            sqlbulkcopy.ColumnMappings.Clear();
                            sqlbulkcopy.DestinationTableName = o.TableName;
                            foreach (DataColumn item in o.Columns) sqlbulkcopy.ColumnMappings.Add(item.ColumnName, item.ColumnName);
                            sqlbulkcopy.WriteToServer(o);
                        });

                        setTbList.ForEach(o =>
                        {
                            var primaryKeyName = o.PrimaryKey.First().ColumnName;
                            var addOrSetFields = new List<string>();
                            foreach (DataColumn column in o.Columns) addOrSetFields.Add(column.ColumnName);
                            cmd.CommandText = string.Format(@"SELECT {0} into {1} from {2} A WHERE 1=2;", string.Join(',', addOrSetFields.Select(p => "A." + p)), tempTablePre + o.TableName + tempTableSuf, o.TableName);
                            cmd.ExecuteNonQuery();

                            sqlbulkcopy.ColumnMappings.Clear();
                            sqlbulkcopy.DestinationTableName = tempTablePre + o.TableName + tempTableSuf;
                            foreach (var item in addOrSetFields) sqlbulkcopy.ColumnMappings.Add(item, item);
                            sqlbulkcopy.WriteToServer(o);

                            StringBuilder updateSql = new StringBuilder(), onSql = new StringBuilder();
                            addOrSetFields.Remove(primaryKeyName);
                            foreach (var column in addOrSetFields) updateSql.Append(string.Format(@"A.{0} = B.{0},", column));
                            onSql.Append(string.Format(@"A.{0} = B.{0}", primaryKeyName));
                            cmd.CommandText = string.Format(@"UPDATE A SET {0} FROM {1} A INNER JOIN {2} B ON {3};drop table {2};",
                                updateSql.ToString().Trim(','), o.TableName, tempTablePre + o.TableName + tempTableSuf, onSql.ToString());
                            cmd.ExecuteNonQuery();
                        });

                        tran.Commit();
                    }
                }
                catch
                {
                    return default;
                }
                finally
                {
                    sqlbulkcopy.Close();
                    tran.Dispose();
                    conn.Close();
                }
            }
            return true;
        }
        #endregion

        #region ExecuteNonQuery
        /// <summary>
        /// 执行SQL语句,返回影响的行数
        /// </summary>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回影响的行数</returns>
        public static int ExecuteNonQuery(string commandText, params SqlParameter[] parms)
        {
            return ExecuteNonQuery(connectionString, CommandType.Text, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回影响的行数
        /// </summary>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回影响的行数</returns>
        public static int ExecuteNonQuery(CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteNonQuery(connectionString, commandType, commandText, parms);
        }
        #endregion

        #region GetField

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行第一列
        /// </summary>
        /// <typeparam name="T">返回对象类型</typeparam>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一行第一列</returns>
        public static T GetField<T>(string commandText, params SqlParameter[] parms)
        {
            return GetField<T>(connectionString, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行第一列
        /// </summary>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一行第一列</returns>
        public static object ExecuteScalar(string commandText, params SqlParameter[] parms)
        {
            return GetField(connectionString, CommandType.Text, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行第一列
        /// </summary>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一行第一列</returns>
        public static object ExecuteScalar(CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return GetField(connectionString, commandType, commandText, parms);
        }

        #endregion GetField

        #region ExecuteDataReader

        /// <summary>
        /// 执行SQL语句,返回只读数据集
        /// </summary>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回只读数据集</returns>
        public static SqlDataReader ExecuteDataReader(string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataReader(connectionString, CommandType.Text, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回只读数据集
        /// </summary>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回只读数据集</returns>
        public static SqlDataReader ExecuteDataReader(CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataReader(connectionString, commandType, commandText, parms);
        }
        #endregion

        #region ExecuteDataRow

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行
        /// </summary>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一行</returns>
        public static DataRow ExecuteDataRow(string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataRow(connectionString, CommandType.Text, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行
        /// </summary>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一行</returns>
        public static DataRow ExecuteDataRow(CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataRow(connectionString, commandType, commandText, parms);
        }

        #endregion ExecuteDataRow

        #region ExecuteDataTable

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteDataTable(string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataTable(connectionString, CommandType.Text, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteDataTable(CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataSet(connectionString, commandType, commandText, parms).Tables[0];
        }

        /// <summary>
        ///  执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="order">排序SQL,如"ORDER BY ID DESC"</param>
        /// <param name="pageSize">每页记录数</param>
        /// <param name="pageIndex">页索引</param>
        /// <param name="parms">查询参数</param>
        /// <param name="query">查询SQL</param>        
        /// <returns></returns>
        public static DataTable ExecutePageDataTable(string sql, string order, int pageSize, int pageIndex, SqlParameter[] parms = null, string query = null, string cte = null)
        {
            return ExecutePageDataTable(sql, order, pageSize, pageIndex, parms, query, cte);
        }
        #endregion ExecuteDataTable

        #region ExecuteDataSet

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        public static DataSet ExecuteDataSet(string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataSet(connectionString, CommandType.Text, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        public static DataSet ExecuteDataSet(CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataSet(connectionString, commandType, commandText, parms);
        }

        #endregion ExecuteDataSet

        #region 批量操作

        /// <summary>
        /// 大批量数据插入
        /// </summary>
        /// <param name="table">数据表</param>
        public static void BulkInsert(DataTable table)
        {
            BulkInsert(connectionString, table);
        }

        /// <summary>
        /// 使用MySqlDataAdapter批量更新数据
        /// </summary>
        /// <param name="table">数据表</param>
        public static void BatchUpdate(DataTable table)
        {
            BatchUpdate(connectionString, table);
        }

        /// <summary>
        /// 分批次批量删除数据
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="batchSize">每批次删除记录行数</param>
        /// <param name="interval">批次执行间隔(秒)</param>
        public static void BatchDelete(string sql, int batchSize = 1000, int interval = 1)
        {
            BatchDelete(connectionString, sql, batchSize, interval);
        }

        /// <summary>
        /// 分批次批量更新数据
        /// </summary>
        /// <param name="sql">SQL语句</param>
        /// <param name="batchSize">每批次更新记录行数</param>
        /// <param name="interval">批次执行间隔(秒)</param>
        public static void BatchUpdate(string sql, int batchSize = 1000, int interval = 1)
        {
            BatchUpdate(connectionString, sql, batchSize, interval);
        }

        #endregion 批量操作

        private static void PrepareCommand(SqlCommand command, SqlConnection connection, SqlTransaction transaction, CommandType commandType, string commandText, SqlParameter[] parms)
        {
            if (connection.State != ConnectionState.Open) connection.Open();

            command.Connection = connection;
            command.CommandTimeout = CommandTimeOut;
            // 设置命令文本(存储过程名或SQL语句)
            command.CommandText = commandText;
            // 分配事务
            if (transaction != null)
            {
                command.Transaction = transaction;
            }
            // 设置命令类型.
            command.CommandType = commandType;
            if (parms != null && parms.Length > 0)
            {
                //预处理SqlParameter参数数组，将为NULL的参数赋值为DBNull.Value;
                foreach (SqlParameter parameter in parms)
                {
                    if ((parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.Input) && (parameter.Value == null))
                    {
                        parameter.Value = DBNull.Value;
                    }
                }
                command.Parameters.AddRange(parms);
            }
        }

        #region ExecuteNonQuery

        /// <summary>
        /// 执行SQL语句,返回影响的行数
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回影响的行数</returns>
        public static int ExecuteNonQuery(string connectionString, string commandText, params SqlParameter[] parms)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                return ExecuteNonQuery(connection, CommandType.Text, commandText, parms);
            }
        }

        /// <summary>
        /// 执行SQL语句,返回影响的行数
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回影响的行数</returns>
        public static int ExecuteNonQuery(string connectionString, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                return ExecuteNonQuery(connection, commandType, commandText, parms);
            }
        }

        /// <summary>
        /// 执行SQL语句,返回影响的行数
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回影响的行数</returns>
        public static int ExecuteNonQuery(SqlConnection connection, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteNonQuery(connection, null, commandType, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回影响的行数
        /// </summary>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回影响的行数</returns>
        public static int ExecuteNonQuery(SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteNonQuery(transaction.Connection, transaction, commandType, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回影响的行数
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回影响的行数</returns>
        private static int ExecuteNonQuery(SqlConnection connection, SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            SqlCommand command = new SqlCommand();
            PrepareCommand(command, connection, transaction, commandType, commandText, parms);
            int retval = command.ExecuteNonQuery();
            command.Parameters.Clear();
            return retval;
        }

        #endregion ExecuteNonQuery

        #region GetField

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行第一列
        /// </summary>
        /// <typeparam name="T">返回对象类型</typeparam>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一行第一列</returns>
        public static T GetField<T>(string connectionString, string commandText, params SqlParameter[] parms)
        {
            object result = GetField(connectionString, commandText, parms);
            if (result != null)
            {
                return (T)ChangeType(result, typeof(T)); ;
            }
            return default(T);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行第一列
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一行第一列</returns>
        public static object GetField(string connectionString, string commandText, params SqlParameter[] parms)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                return GetField(connection, CommandType.Text, commandText, parms);
            }
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行第一列
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一行第一列</returns>
        public static object GetField(string connectionString, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                return GetField(connection, commandType, commandText, parms);
            }
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行第一列
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一行第一列</returns>
        public static object GetField(SqlConnection connection, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return GetField(connection, null, commandType, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行第一列
        /// </summary>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一行第一列</returns>
        public static object GetField(SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return GetField(transaction.Connection, transaction, commandType, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行第一列
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一行第一列</returns>
        private static object GetField(SqlConnection connection, SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            SqlCommand command = new SqlCommand();
            PrepareCommand(command, connection, transaction, commandType, commandText, parms);
            object retval = command.ExecuteScalar();
            command.Parameters.Clear();
            return retval;
        }

        #endregion GetField

        #region ExecuteDataReader

        /// <summary>
        /// 执行SQL语句,返回只读数据集
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回只读数据集</returns>
        private static SqlDataReader ExecuteDataReader(string connectionString, string commandText, params SqlParameter[] parms)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            return ExecuteDataReader(connection, null, CommandType.Text, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回只读数据集
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回只读数据集</returns>
        private static SqlDataReader ExecuteDataReader(string connectionString, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            SqlConnection connection = new SqlConnection(connectionString);
            return ExecuteDataReader(connection, null, commandType, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回只读数据集
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回只读数据集</returns>
        private static SqlDataReader ExecuteDataReader(SqlConnection connection, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataReader(connection, null, commandType, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回只读数据集
        /// </summary>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回只读数据集</returns>
        private static SqlDataReader ExecuteDataReader(SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataReader(transaction.Connection, transaction, commandType, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回只读数据集
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回只读数据集</returns>
        private static SqlDataReader ExecuteDataReader(SqlConnection connection, SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            SqlCommand command = new SqlCommand();
            PrepareCommand(command, connection, transaction, commandType, commandText, parms);
            return command.ExecuteReader(CommandBehavior.CloseConnection);
        }

        #endregion

        #region ExecuteDataRow

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>,返回结果集中的第一行</returns>
        public static DataRow ExecuteDataRow(string connectionString, string commandText, params SqlParameter[] parms)
        {
            DataTable dt = ExecuteDataTable(connectionString, CommandType.Text, commandText, parms);
            return dt.Rows.Count > 0 ? dt.Rows[0] : null;
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>,返回结果集中的第一行</returns>
        public static DataRow ExecuteDataRow(string connectionString, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            DataTable dt = ExecuteDataTable(connectionString, commandType, commandText, parms);
            return dt.Rows.Count > 0 ? dt.Rows[0] : null;
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>,返回结果集中的第一行</returns>
        public static DataRow ExecuteDataRow(SqlConnection connection, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            DataTable dt = ExecuteDataTable(connection, commandType, commandText, parms);
            return dt.Rows.Count > 0 ? dt.Rows[0] : null;
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一行
        /// </summary>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>,返回结果集中的第一行</returns>
        public static DataRow ExecuteDataRow(SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            DataTable dt = ExecuteDataTable(transaction, commandType, commandText, parms);
            return dt.Rows.Count > 0 ? dt.Rows[0] : null;
        }

        #endregion ExecuteDataRow

        #region ExecuteDataTable

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteDataTable(string connectionString, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataSet(connectionString, CommandType.Text, commandText, parms).Tables[0];
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteDataTable(string connectionString, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataSet(connectionString, commandType, commandText, parms).Tables[0];
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteDataTable(SqlConnection connection, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataSet(connection, commandType, commandText, parms).Tables[0];
        }

        /// <summary>
        /// 执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteDataTable(SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataSet(transaction, commandType, commandText, parms).Tables[0];
        }

        /// <summary>
        /// 获取空表结构
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="tableName">数据表名称</param>
        /// <returns>返回结果集中的第一个数据表</returns>
        public static DataTable ExecuteEmptyDataTable(string connectionString, string tableName)
        {
            return ExecuteDataSet(connectionString, CommandType.Text, string.Format("select * from {0} where 1=-1", tableName)).Tables[0];
        }

        /// <summary>
        ///  执行SQL语句,返回结果集中的第一个数据表
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="sql">SQL语句</param>
        /// <param name="order">排序SQL,如"ORDER BY ID DESC"</param>
        /// <param name="pageSize">每页记录数</param>
        /// <param name="pageIndex">页索引</param>
        /// <param name="parms">查询参数</param>      
        /// <param name="query">查询SQL</param>
        /// <param name="cte">CTE表达式</param>
        /// <returns></returns>
        public static DataTable ExecutePageDataTable(string connectionString, string sql, string order, int pageSize, int pageIndex, SqlParameter[] parms = null, string query = null, string cte = null)
        {
            string psql = string.Format(@"
                                        {3}
                                        SELECT  *
                                        FROM    (
                                                 SELECT ROW_NUMBER() OVER (ORDER BY {1}) RowNumber,*
                                                 FROM   (
                                                         {0}
                                                        ) t
                                                 WHERE  1 = 1 {2}
                                                ) t
                                        WHERE   RowNumber BETWEEN @RowNumber_Begin
                                                          AND     @RowNumber_End", sql, order, query, cte);

            List<SqlParameter> paramlist = new List<SqlParameter>()
            {
                new SqlParameter("@RowNumber_Begin", SqlDbType.Int){ Value = (pageIndex - 1) * pageSize + 1 },
                new SqlParameter("@RowNumber_End", SqlDbType.Int){ Value = pageIndex * pageSize }
            };
            if (parms != null) paramlist.AddRange(parms);
            return ExecuteDataTable(connectionString, psql, paramlist.ToArray());
        }

        #endregion ExecuteDataTable

        #region ExecuteDataSet

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandText">SQL语句</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        public static DataSet ExecuteDataSet(string connectionString, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataSet(connectionString, CommandType.Text, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        public static DataSet ExecuteDataSet(string connectionString, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                return ExecuteDataSet(connection, commandType, commandText, parms);
            }
        }

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        public static DataSet ExecuteDataSet(SqlConnection connection, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataSet(connection, null, commandType, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        public static DataSet ExecuteDataSet(SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            return ExecuteDataSet(transaction.Connection, transaction, commandType, commandText, parms);
        }

        /// <summary>
        /// 执行SQL语句,返回结果集
        /// </summary>
        /// <param name="connection">数据库连接</param>
        /// <param name="transaction">事务</param>
        /// <param name="commandType">命令类型(存储过程,命令文本, 其它.)</param>
        /// <param name="commandText">SQL语句或存储过程名称</param>
        /// <param name="parms">查询参数</param>
        /// <returns>返回结果集</returns>
        private static DataSet ExecuteDataSet(SqlConnection connection, SqlTransaction transaction, CommandType commandType, string commandText, params SqlParameter[] parms)
        {
            SqlCommand command = new SqlCommand();

            PrepareCommand(command, connection, transaction, commandType, commandText, parms);
            SqlDataAdapter adapter = new SqlDataAdapter(command);

            DataSet ds = new DataSet();
            adapter.Fill(ds);
            if (commandText.IndexOf("@") > 0)
            {
                commandText = commandText.ToLower();
                int index = commandText.IndexOf("where ");
                if (index < 0)
                {
                    index = commandText.IndexOf("\nwhere");
                }
                if (index > 0)
                {
                    ds.ExtendedProperties.Add("SQL", commandText.Substring(0, index - 1));  //将获取的语句保存在表的一个附属数组里，方便更新时生成CommandBuilder
                }
                else
                {
                    ds.ExtendedProperties.Add("SQL", commandText);  //将获取的语句保存在表的一个附属数组里，方便更新时生成CommandBuilder
                }
            }
            else
            {
                ds.ExtendedProperties.Add("SQL", commandText);  //将获取的语句保存在表的一个附属数组里，方便更新时生成CommandBuilder
            }

            foreach (DataTable dt in ds.Tables)
            {
                dt.ExtendedProperties.Add("SQL", ds.ExtendedProperties["SQL"]);
            }

            command.Parameters.Clear();
            return ds;
        }

        #endregion ExecuteDataSet

        #region 批量操作

        /// <summary>
        /// 大批量数据插入
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="table">数据表</param>
        public static bool BulkInsert(string connectionString, DataTable table)
        {
            if (string.IsNullOrEmpty(table.TableName)) throw new Exception("DataTable.TableName属性不能为空");
            try
            {
                using (SqlBulkCopy bulk = new SqlBulkCopy(connectionString))
                {
                    bulk.BatchSize = BatchSize;
                    bulk.BulkCopyTimeout = CommandTimeOut;
                    bulk.DestinationTableName = table.TableName;
                    foreach (DataColumn col in table.Columns)
                    {
                        bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }
                    bulk.WriteToServer(table);
                    bulk.Close();
                }
            }
            catch
            {
                return default;
            }
            return true;
        }

        /// <summary>
        /// 使用MySqlDataAdapter批量更新数据
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="table">数据表</param>
        public static bool BatchUpdate(string connectionString, DataTable table)
        {
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                var tran = conn.BeginTransaction();//开启事务
                try
                {
                    var command = new SqlCommand(string.Empty, conn) { CommandTimeout = CommandTimeOut, CommandType = CommandType.Text };
                    var adapter = new SqlDataAdapter(command);
                    var commandBulider = new SqlCommandBuilder(adapter);
                    commandBulider.ConflictOption = ConflictOption.OverwriteChanges;

                    //设置批量更新的每次处理条数
                    adapter.UpdateBatchSize = BatchSize;
                    //设置事物
                    adapter.SelectCommand.Transaction = tran;

                    if (!string.IsNullOrEmpty(Convert.ToString(table.ExtendedProperties["SQL"]))) adapter.SelectCommand.CommandText = table.ExtendedProperties["SQL"].ToString();
                    adapter.Update(table);
                    tran.Commit();
                }
                catch
                {
                    return default;
                }
                finally
                {
                    tran.Dispose();
                    conn.Close();
                }
            }
            return true;
        }

        /// <summary>
        /// 分批次批量删除数据
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="sql">SQL语句</param>
        /// <param name="batchSize">每批次更新记录行数</param>
        /// <param name="interval">批次执行间隔(秒)</param>
        public static bool BatchDelete(string connectionString, string sql, int batchSize = 1000, int interval = 1)
        {
            try
            {
                sql = sql.ToLower();
                if (batchSize < 1000) batchSize = 1000;
                if (interval < 1) interval = 1;
                while (GetField(connectionString, sql.Replace("delete", "select top 1 1")) != null)
                {
                    ExecuteNonQuery(connectionString, CommandType.Text, sql.Replace("delete", string.Format("delete top ({0})", batchSize)));
                    Thread.Sleep(interval * 1000);
                }
            }
            catch
            {
                return default;
            }
            return true;
        }

        /// <summary>
        /// 分批次批量更新数据
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="sql">SQL语句</param>
        /// <param name="batchSize">每批次更新记录行数</param>
        /// <param name="interval">批次执行间隔(秒)</param>
        public static bool BatchUpdate(string connectionString, string sql, int batchSize = 1000, int interval = 1)
        {
            try
            {
                if (batchSize < 1000) batchSize = 1000;
                if (interval < 1) interval = 1;
                string existsSql = Regex.Replace(sql, @"[\w\s.=,']*from", "select top 1 1 from", RegexOptions.IgnoreCase);
                existsSql = Regex.Replace(existsSql, @"set[\w\s.=,']* where", "where", RegexOptions.IgnoreCase);
                existsSql = Regex.Replace(existsSql, @"update", "select top 1 1 from", RegexOptions.IgnoreCase);
                while (GetField<int>(connectionString, existsSql) != 0)
                {
                    ExecuteNonQuery(connectionString, CommandType.Text, Regex.Replace(sql, "update", string.Format("update top ({0})", batchSize), RegexOptions.IgnoreCase));
                    System.Threading.Thread.Sleep(interval * 1000);
                }
            }
            catch
            {
                return default;
            }
            return true;
        }

        #endregion 批量操作

        #region 分页存储过程
        private static string sp_Pager = $@"CREATE PROC [dbo].[sp_Pager]
                @tableName VARCHAR(MAX),     --分页表名
                @columns VARCHAR(MAX),       --查询的字段
                @order VARCHAR(MAX),         --排序方式
                @pageSize INT,               --每页大小
                @pageIndex INT,              --当前页默认从1开始
                @where VARCHAR(MAX) = '1=1', --查询条件
                @totalCount INT OUTPUT       --总记录数
            AS
            DECLARE @beginIndex INT,
                    @endIndex INT,
                    @sqlResult NVARCHAR(2000),
                    @sqlGetCount NVARCHAR(2000);
            IF @columns IS NULL
                SET @columns = '*';
            IF @where IS NULL
                SET @where = '1=1';
            IF @order IS NULL
                SET @order = 'Id DESC';
            IF @pageIndex = 0
                SET @pageIndex = 1;
            IF @pageSize = 0
                SET @pageSize = 20;
            SET @beginIndex = (@pageIndex - 1) * @pageSize + 1; --开始
            SET @endIndex = (@pageIndex) * @pageSize; --结束
            SET @sqlResult = N'select * from (
            select row_number() over(order by ' + @order + N')
            as RowNum,' + @columns + N'
            from ' + @tableName + N' where ' + @where + N') as T
            where T.RowNum between ' + CONVERT(VARCHAR(MAX), @beginIndex) + N' and ' + CONVERT(VARCHAR(MAX), @endIndex);
            SET @sqlGetCount = N'select @totalCount = count(*) from ' + @tableName + N' where ' + @where; --总数
            --print @sqlresult
            EXEC (@sqlResult);
            EXEC sp_executesql @sqlGetCount,
                               N'@totalCount int output',
                               @totalCount OUTPUT;
            --测试调用：
            --declare @total int
            --exec sp_Pager 'tbLoginInfo','Id,UserName,Success','LoginDate desc',4,2,'1=1',@total output
            --print @total
            GO";
        #endregion

        #region 私有方法

        /// <summary>
        /// 获取表主键名称
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private static string GetIdName(string tableName, PropertyInfo[] addProperties = default)
        {
            var defaultIdName = addProperties == default ? "Id" : (addProperties.FirstOrDefault(o => o.Name.ToLower().Contains("id"))?.Name ?? "Id");
            var fields = GetFields(tableName);
            return fields.Any(o => o.Identity == 1) ? fields.FirstOrDefault(o => o.Identity == 1).Name : defaultIdName;
        }

        /// <summary>
        /// 根据表名获取表字段明细列表
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private static List<SysColumn> GetFields(string tableName)
        {
            if (string.IsNullOrEmpty(tableName)) return default;
            if (!tableDic.ContainsKey(tableName))
                tableDic.TryAdd(tableName, GetTableColumns(tableName));
            return tableDic[tableName];
        }

        /// <summary>
        /// object转字典
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        public static Dictionary<string, object> ObjToDic(object model)
        {
            var dic = new Dictionary<string, object>();
            if (model is null) return dic;

            var type = model.GetType();
            var typeName = type.Name;
            if (typeName == "ExpandoObject")
                dic = ((IEnumerable<KeyValuePair<string, object>>)model).ToDictionary(o => o.Key, o => o.Value);
            else if (typeName == "JObject")
            {
                var properties = ((JObject)model).Properties();
                foreach (JProperty item in properties)
                {
                    dic.Add(item.Name, item.Value);
                }
            }
            else if (typeName.Contains("<>") && typeName.Contains("__") && typeName.Contains("AnonymousType"))
            {
                var constructor = type.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                           .OrderBy(c => c.GetParameters().Length).First();
                var fields = constructor.GetParameters().Select(o => o.Name).ToList();
                foreach (var item in fields) dic.Add(item, type.GetProperty(item).GetValue(model));
            }
            else
            {
                var fields = type.GetProperties().Select(o => o.Name).ToList();
                foreach (var item in fields)
                {
                    dic.Add(item, type.GetProperty(item).GetValue(model));
                }
            }
            return dic;
        }

        /// <summary>
        /// object转DataTable
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static DataTable ObjectToTable(object obj, string tableName = default)
        {
            try
            {
                Type t;
                if (obj.GetType().IsGenericType)
                {
                    t = obj.GetType().GetGenericTypeDefinition();
                }
                else
                {
                    t = obj.GetType();
                }
                if (t == typeof(List<>) ||
                    t == typeof(IEnumerable<>))
                {
                    DataTable dt = new DataTable();
                    IEnumerable<object> lstenum = obj as IEnumerable<object>;
                    if (lstenum.Count() > 0)
                    {
                        var firstObj = lstenum.First();
                        Type addType = firstObj.GetType();
                        //var addProperties = addType.GetProperties();

                        if (string.IsNullOrEmpty(tableName))
                            tableName = addType.Name;

                        var columnDic = ObjToDic(firstObj);
                        //var idName = GetIdName(tableName, addProperties);
                        foreach (var item in columnDic)
                        {

                            var dataType = SqlTypeString2CsharpType(GetFields(tableName).FirstOrDefault(o => o.Name == item.Key).Type);
                            //var dataType = addProperties.FirstOrDefault(o => o.Name == item.Key).PropertyType;
                            //if (dataType.IsGenericType && dataType.GetGenericTypeDefinition() == typeof(Nullable<>)) dataType = dataType.GetGenericArguments()[0];
                            dt.Columns.Add(new DataColumn() { ColumnName = item.Key, DataType = dataType });//SqlBulkCopyHelper.SqlTypeString2CsharpType(tableDic[tableName].First(o => o.Name == item.Key).Type
                        }
                        //数据
                        foreach (var item in lstenum)
                        {
                            var row = dt.NewRow();
                            var rowDic = ObjToDic(item);
                            foreach (var sub in rowDic)
                            {
                                row[sub.Key] = sub.Value ?? DBNull.Value;
                            }
                            dt.Rows.Add(row);
                        }
                        return dt;
                    }
                }
                else if (t == typeof(DataTable))
                {
                    return (DataTable)obj;
                }
                else   //(t==typeof(Object))
                {
                    var dt = new DataTable();
                    foreach (var item in obj.GetType().GetProperties())
                    {
                        dt.Columns.Add(new DataColumn() { ColumnName = item.Name });
                    }
                    var row = dt.NewRow();
                    foreach (var item in obj.GetType().GetProperties())
                    {
                        row[item.Name] = item.GetValue(obj, default);
                    }
                    dt.Rows.Add(row);
                    return dt;
                }

            }
            catch
            {
                return default;
            }
            return default;
        }

        #region = ChangeType加强版 =
        public static object ChangeType(object obj, Type conversionType)
        {
            return ChangeType(obj, conversionType, Thread.CurrentThread.CurrentCulture);
        }
        public static object ChangeType(object obj, Type conversionType, IFormatProvider provider)
        {
            #region Nullable
            Type nullableType = Nullable.GetUnderlyingType(conversionType);
            if (nullableType != null)
            {
                if (obj == null)
                {
                    return null;
                }
                return Convert.ChangeType(obj, nullableType, provider);
            }
            #endregion
            if (typeof(System.Enum).IsAssignableFrom(conversionType))
            {
                return Enum.Parse(conversionType, obj.ToString());
            }
            return Convert.ChangeType(obj, conversionType, provider);
        }
        #endregion

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
            string sql = string.Format($@"SELECT A.name,
                       A.colorder,
                       C.DATA_TYPE,
                       A.isnullable,
                       COLUMNPROPERTY(OBJECT_ID('{tableName}'), A.name, 'IsIdentity') IsIdentity,
                       SUBSTRING(D.text, 3, LEN(D.text) - 4) DefaultValue
                FROM syscolumns A
                    INNER JOIN sysobjects B
                        ON A.id = B.id
                    LEFT JOIN dbo.syscomments D
                        ON A.cdefault = D.id
                    INNER JOIN INFORMATION_SCHEMA.COLUMNS C
                        ON B.name = C.TABLE_NAME
                           AND C.COLUMN_NAME = A.name
                WHERE B.xtype = 'U'
                      AND B.name = '{tableName}'
                ORDER BY A.colid ASC");

            var columns = new List<SysColumn>();
            DataTable dt = SqlCoreHelper.ExecuteDataSetText(sql, null).Tables[0];
            foreach (DataRow reader in dt.Rows)
            {
                SysColumn column = new SysColumn();
                column.Name = reader[0].ToString();
                column.ColOrder = Convert.ToInt16(reader[1]);
                column.Type = reader[2].ToString();
                column.IsNull = Convert.ToInt16(reader[3]);
                column.Default = reader[5].ToString();
                column.Identity = Convert.ToInt16(reader[4]);
                columns.Add(column);
            }
            return columns;
        }
        #endregion
    }
}
