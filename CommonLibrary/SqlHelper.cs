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

namespace CommonLibrary
{
    /// <summary>
    /// SqlHelper操作类
    /// </summary>
    public sealed partial class SqlHelper
    {
        //加载appsetting.json
        private readonly static IConfiguration configuration = new ConfigurationBuilder()
      .SetBasePath(Directory.GetCurrentDirectory())
     .AddJsonFile("appsettings.json").Build();

        private static ConcurrentDictionary<string, List<SysColumn>> tableDic = new ConcurrentDictionary<string, List<SysColumn>>();

        /// <summary>
        /// 批量操作每批次记录数
        /// </summary>
        public static int BatchSize = 2000;

        /// <summary>
        /// 超时时间
        /// </summary>
        public static int CommandTimeOut = 600;

        /// <summary>
        /// 数据库连接字符串
        /// </summary>
        public static string connectionString = configuration["DBSetting:ConnectString"];

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

            var idName = GetIdName(tableName,addProperties);

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
                                Property.SetValue(RowInstance, Convert.ChangeType(reader.GetValue(Ordinal), Property.PropertyType), null);
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
                                Property.SetValue(RowInstance, Convert.ChangeType(reader.GetValue(Ordinal), Property.PropertyType), null);
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
                                        Property.SetValue(RowInstance, Convert.ChangeType(reader.GetValue(Ordinal), Property.PropertyType), null);
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
        /// <param name="pageIndex">当前页</param>
        /// <param name="where">查询条件</param>
        /// <param name="totalCount">总记录数</param>
        public static List<T> GetPager<T>(string tableName, string columns, string order, int pageSize, int pageIndex, string where, out int totalCount)
        {
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
                                        Property.SetValue(RowInstance, Convert.ChangeType(reader.GetValue(Ordinal), Property.PropertyType), null);
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
                                    itemValue = Convert.ChangeType(fieldValue, parameter.ParameterType);
                                }
                                else
                                {
                                    Type genericTypeDefinition = parameter.ParameterType.GetGenericTypeDefinition();
                                    if (genericTypeDefinition == typeof(Nullable<>))
                                    {
                                        itemValue = Convert.ChangeType(fieldValue, Nullable.GetUnderlyingType(parameter.ParameterType));
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
        /// <param name="pageIndex">当前页</param>
        /// <param name="where">查询条件</param>
        /// <param name="totalCount">总记录数</param>
        public static IList GetAnonymousPager(Type anonymousType, string tableName, string columns, string order, int pageSize, int pageIndex, string where, out int totalCount)
        {
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
                                    itemValue = Convert.ChangeType(fieldValue, parameter.ParameterType);
                                }
                                else
                                {
                                    Type genericTypeDefinition = parameter.ParameterType.GetGenericTypeDefinition();
                                    if (genericTypeDefinition == typeof(Nullable<>))
                                    {
                                        itemValue = Convert.ChangeType(fieldValue, Nullable.GetUnderlyingType(parameter.ParameterType));
                                    }
                                }
                            }
                            values[i] = itemValue;
                        }
                        list.Add(constructor.Invoke(values));
                    }
                }
                catch { }

                totalCount = Convert.ToInt32(paras[6].Value);//获取存储过程输出参数的值 即当前记录总数
            }

            return list;
        }

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
                return (T)Convert.ChangeType(result, typeof(T)); ;
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
        public static void BulkInsert(string connectionString, DataTable table)
        {
            if (string.IsNullOrEmpty(table.TableName)) throw new Exception("DataTable.TableName属性不能为空");
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

        /// <summary>
        /// 使用MySqlDataAdapter批量更新数据
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="table">数据表</param>
        public static void BatchUpdate(string connectionString, DataTable table)
        {
            SqlConnection connection = new SqlConnection(connectionString);

            SqlCommand command = connection.CreateCommand();
            command.CommandTimeout = CommandTimeOut;
            command.CommandType = CommandType.Text;
            SqlDataAdapter adapter = new SqlDataAdapter(command);
            SqlCommandBuilder commandBulider = new SqlCommandBuilder(adapter);
            commandBulider.ConflictOption = ConflictOption.OverwriteChanges;

            SqlTransaction transaction = null;
            try
            {
                connection.Open();
                transaction = connection.BeginTransaction();
                //设置批量更新的每次处理条数
                adapter.UpdateBatchSize = BatchSize;
                //设置事物
                adapter.SelectCommand.Transaction = transaction;

                if (table.ExtendedProperties["SQL"] != null)
                {
                    adapter.SelectCommand.CommandText = table.ExtendedProperties["SQL"].ToString();
                }
                adapter.Update(table);
                transaction.Commit();
            }
            catch (SqlException ex)
            {
                if (transaction != null) transaction.Rollback();
                throw ex;
            }
            finally
            {
                connection.Close();
                connection.Dispose();
            }
        }

        /// <summary>
        /// 分批次批量删除数据
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="sql">SQL语句</param>
        /// <param name="batchSize">每批次更新记录行数</param>
        /// <param name="interval">批次执行间隔(秒)</param>
        public static void BatchDelete(string connectionString, string sql, int batchSize = 1000, int interval = 1)
        {
            sql = sql.ToLower();

            if (batchSize < 1000) batchSize = 1000;
            if (interval < 1) interval = 1;
            while (GetField(connectionString, sql.Replace("delete", "select top 1 1")) != null)
            {
                ExecuteNonQuery(connectionString, CommandType.Text, sql.Replace("delete", string.Format("delete top ({0})", batchSize)));
                System.Threading.Thread.Sleep(interval * 1000);
            }
        }

        /// <summary>
        /// 分批次批量更新数据
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="sql">SQL语句</param>
        /// <param name="batchSize">每批次更新记录行数</param>
        /// <param name="interval">批次执行间隔(秒)</param>
        public static void BatchUpdate(string connectionString, string sql, int batchSize = 1000, int interval = 1)
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

        #endregion 批量操作

        #region 分页存储过程
        private static string sp_Pager = $@"CREATE proc [dbo].[sp_Pager]
            @tableName varchar(64),  --分页表名
            @columns varchar(1000),  --查询的字段
            @order varchar(256),    --排序方式
            @pageSize int,  --每页大小
            @pageIndex int,  --当前页
            @where varchar(2000) = '1=1',  --查询条件
            @totalCount int output  --总记录数
            as
            declare @beginIndex int,@endIndex int,@sqlResult nvarchar(2000),@sqlGetCount nvarchar(2000)
            set @beginIndex = (@pageIndex - 1) * @pageSize + 1  --开始
            set @endIndex = (@pageIndex) * @pageSize  --结束
            set @sqlresult = 'select '+@columns+' from (
            select row_number() over(order by '+ @order +')
            as Rownum,'+@columns+'
            from '+@tableName+' where '+ @where +') as T
            where T.Rownum between ' + CONVERT(varchar(max),@beginIndex) + ' and ' + CONVERT(varchar(max),@endIndex)
            set @sqlGetCount = 'select @totalCount = count(*) from '+@tablename+' where ' + @where  --总数
            --print @sqlresult
            exec(@sqlresult)
            exec sp_executesql @sqlGetCount,N'@totalCount int output',@totalCount output
            --测试调用：
            --declare @total int
            --exec sp_Pager 'tbLoginInfo','Id,UserName,Success','LoginDate desc',4,2,'1=1',@total output
            --print @total
 
            GO";
        #endregion

        #region 私有方法

        /// <summary>
        /// object转字典
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        private static Dictionary<string, object> ObjToDic(object model)
        {
            var dic = new Dictionary<string, object>();
            if (model is null) return dic;

            var type = model.GetType();
            var typeName = type.Name;
            if (typeName == "ExpandoObject")
                dic = ((IEnumerable<KeyValuePair<string, object>>)model).ToDictionary(o => o.Key, o => o.Value);
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
        /// 获取表主键名称
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private static string GetIdName(string tableName, PropertyInfo[] addProperties)
        {
            if (!tableDic.ContainsKey(tableName))
                tableDic.TryAdd(tableName, MSTest.SqlBulkCopyHelper.GetTableColumns(tableName));
            return tableDic[tableName].Any(o => o.Identity == 1) ? tableDic[tableName].First(o => o.Identity == 1).Name : addProperties.FirstOrDefault(o => o.Name.ToLower().Contains("id"))?.Name ?? "Id";
        }
        #endregion
    }
}
