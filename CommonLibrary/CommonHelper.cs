using DTO;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace CommonLibrary
{
    public static partial class CommonHelper
    {
        /// <summary>
        /// 获取测试数据DataTable
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public static DataTable GetTestDataTable(int count = 10000)
        {
            return CommonMethod.ListToDt(GetTestListDto(count));
        }

        /// <summary>
        /// 获取测试数据
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public static List<CommonDto> GetTestListDto(int count = 10000)
        {
            var sw = new Stopwatch(); sw.Start();
            Console.WriteLine("开始构造测试数据");
            var list = new List<CommonDto>();

            for (int i = 0; i < count; i++)
            {
                list.Add(GetDtoByClass<CommonDto>());
            }
            Console.WriteLine("获取测试数据" + count + "条：结束，耗时：" + sw.Elapsed.TotalSeconds + "秒");
            return list;
        }

        /// <summary>
        /// 获取测试数据
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public static List<CommonDtoTwo> GetTestListDtoTwo(int count = 10000)
        {
            var sw = new Stopwatch(); sw.Start();
            Console.WriteLine("开始构造测试数据");
            var list = new List<CommonDtoTwo>();

            for (int i = 0; i < count; i++)
            {
                list.Add(GetDtoByClass<CommonDtoTwo>());
            }
            Console.WriteLine("获取测试数据" + count + "条：结束，耗时：" + sw.Elapsed.TotalSeconds + "秒");
            return list;
        }
        /// <summary>
        /// 获取动态测试数据
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>

        public static List<dynamic> GetTestListDyc(int count = 10000)
        {
            var listDtos = GetTestListDto();
            var sw = new Stopwatch(); sw.Start();
            Console.WriteLine("开始转换测试数据");
            var list = CommonMethod.DtoListToTOrTList<List<CommonDto>, List<dynamic>>(listDtos);
            Console.WriteLine("获取转换测试数据" + count + "条：结束，耗时：" + sw.Elapsed.TotalSeconds + "秒");
            return list;

        }
        /// <summary>
        /// 初始化一个实例并赋值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        private static T GetDtoByClass<T>()
        {
            var removeFields = new List<string>() {"Id", "FieldU", "FieldV", "FieldW", "FieldX" };
            var dto = System.Activator.CreateInstance<T>();
            var type = typeof(T);
            var propertys = type.GetProperties();
            propertys.AsParallel().ForAll(o =>
            {
                string sufStr = string.Empty;
                for (int i = 0; i < 5; i++)
                {
                    sufStr += ListEnStr[rand.Next(26)];
                }
                if (removeFields.Contains(o.Name))
                    return;
                o.SetValue(dto, o.Name + sufStr, null);
            });
            return dto;
        }
        public static readonly Random rand = new Random();
        public static readonly List<string> ListEnStr = new List<string>() { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };



        /// <summary>
        /// 将DataTable 转成 匿名对象集合
        /// </summary>
        /// <param name="dataTable"></param>
        /// <param name="template"></param>
        /// <param name="func"></param>
        /// <returns></returns>
        public static IList ToAnonymousList(this DataTable dataTable, object template = default, Func<string, object, DataRow, object> func = default)
        {
            Type GenericType = template.GetType();
            Type typeFromHandle = typeof(List<>);
            Type type = typeFromHandle.MakeGenericType(GenericType);
            IList list = Activator.CreateInstance(type) as IList;
            if (dataTable == null || dataTable.Rows.Count == 0)
            {
                return list;
            }

            ConstructorInfo constructorInfo = (from c in GenericType.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)

                                               orderby c.GetParameters().Length

                                               select c).First();

            ParameterInfo[] parameters = constructorInfo.GetParameters();

            object[] array = new object[parameters.Length];

            foreach (DataRow row in dataTable.Rows)

            {
                int num = 0;
                ParameterInfo[] array2 = parameters;
                foreach (ParameterInfo parameterInfo in array2)
                {
                    object obj = null;
                    if (parameterInfo.Name == "Index")

                    {
                        obj = list.Count + 1;
                    }
                    else if (dataTable.Columns.Contains(parameterInfo.Name) && row[parameterInfo.Name] != null && row[parameterInfo.Name] != DBNull.Value)

                    {
                        if (!parameterInfo.ParameterType.IsGenericType)
                        {
                            obj = Convert.ChangeType(row[parameterInfo.Name], parameterInfo.ParameterType);
                        }
                        else
                        {
                            Type genericTypeDefinition = parameterInfo.ParameterType.GetGenericTypeDefinition();

                            if (genericTypeDefinition == typeof(Nullable<>))

                            {
                                obj = Convert.ChangeType(row[parameterInfo.Name], Nullable.GetUnderlyingType(parameterInfo.ParameterType));
                            }
                        }
                    }
                    if (func != null)
                    {
                        obj = func(parameterInfo.Name, obj, row);
                    }
                    array[num++] = obj;
                }
                list.Add(constructorInfo.Invoke(array));
            }
            return list;
        }

        /// <summary>
        /// 将DataRow 转成 dynamic
        /// </summary>
        /// <param name="dr">要转换的DataRow对象</param>
        /// <param name="filterFields">要筛选的列。默认不传，此时返回全部列</param>
        /// <param name="includeOrExclude">指定上一个参数条件是要保留的列还是要排除的列</param>
        /// <returns></returns>
        public static dynamic ToDynamic(this DataRow dr, string[] filterFields = default, bool includeOrExclude = true)
        {

            var isFilter = filterFields != null && filterFields.Any();

            IEnumerable reservedColumns = dr.Table.Columns;

            if (isFilter)

                reservedColumns = dr.Table.Columns.Cast<DataColumn>().Where(c => filterFields.Contains(c.ColumnName) == includeOrExclude);

            dynamic model = new ExpandoObject();

            var dict = (IDictionary<string, object>)model;

            foreach (DataColumn column in reservedColumns)
            {
                dict[column.ColumnName] = dr[column];
            }
            return dict;
        }


    }
}
