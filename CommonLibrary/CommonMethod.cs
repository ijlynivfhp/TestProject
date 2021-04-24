using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommonLibrary
{
    public partial class CommonMethod
    {
        #region 实体对象集合转成动态类Or集合
        public static TNew DtoListToTOrTList<TOld,TNew>(TOld tOld) {
            string str = JsonConvert.SerializeObject(tOld);
            return JsonConvert.DeserializeObject<TNew>(str);
        }
        #endregion

        #region 泛型集合转DataTable  
        /// <summary>  
        /// 泛型集合转DataTable  
        /// </summary>  
        /// <typeparam name="T">集合类型</typeparam>  
        /// <param name="entityList">泛型集合</param>  
        /// <returns>DataTable</returns>  
        public static DataTable ListToDt<T>(IList<T> entityList)
        {
            if (entityList == null) return null;
            DataTable dt = CreateTable<T>();
            Type entityType = typeof(T);
            //PropertyInfo[] properties = entityType.GetProperties();  
            PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(entityType);
            foreach (T item in entityList)
            {
                DataRow row = dt.NewRow();
                foreach (PropertyDescriptor property in properties)
                {
                    row[property.Name] = property.GetValue(item);
                }
                dt.Rows.Add(row);
            }

            return dt;
        }
        #endregion

        #region 创建DataTable的结构  
        private static DataTable CreateTable<T>()
        {
            Type entityType = typeof(T);
            //PropertyInfo[] properties = entityType.GetProperties();  
            PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(entityType);
            //生成DataTable的结构  
            DataTable dt = new DataTable();
            foreach (PropertyDescriptor prop in properties)
            {
                dt.Columns.Add(prop.Name);
            }
            return dt;
        }
        #endregion

    }
}
