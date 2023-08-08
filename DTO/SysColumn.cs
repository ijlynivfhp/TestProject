using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DTO
{
    #region 基础数据
    /// <summary>
    /// 读取数据库字段属性类
    /// </summary>
    public class SysColumn
    {
        public string Name { get; set; }
        public short ColOrder { get; set; }
        public string Type { get; set; }
        public short IsNull { get; set; }
        public short Identity { get; set; }
        public string Default { get; set; }
    }
    #endregion
}
