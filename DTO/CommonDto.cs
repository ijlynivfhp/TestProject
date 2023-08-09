using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DTO
{
    /// <summary>
    /// 共用测试对象
    /// </summary>
    public partial class CommonDto
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        public string FieldA { get; set; }
        public string FieldB { get; set; }
        public string FieldC { get; set; }
        public string FieldD { get; set; }
        public string FieldE { get; set; }
        public string FieldF { get; set; }
        public string FieldG { get; set; }
        public string FieldH { get; set; }
        public string FieldI { get; set; }
        public string FieldJ { get; set; }
        public string FieldK { get; set; }
        public string FieldL { get; set; }
        public string FieldM { get; set; }
        public string FieldN { get; set; }
        public string FieldP { get; set; }
        public string FieldQ { get; set; }
        public string FieldR { get; set; }
        public string FieldS { get; set; }
        public string FieldT { get; set; }
        public DateTime FieldU { get; set; } = DateTime.Now;
        public decimal? FieldV { get; set; }
        public int FieldX { get; set; }
    }

    public partial class CommonDtoTwo
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        public string FieldA { get; set; }
        public string FieldB { get; set; }
        public string FieldC { get; set; }
        public string FieldD { get; set; }
        public string FieldE { get; set; }
        public string FieldF { get; set; }
        public string FieldG { get; set; }
        public string FieldH { get; set; }
        public string FieldI { get; set; }
        public string FieldJ { get; set; }
        public string FieldK { get; set; }
        public string FieldL { get; set; }
        public string FieldM { get; set; }
        public string FieldN { get; set; }
        public string FieldO { get; set; }
        public string FieldQ { get; set; }
        public string FieldR { get; set; }
        public string FieldS { get; set; }
        public string FieldT { get; set; }
        public decimal FieldW { get; set; }
        public int FieldX { get; set; }
    }

    public class TestDto
    {
        public int Id { get; set; }
        public int PwId { get; set; }
        public Guid ProId { get; set; }
        public string WorkerName { get; set; }
        public string PaperSigned { get; set; }
    }
    /// <summary>
    /// 备用扩展列数
    /// </summary>
    //public partial class CommonDto
    //{
    //    public string FieldAA { get; set; }
    //    public string FieldBB { get; set; }
    //    public string FieldCC { get; set; }
    //    public string FieldDD { get; set; }
    //    public string FieldEE { get; set; }
    //    public string FieldFF { get; set; }
    //    public string FieldGG { get; set; }
    //    public string FieldHH { get; set; }
    //    public string FieldII { get; set; }
    //    public string FieldJJ { get; set; }
    //    public string FieldKK { get; set; }
    //    public string FieldLL { get; set; }
    //    public string FieldMM { get; set; }
    //    public string FieldNN { get; set; }
    //    public string FieldOO { get; set; }
    //    public string FieldPP { get; set; }
    //    public string FieldQQ { get; set; }
    //    public string FieldRR { get; set; }
    //    public string FieldSS { get; set; }
    //    public string FieldTT { get; set; }
    //}

    public partial class Sync_DataTransfer
    {
        #region 私有成员
        private Int32? _Id;
        private String _PkValue;
        private String _PkCode;
        private String _PkName;
        private Int32? _SyncType;
        private String _SyncTypeName;
        private Int32? _CoId;
        private String _CoName;
        private Guid? _ProId;
        private String _ProName;
        private Int32? _BizType;
        private String _BizTypeName;
        private String _FunctionName;
        private String _RequestData;
        private String _ResponseData;
        private String _SyncResult;
        private Int32? _SyncStatus;
        private Int32? _SyncCount;
        private String _SyncText;
        private Int32? _ServiceType;
        private String _ServiceTypeName;
        private Int32? _ConfirmStatus;
        private Int32? _HandleLevel;
        private String _HandleLevelName;
        private Int32? _SyncSubType;
        private String _SyncSubTypeName;
        private Int32? _SyncAction;
        private String _SyncActionName;
        private String _SyncRemark;
        private String _CreateUser;
        private DateTime? _CreateTime;
        private String _UpdateUser;
        private DateTime? _UpdateTime;
        private Int32? _ParentId;
        private String _DataSource;
        private String _ExtraStr;
        private Int32? _ExtraInt;
        #endregion

        #region 公共成员
        /// <summary>
        /// 主键
        /// </summary>
        public Int32? Id
        {
            get { return _Id; }
            set { _Id = value; }
        }
        /// <summary>
        /// 关联Id
        /// </summary>
        public String PkValue
        {
            get { return _PkValue; }
            set { _PkValue = value; }
        }
        /// <summary>
        /// 关联编码
        /// </summary>
        public String PkCode
        {
            get { return _PkCode; }
            set { _PkCode = value; }
        }
        /// <summary>
        /// 关联Code
        /// </summary>
        public String PkName
        {
            get { return _PkName; }
            set { _PkName = value; }
        }
        /// <summary>
        /// 同步平台Id
        /// </summary>
        public Int32? SyncType
        {
            get { return _SyncType; }
            set { _SyncType = value; }
        }
        /// <summary>
        /// 同步平台名称
        /// </summary>
        public String SyncTypeName
        {
            get { return _SyncTypeName; }
            set { _SyncTypeName = value; }
        }
        /// <summary>
        /// 企业Id
        /// </summary>
        public Int32? CoId
        {
            get { return _CoId; }
            set { _CoId = value; }
        }
        /// <summary>
        /// 企业名称
        /// </summary>
        public String CoName
        {
            get { return _CoName; }
            set { _CoName = value; }
        }
        /// <summary>
        /// 项目Id
        /// </summary>
        public Guid? ProId
        {
            get { return _ProId; }
            set { _ProId = value; }
        }
        /// <summary>
        /// 同步项目名称
        /// </summary>
        public String ProName
        {
            get { return _ProName; }
            set { _ProName = value; }
        }
        /// <summary>
        /// 同步节点
        /// </summary>
        public Int32? BizType
        {
            get { return _BizType; }
            set { _BizType = value; }
        }
        /// <summary>
        /// 同步节点名称
        /// </summary>
        public String BizTypeName
        {
            get { return _BizTypeName; }
            set { _BizTypeName = value; }
        }
        /// <summary>
        /// 函数名称
        /// </summary>
        public String FunctionName
        {
            get { return _FunctionName; }
            set { _FunctionName = value; }
        }
        /// <summary>
        /// 请求数据
        /// </summary>
        public String RequestData
        {
            get { return _RequestData; }
            set { _RequestData = value; }
        }
        /// <summary>
        /// 接收数据
        /// </summary>
        public String ResponseData
        {
            get { return _ResponseData; }
            set { _ResponseData = value; }
        }
        /// <summary>
        /// 同步结果
        /// </summary>
        public String SyncResult
        {
            get { return _SyncResult; }
            set { _SyncResult = value; }
        }
        /// <summary>
        /// 同步状态
        /// </summary>
        public Int32? SyncStatus
        {
            get { return _SyncStatus; }
            set { _SyncStatus = value; }
        }
        /// <summary>
        /// 同步次数
        /// </summary>
        public Int32? SyncCount
        {
            get { return _SyncCount; }
            set { _SyncCount = value; }
        }
        /// <summary>
        /// 同步结果显示
        /// </summary>
        public String SyncText
        {
            get { return _SyncText; }
            set { _SyncText = value; }
        }
        /// <summary>
        /// 业务类型
        /// </summary>
        public Int32? ServiceType
        {
            get { return _ServiceType; }
            set { _ServiceType = value; }
        }
        /// <summary>
        /// 业务类型名称
        /// </summary>
        public String ServiceTypeName
        {
            get { return _ServiceTypeName; }
            set { _ServiceTypeName = value; }
        }
        /// <summary>
        /// 确认状态
        /// </summary>
        public Int32? ConfirmStatus
        {
            get { return _ConfirmStatus; }
            set { _ConfirmStatus = value; }
        }
        /// <summary>
        /// 待处理待级
        /// </summary>
        public Int32? HandleLevel
        {
            get { return _HandleLevel; }
            set { _HandleLevel = value; }
        }
        /// <summary>
        /// 待处理等级名称
        /// </summary>
        public String HandleLevelName
        {
            get { return _HandleLevelName; }
            set { _HandleLevelName = value; }
        }
        /// <summary>
        /// 同步子类型
        /// </summary>
        public Int32? SyncSubType
        {
            get { return _SyncSubType; }
            set { _SyncSubType = value; }
        }
        /// <summary>
        /// 同步子类型名称
        /// </summary>
        public String SyncSubTypeName
        {
            get { return _SyncSubTypeName; }
            set { _SyncSubTypeName = value; }
        }
        /// <summary>
        /// 同步操作行为
        /// </summary>
        public Int32? SyncAction
        {
            get { return _SyncAction; }
            set { _SyncAction = value; }
        }
        /// <summary>
        /// 同步操作行为名称
        /// </summary>
        public String SyncActionName
        {
            get { return _SyncActionName; }
            set { _SyncActionName = value; }
        }
        /// <summary>
        /// 备注
        /// </summary>
        public String SyncRemark
        {
            get { return _SyncRemark; }
            set { _SyncRemark = value; }
        }
        /// <summary>
        /// 创建人
        /// </summary>
        public String CreateUser
        {
            get { return _CreateUser; }
            set { _CreateUser = value; }
        }
        /// <summary>
        /// 创建时间
        /// </summary>
        public DateTime? CreateTime
        {
            get { return _CreateTime; }
            set { _CreateTime = value; }
        }
        /// <summary>
        /// 更新人
        /// </summary>
        public String UpdateUser
        {
            get { return _UpdateUser; }
            set { _UpdateUser = value; }
        }
        /// <summary>
        /// 更新时间
        /// </summary>
        public DateTime? UpdateTime
        {
            get { return _UpdateTime; }
            set { _UpdateTime = value; }
        }
        /// <summary>
        /// 父级Id
        /// </summary>
        public Int32? ParentId
        {
            get { return _ParentId; }
            set { _ParentId = value; }
        }
        /// <summary>
        /// 数据来源
        /// </summary>
        public String DataSource
        {
            get { return _DataSource; }
            set { _DataSource = value; }
        }
        /// <summary>
        /// 扩展nvarchar
        /// </summary>
        public String ExtraStr
        {
            get { return _ExtraStr; }
            set { _ExtraStr = value; }
        }
        /// <summary>
        /// 扩展Int
        /// </summary>
        public Int32? ExtraInt
        {
            get { return _ExtraInt; }
            set { _ExtraInt = value; }
        }
        #endregion
    }
}
