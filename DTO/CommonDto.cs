using System;
using System.Collections.Generic;
using System.Linq;
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
}
