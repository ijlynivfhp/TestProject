using DTO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommonLibrary
{
    public partial class CommonHelper
    {
        /// <summary>
        /// 获取测试数据DataTable
        /// </summary>
        /// <param name="count"></param>
        /// <returns></returns>
        public static DataTable GetTestDataTable(int count=10000) {
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
            o.SetValue(dto, o.Name + sufStr, null);
        });
        return dto;
    }
    public static readonly Random rand = new Random();
    public static readonly List<string> ListEnStr = new List<string>() { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };
}
}
