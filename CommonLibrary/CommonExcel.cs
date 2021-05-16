using DTO;
using MiniExcelLibs;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CommonLibrary
{
    public partial class CommonExcel
    {
        public static void MiniExcelExport()
        {
            var dataList = CommonHelper.GetTestListDto(1000000);
            Stopwatch stopWatch = new Stopwatch();stopWatch.Start();
            MiniExcel.SaveAs(Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid().ToString()}.xlsx"), dataList);
            stopWatch.Stop();Console.WriteLine(stopWatch.Elapsed.TotalSeconds);
        }
    }
}
