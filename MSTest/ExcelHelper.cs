using CommonLibrary;
using DTO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System;

namespace MSTest
{
    [TestClass]
    public class ExcelHelper
    {
        [TestMethod]
        public void ExcelTest()
        {
            CommonExcel.MiniExcelExport();
        }

        [TestMethod]
        public void DataInsert()
        {
            var dataList = CommonHelper.GetTestListDto(5000);
            var dataListTwo = CommonHelper.GetTestListDtoTwo(5000);
            var table = SqlBulkCopyHelper.ListToTable(dataList, "BulkTest");
            var tableTwo = SqlBulkCopyHelper.ListToTable(dataListTwo, "BulkTestTwo");
            Stopwatch sw = new Stopwatch();
            sw.Start();
            SqlBulkCopyHelper.BulkInsertTables(new List<DataTable>() { table, tableTwo }).Wait();
            sw.Stop();
            string bb = sw.Elapsed.TotalSeconds.ToString();
            Console.WriteLine(bb);
        }
        [TestMethod]
        public void DataUpdate()
        {
            var table = SqlCoreHelper.ExecuteDataSetText("select Id,FieldD from BulkTest", null).Tables[0];
            var tableTwo = SqlCoreHelper.ExecuteDataSetText("select Id,FieldD from BulkTestTwo", null).Tables[0];
            foreach (DataRow item in table.Rows)
            {
                item["FieldD"] = "111";
            }
            foreach (DataRow item in tableTwo.Rows)
            {
                item["FieldD"] = "222";
            }
            table.TableName = "BulkTest";
            tableTwo.TableName = "BulkTestTwo";
            Stopwatch sw = new Stopwatch();
            sw.Start();
            SqlBulkCopyHelper.BulkUpdateTables(new List<DataTable>() { table, tableTwo });
            sw.Stop();
            string bb = sw.Elapsed.TotalSeconds.ToString();
            Console.WriteLine(bb);
        }
        [TestMethod]
        public void DataEdit()
        {
            var dataList = CommonHelper.GetTestListDto(5000);
            var dataListTwo = CommonHelper.GetTestListDtoTwo(5000);
            var table = SqlBulkCopyHelper.ListToTable(dataList, "BulkTest");
            var tableTwo = SqlBulkCopyHelper.ListToTable(dataListTwo, "BulkTestTwo");

            var tableUpdate = SqlCoreHelper.ExecuteDataSetText("select top 5000 Id,FieldD from BulkTest order by FieldU asc", null).Tables[0];
            var tableTwoUpdate = SqlCoreHelper.ExecuteDataSetText("select top 5000 Id,FieldD from BulkTestTwo order by FieldU asc", null).Tables[0];
            foreach (DataRow item in tableUpdate.Rows)
            {
                item["FieldD"] = "555";
            }
            foreach (DataRow item in tableTwoUpdate.Rows)
            {
                item["FieldD"] = "666";
            }
            tableUpdate.TableName = "BulkTest";
            tableTwoUpdate.TableName = "BulkTestTwo";
            Stopwatch sw = new Stopwatch();
            sw.Start();
            SqlBulkCopyHelper.BulkEditTables(new List<DataTable>() { table, tableTwo }, new List<DataTable>() { tableUpdate, tableTwoUpdate });
            sw.Stop();
            string bb = sw.Elapsed.TotalSeconds.ToString();
            Console.WriteLine(bb);
        }
        [TestMethod]
        public void Test()
        {
            var str = 0 + null;
            var str1 = "" + null;
            var dataList = CommonHelper.GetTestListDto(10);
            var table = SqlBulkCopyHelper.ListToTable(dataList, "BulkTest");
            var bb = new { Id = Guid.Empty, FieldB = string.Empty, FieldU = default(DateTime?), FieldV = default(decimal?), FieldX = default(int) };
            var dd = table.ToAnonymousList(bb);
            var test = new object().GetType();
            var test1 = new System.Dynamic.ExpandoObject().GetType();
            var test2 = bb.GetType();
        }
    }
}
