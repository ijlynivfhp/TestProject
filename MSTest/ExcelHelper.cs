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
            var dataList = CommonHelper.GetTestListDto(500000);
            var dataListTwo = CommonHelper.GetTestListDtoTwo(500000);
            var table = SqlBulkCopyHelper.ListToTable(dataList, "BulkTest");
            var tableTwo = SqlBulkCopyHelper.ListToTable(dataListTwo, "BulkTestTwo");
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var task = SqlBulkCopyHelper.BulkInsertTables(new List<DataTable>() { table, tableTwo });
            task.Wait();
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
                item["FieldD"] = "222";
            }
            foreach (DataRow item in tableTwo.Rows)
            {
                item["FieldD"] = "333";
            }
            table.TableName = "BulkTest";
            tableTwo.TableName = "BulkTestTwo";
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var task = SqlBulkCopyHelper.BulkUpdateTables(new List<DataTable>() { table, tableTwo });
            task.Wait();
            sw.Stop();
            string bb = sw.Elapsed.TotalSeconds.ToString();
            Console.WriteLine(bb);
        }
        [TestMethod]
        public void DataEdit()
        {
            var dataList = CommonHelper.GetTestListDto(500000);
            var dataListTwo = CommonHelper.GetTestListDtoTwo(500000);
            var table = SqlBulkCopyHelper.ListToTable(dataList, "BulkTest");
            var tableTwo = SqlBulkCopyHelper.ListToTable(dataListTwo, "BulkTestTwo");

            var tableUpdate = SqlCoreHelper.ExecuteDataSetText("select top 500000 Id,FieldD from BulkTest order by FieldU asc", null).Tables[0];
            var tableTwoUpdate = SqlCoreHelper.ExecuteDataSetText("select top 500000 Id,FieldD from BulkTestTwo order by FieldU asc", null).Tables[0];
            foreach (DataRow item in tableUpdate.Rows)
            {
                item["FieldD"] = "5555";
            }
            foreach (DataRow item in tableTwoUpdate.Rows)
            {
                item["FieldD"] = "6666";
            }
            tableUpdate.TableName = "BulkTest";
            tableTwoUpdate.TableName = "BulkTestTwo";
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var task = SqlBulkCopyHelper.BulkEditTables(new List<DataTable>() { table, tableTwo }, new List<DataTable>() { tableUpdate, tableTwoUpdate });
            task.Wait();
            sw.Stop();
            string bb = sw.Elapsed.TotalSeconds.ToString();
            Console.WriteLine(bb);
        }
        [TestMethod]
        public void ListExpect()
        {
            var oldList = new List<string>() { "aaaa", "aaaa", "bbbb", "cccc", "dddd" };
            var newList = new List<string>() { "bbbb", "cccc" };
            var list = oldList.Except(newList);
        }
    }
}
