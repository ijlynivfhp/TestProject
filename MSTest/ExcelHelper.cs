using CommonLibrary;
using DTO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;

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
            var dataList = CommonHelper.GetTestListDto(100000);
            var dataListTwo = CommonHelper.GetTestListDtoTwo(100000);
            var table = SqlBulkCopyHelper.ListToTable(dataList, "BulkTest");
            var tableTwo = SqlBulkCopyHelper.ListToTable(dataListTwo, "BulkTestTwo");
            Stopwatch sw = new Stopwatch();
            sw.Start();
            SqlBulkCopyHelper.BulkInsertTables(new List<DataTable>() { table, tableTwo }).Wait();
            sw.Stop();
            string bb = sw.Elapsed.TotalSeconds.ToString();
        }
        [TestMethod]
        public void DataUpdate()
        {
            var table = SqlCoreHelper.ExecuteDataSetText("select * from BulkTest", null).Tables[0];
            var tableTwo = SqlCoreHelper.ExecuteDataSetText("select * from BulkTestTwo", null).Tables[0];
            foreach (DataRow item in table.Rows)
            {
                item["FieldD"] = "1111";
            }
            foreach (DataRow item in tableTwo.Rows)
            {
                item["FieldD"] = "2222";
            }
            table.TableName = "BulkTest";
            tableTwo.TableName = "BulkTestTwo";
            Stopwatch sw = new Stopwatch();
            sw.Start();
            SqlBulkCopyHelper.BulkUpdateTables(new List<BulkTable>() { new BulkTable() { Table = table }, new BulkTable() { Table = tableTwo } });
            sw.Stop();
            string bb = sw.Elapsed.TotalSeconds.ToString();
        }
        [TestMethod]
        public void DataEdit()
        {
            var dataList = CommonHelper.GetTestListDto(100000);
            var dataListTwo = CommonHelper.GetTestListDtoTwo(100000);
            var table = SqlBulkCopyHelper.ListToTable(dataList, "BulkTest");
            var tableTwo = SqlBulkCopyHelper.ListToTable(dataListTwo, "BulkTestTwo");

            var tableUpdate = SqlCoreHelper.ExecuteDataSetText("select top 10000 * from BulkTest order by FieldU asc", null).Tables[0];
            var tableTwoUpdate = SqlCoreHelper.ExecuteDataSetText("select top 10000 * from BulkTestTwo order by FieldU asc", null).Tables[0];
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
            SqlBulkCopyHelper.BulkEditTables(new List<DataTable>() { table, tableTwo }, new List<BulkTable>() { new BulkTable() { Table = tableUpdate }, new BulkTable() { Table = tableTwoUpdate } });
            sw.Stop();
            string bb = sw.Elapsed.TotalSeconds.ToString();
        }
    }
}
