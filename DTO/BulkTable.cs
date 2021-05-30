using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DTO
{
    public class BulkTable
    {
        public DataTable Table { get; set; }
        public string TableName { get; set; }
        public string Primary { get; set; } = "Id";
        public List<string> TableFields { get; set; } = new List<string>();
        public List<string> UpdateFields { get; set; } = new List<string>();
        public List<string> RemoveFields { get; set; } = new List<string>();

    }
}
