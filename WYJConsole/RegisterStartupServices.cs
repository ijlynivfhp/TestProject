using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WYJConsole
{
    public static class RegisterStartupServices
    {
        public static WebApplicationBuilder RegisterServices(this buil builder)
        {
            builder.Services.AddControllers();
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();
            return builder;
        }
    }
}
