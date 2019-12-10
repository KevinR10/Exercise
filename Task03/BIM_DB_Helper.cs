using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Configuration;
using System.Web;

namespace Task03
{
    public class BIM_DB_Helper
    {
        #region 类描述
        /// **********************************************************************
        /// Copyright (C), BIM
        /// 类名: BIM_DB_Helper.cs        
        /// 描述: 底层调用数据库的基类
        /// 方法列表: 
        /// /*
        /// 1.public static DataTable FExecProcForQuery(string procName, params SqlParameter[] sqlParams)
        ///   描述：执行存储过程的，参数1是存储过程名称，参数2是存储过程的参数
        ///         返回执行存储过程后得到的结果集
        /// */
        /// /*
        /// 2.public static int FExecProcForModify(string procName, params SqlParameter[] sqlParams)
        ///   描述：执行存储过程的，参数1是存储过程名称，参数2是存储过程的参数
        ///         返回执行存储过程后得到的影响行数
        /// */
        /// /// 3.public static bool Exists(string execSql)
        ///   描述：执行SQL语句的，参数执行的SQL语句
        ///         返回执行SQL语句后判断是否存在一条指定的记录
        /// */
        /// /// 4.public static int ExecuteSql(string execSql)
        ///   描述：执行SQL语句的，参数执行的SQL语句
        ///         返回执行SQL语句后得到的影响行数
        /// */
        /// /// 5. public static DataSet Query(string execSql)
        ///   描述：执行SQL语句的，参数执行的SQL语句
        ///         返回执行SQL语句后得到的结果集
        /// */
        /// 最后修改: 
        /// 修改日期: 2019/04/25
        /// 修改人: 李从宝
        /// 修改描述: 修改了对类和方法的注释
        /// 
        /// **********************************************************************
        #endregion
        #region 属性
        //public static string sqlConection = System.Configuration.ConfigurationManager.AppSettings["connString"].ToString();
        public static string sqlConection = System.Configuration.ConfigurationManager.AppSettings["connString"].ToString();
        //string sonnstr_master=System.Web.HttpContext.Current.Request.Cookies["sonnstr_master"].Value;
        //public BIM_DB_Helper(string connstr) {
        //        sqlConection = connstr;
        //}
        #endregion
        #region 影藏
        ///// <summary>
        ///// 执行SQL语句,主要用于增、删、改
        ///// </summary>
        ///// <param name="execSql">要执行的SQL语句</param>
        ///// <returns>返回执行SQL语句影响的行数</returns>
        //public static int FExecSqlForModify(string execSql)
        //{
        //    SqlConnection sqlConn = new SqlConnection(sqlConection);
        //    SqlCommand sqlCmd = new SqlCommand(execSql,sqlConn);
        //    try
        //    {
        //        sqlConn.Open();
        //        return sqlCmd.ExecuteNonQuery();
        //    }
        //    catch { return 0; }
        //    finally
        //    {
        //        sqlConn.Close();
        //        sqlConn.Dispose();
        //    }
        //}
        ///// <summary>
        ///// 执行SQL语句,主要用于查
        ///// </summary>
        ///// <param name="execSql">要执行的SQL语句</param>
        ///// <returns>返回查询得出的结果集，是一个DataTable</returns>
        //public static DataTable FExecSqlForQuery(string execSql)
        //{
        //    SqlConnection sqlConn = new SqlConnection(sqlConection);
        //    SqlCommand sqlCmd = new SqlCommand(execSql, sqlConn);
        //    DataTable queryDataSource = new DataTable();
        //    SqlDataAdapter sda = new SqlDataAdapter(sqlCmd);
        //    try
        //    {
        //        sda.Fill(queryDataSource);
        //        return queryDataSource;
        //    }
        //    catch { return null; }
        //    finally
        //    {
        //        sda.Dispose();
        //    }
        //}
        #endregion
        #region 使用的
        /// <summary>
        /// 执行存储过程,主要用于查
        /// </summary>
        /// <param name="procName">存储过程名称</param>
        /// <param name="sqlParams">存储过程需要的参数</param>
        /// <returns>返回查询出来的结果集DataTable</returns>
        public static DataTable FExecProcForQuery(string procName, params SqlParameter[] sqlParams)
        {
            SqlConnection sqlConn = new SqlConnection(sqlConection);
            SqlCommand sqlCmd = new SqlCommand();
            sqlCmd.Connection = sqlConn;
            if (sqlConn.State == ConnectionState.Closed)
            {
                sqlConn.Open();
            }
            sqlCmd.CommandText = procName;
            sqlCmd.CommandType = System.Data.CommandType.StoredProcedure;
            sqlCmd.Parameters.AddRange(sqlParams);
            SqlDataAdapter sda = new SqlDataAdapter(sqlCmd);
            DataTable dataSource = new DataTable();
            try
            {
                sda.Fill(dataSource);
                return dataSource;
            }
            catch
            {
                return null;
            }
            finally
            {
                sda.Dispose();
            }
        }
        /// <summary>
        /// 执行存储过程,主要用于增、修改、删除
        /// </summary>
        /// <param name="procName">存储过程名称</param>
        /// <param name="sqlParams">存储过程需要的参数</param>
        /// <returns>返回执行存储过程影响的行数</returns>
        public static int FExecProcForModify(string procName, params SqlParameter[] sqlParams)
        {
            SqlConnection sqlConn = new SqlConnection(sqlConection);

            SqlCommand sqlCmd = new SqlCommand();
            sqlCmd.Connection = sqlConn;
            sqlCmd.CommandText = procName;
            sqlCmd.CommandType = System.Data.CommandType.StoredProcedure;
            sqlCmd.Parameters.AddRange(sqlParams);
            try
            {
                if (sqlConn.State == ConnectionState.Closed)
                {
                    sqlConn.Open();
                }
              
                return sqlCmd.ExecuteNonQuery();
            }
            catch
            {
                return 0;
            }
            finally
            {
                sqlConn.Close();
                sqlConn.Dispose();
            }
        }
        /// <summary>
        /// 用于判断行是否存在的
        /// </summary>
        /// <param name="execSql">要判断执行的SQL语句</param>
        /// <returns>返回是否存在存在True,否则False</returns>
        public static bool FExists(string execSql)
        {
            SqlConnection sqlConn = new SqlConnection(sqlConection);
            if (sqlConn.State == ConnectionState.Closed)
            {
                sqlConn.Open();
            }
            SqlCommand sqlCmd = new SqlCommand(execSql, sqlConn);
            SqlDataAdapter sda = new SqlDataAdapter(sqlCmd);
            DataTable dt = new DataTable();
            try
            {
                sda.Fill(dt);
                return Convert.ToInt32(dt.Rows[0][0].ToString()) == 0 ? false : true;
            }
            catch { return false; }
            finally
            {
                sqlConn.Close();
                sqlConn.Dispose();
            }
        }
        /// <summary>
        /// 执行SQL语句，返回影响行数
        /// </summary>
        /// <param name="execSql">要执行的SQL语句</param>
        /// <returns>影响行数</returns>
        public static int FExecuteSql(string execSql)
        {
            SqlConnection sqlConn = new SqlConnection(sqlConection);
            SqlCommand sqlCmd = new SqlCommand(execSql, sqlConn);
            sqlCmd.CommandTimeout = 900000000;
            SqlTransaction transaction;
            if (sqlConn.State == ConnectionState.Closed)
            {
                sqlConn.Open();
            }          
            transaction = sqlConn.BeginTransaction("SampleTransaction");
            sqlCmd.Transaction = transaction;
            try
            {
                sqlCmd.ExecuteNonQuery();
                transaction.Commit();
                return 1;
            }
            catch (Exception errr)
            {
                // ExceptionManager.Raise("", "$Error_Command_Execute", ex);
                transaction.Rollback();
                return 0;
            }
            finally
            {
                sqlConn.Close();
                sqlConn.Dispose();
            }
        }
        /// <summary>
        /// 执行多条SQL语句，返回影响行数
        /// </summary>
        /// <param name="execSql">要执行的SQL语句</param>
        /// <returns>影响行数</returns>
        public static int FExecuteSql(string[] execSql)
        {
            SqlConnection sqlConn = new SqlConnection(sqlConection);
            SqlCommand sqlCmd = new SqlCommand();
            sqlCmd.CommandTimeout = 900000000;
            sqlCmd.Connection = sqlConn;
            if (sqlConn.State == ConnectionState.Closed)
            {
                sqlConn.Open();
            }
            SqlTransaction transaction;
            transaction = sqlConn.BeginTransaction("SampleTransaction");
            sqlCmd.Transaction = transaction;
            try
            {
                for (int i = 0; i < execSql.Length; i++)
                {
                    sqlCmd.CommandText = execSql[i];
                    sqlCmd.ExecuteNonQuery();
                }
                transaction.Commit();
                return 1;
            }
            catch (Exception errr)
            {
                transaction.Rollback();
                return 0;
            }
            finally
            {
                sqlConn.Close();
                sqlConn.Dispose();
            }
        }
        /// <summary>
        /// 执行SQL语句，返回结果集
        /// </summary>
        /// <param name="execSql">要执行的SQL语句</param>
        /// <returns>结果集</returns>
        public static DataSet FQuery(string execSql)
        {
            SqlConnection sqlConn = new SqlConnection(sqlConection);
            if (sqlConn.State == ConnectionState.Closed)
            {
                sqlConn.Open();
            }
            SqlCommand sqlCmd = new SqlCommand(execSql, sqlConn);
            DataSet queryDataSource = new DataSet();
            SqlDataAdapter sda = new SqlDataAdapter(sqlCmd);
            try
            {
                sda.Fill(queryDataSource);
                return queryDataSource;
            }
            catch (Exception error) { string msg = error.Message; return null; }
            finally
            {
                sda.Dispose();
            }
        }
        #endregion
        #region 头的武器
        #region 静态属性
        private static string _strSQL;//sql命令或过程名
        /// <summary>
        /// 功  能:获取sql命令或过程名
        /// </summary>
        /// <remarks></remarks>
        public static string SQL
        {
            get { return BIM_DB_Helper._strSQL; }
        }

        private static Boolean _encrypt = false;
        /// <summary>
        /// 获取或设置是否使用加密连接字符串
        /// </summary>
        public static Boolean EncryptConnectionString
        {
            get { return BIM_DB_Helper._encrypt; }
            set
            {
                BIM_DB_Helper._encrypt = value;
                if (value)
                {
                    //调用解密工具
                }
                else
                {
                    //直接获取连接串
                }
            }
        }
        //与SQL Server的连接字符串设置
        public static  string _connectionString = string.Empty;
        #endregion
        //与数据库的连接

        private SqlTransaction _tran;//执行事务对象
        /// <summary>
        /// 获取当前执行的事务对象
        /// </summary>
        public SqlTransaction Transaction
        {
            get { return _tran; }

        }

        static BIM_DB_Helper()
        {
            _connectionString = System.Configuration.ConfigurationManager.AppSettings["connString"].ToString();
            if (System.Web.HttpContext.Current.Request.Cookies["CONN_STRING"] != null && System.Web.HttpContext.Current.Request.Cookies["CONN_STRING"].Value != "")
            {
                _connectionString = HttpUtility.UrlDecode(System.Web.HttpContext.Current.Request.Cookies["CONN_STRING"].Value);
            }
        }
        /// <summary>
        /// 批量 增删改
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="_SQL">SQL语句</param>
        /// <param name="_Paras">参数</param>
        /// <returns></returns>
        public static int FExecNonQuery(DataTable dt, string _SQL, string _Paras)
        {
            SqlConnection connection = new SqlConnection(_connectionString);//create connection
            SqlCommand command = new SqlCommand(_SQL, connection);
            SqlTransaction transaction;

            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            
            transaction = connection.BeginTransaction("SampleTransaction");
            command.CommandTimeout = 900000000;
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandType = CommandType.Text;
            command.CommandText = _SQL;
            try
            {
                int i = 0;
                for (int g = 0; g < dt.Rows.Count; g++)
                {
                    //
                    object[] paraValue = new object[dt.Columns.Count];
                    for (int b = 0; b < dt.Columns.Count; b++)
                    {
                        paraValue[b] = dt.Rows[g][b].ToString();
                    }
                    //
                    command.Parameters.Clear();
                    SqlParameter[] p_ParaList = BIM_DB_Helper.FCreateParaList(_Paras, paraValue);
                    if (null != p_ParaList)//添加参数
                    {
                        foreach (SqlParameter para in p_ParaList)
                        {
                            if (null != para)
                            {
                                command.Parameters.Add(para);
                            }
                        }
                    }


                    i += command.ExecuteNonQuery();
                }
                transaction.Commit();
                connection.Close();
                return i;

            }
            catch (Exception ex)
            {
                // ExceptionManager.Raise("", "$Error_Command_Execute", ex);
                transaction.Rollback();
                connection.Close();
                throw new Exception();


            }
            finally
            {
                connection.Close();
            }

        }

        /// <summary>
        /// 执行sql
        /// </summary>
        /// <param name="p_SQL">sql语句</param>
        /// <param name="p_ParaList">参数集(null代表无参数)</param>
        /// <param name="p_Type">类型</param>
        /// <returns>返回影响行数</returns>
        public static int FExecNonQuery(string p_SQL, SqlParameter[] p_ParaList, CommandType p_Type)
        {
            BIM_DB_Helper._strSQL = p_SQL;
            SqlCommand command = BIM_DB_Helper.FCreateCommand(p_SQL, p_ParaList, p_Type);//创建sql命令对象
            command.CommandTimeout = 13600;
            System.Diagnostics.Debug.Assert(null != command);
            SqlTransaction transaction;

            try
            {
                if (command.Connection.State == ConnectionState.Closed)
                {
                    command.Connection.Open();
                }
                transaction = command.Connection.BeginTransaction("SampleTransaction");
                command.Transaction = transaction;
                try
                {

                    DateTime dtStart = DateTime.Now;
                    //log.Info("************StartDateTime:" + dtStart.ToString() + "," + dtStart.Millisecond);
                    //log.Info(" Parameter SQL:" + command.CommandText);
                    int i = command.ExecuteNonQuery();//执行命令 
                    //DateTime dtEnd = DateTime.Now;
                    //TimeSpan ts = dtEnd - dtStart;
                    //log.Info("************EndDateTime:" + dtEnd.ToString() + "," + dtEnd.Millisecond + "*********"
                    //    + "Cost: " + ts.Seconds + ":" + ts.Milliseconds);
                    transaction.Commit();
                    return i;
                }
                catch (Exception e)
                {
                    // log.Error(e.Message + " Parameter SQL:" + command.CommandText);
                    transaction.Rollback();
                    throw new Exception(e.Message);
                    //ExceptionManager.Raise(this.GetType(), "$Error_Command_Execute", e);
                }
            }
            finally
            {
                command.Connection.Close();
            }
        }
        /// <summary>
        /// 执行查询命令
        /// </summary>
        /// <param name="p_SQL">要执行的sql命令或存储过程名</param>
        /// <param name="p_ParaList">参数集(null代表无参数)</param>
        /// <returns>填充了数据的DataSet</returns>
        /// <remarks></remarks>
        /// <param name="p_Type"></param>
        public static DataSet FExecQuery(string p_SQL, SqlParameter[] p_ParaList, CommandType p_Type)
        {
            BIM_DB_Helper._strSQL = p_SQL;
            SqlCommand command = BIM_DB_Helper.FCreateCommand(p_SQL, p_ParaList, p_Type);//创建sql命令对象
            command.CommandTimeout = 900000000;
            System.Diagnostics.Debug.Assert(null != command);

            DataSet dsContain = new DataSet();
            SqlDataAdapter da = new SqlDataAdapter(command);
            System.Diagnostics.Debug.Assert(null != dsContain);
            try
            {
                DateTime dtStart = DateTime.Now;
                da.Fill(dsContain);//执行并填充
                DateTime dtEnd = DateTime.Now;
                TimeSpan ts = dtEnd - dtStart;
                return dsContain;
            }
            catch (Exception e)
            {
                // ExceptionManager.Raise(this.GetType(), "$Error_Command_Execute", e);
                throw new Exception();
            }
            finally
            {
                command.Connection.Close();
            }
        }
        #region 辅助工具
        /// <summary>
        /// 创建简单的Sql参数集(对image等需要强类型的参数不适用) 
        /// </summary>
        /// <param name="paraNames">参数名</param>
        /// <param name="paraValue">参数值</param>
        /// <returns>构建好的参数集</returns>
        /// <remarks></remarks>
        public static SqlParameter[] FCreateParaList(string paraNames, object[] paraValue)
        {
            string[] names = paraNames.Split(new char[] { '/', ',', ';' });
            if (names.Length != paraValue.Length)
            {
                throw new Exception("参数名和参数值数量不一样!");
            }
            SqlParameter[] paraList = new SqlParameter[names.Length];
            for (int index = 0; index < names.Length; index++)
            {
                paraList[index] = new SqlParameter();
                if (-1 == names[index].IndexOf('@'))
                {
                    names[index] = "@" + names[index];
                }
                paraList[index].ParameterName = names[index].Trim();
                if (null == paraValue[index] || paraValue[index].ToString().Equals(string.Empty))
                {
                    paraList[index].Value = System.DBNull.Value;
                }
                else
                {
                    paraList[index].Value = paraValue[index];
                }
            }
            return paraList;
        }

        /// <summary>
        /// 创建sql命令
        /// </summary>
        /// <param name="p_SQL">要执行的sql命令或存储过程名</param>
        /// <param name="p_ParaList">参数集(null代表无参数)</param>
        /// <returns>构建好的command</returns>
        /// <remarks> </remarks>
        /// <param name="p_Type"></param>
        public static SqlCommand FCreateCommand(string p_SQL, SqlParameter[] p_ParaList, CommandType p_Type)
        {
           
            _connectionString = System.Configuration.ConfigurationManager.AppSettings["connString"].ToString();
            if (System.Web.HttpContext.Current.Request.Cookies["CONN_STRING"] != null && System.Web.HttpContext.Current.Request.Cookies["CONN_STRING"].Value != "")
            {
                _connectionString = HttpUtility.UrlDecode(System.Web.HttpContext.Current.Request.Cookies["CONN_STRING"].Value);

            }
            SqlConnection conn = new SqlConnection(BIM_DB_Helper._connectionString);//create connection
            SqlCommand command = new SqlCommand(p_SQL, conn);//create command
            if (conn.State == ConnectionState.Closed)
            {
                conn.Open();
            }
            command.CommandType = p_Type;
            if (null != p_ParaList)//添加参数
            {
                foreach (SqlParameter para in p_ParaList)
                {
                    if (null != para)
                    {
                        command.Parameters.Add(para);
                    }
                }
            }
            return command;
        }
        /// <summary>
        /// 把整个表批量插入
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="TableName"></param>
        /// <param name="dt"></param>
        public static void SqlBulkCopyByDataTable(string TableName, DataTable dt)
        {
            using (SqlConnection sqlconn = new SqlConnection(_connectionString))
            {
                using (SqlBulkCopy sqlbulkcopy = new SqlBulkCopy(_connectionString, SqlBulkCopyOptions.UseInternalTransaction))
                {
                    try
                    {
                        sqlbulkcopy.DestinationTableName = TableName;
                        sqlbulkcopy.WriteToServer(dt);
                    }
                    catch (System.Exception ex)
                    {
                        throw ex;
                    }
                }
            }
        }

        #endregion
        #endregion
    }
}
