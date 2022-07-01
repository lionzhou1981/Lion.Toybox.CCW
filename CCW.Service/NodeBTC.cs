using System;
using System.Collections.Concurrent;
using System.Data;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using MySql.Data.MySqlClient;
using Lion;
using Lion.CryptoCurrency.Bitcoin;
using Lion.Data;
using Lion.Data.MySqlClient;
using Lion.Encrypt;
using Lion.Net;
using CCW.Core;

namespace CCW.Service
{
    internal class NodeBTC
    {
        private static string Chain = "BTC";
        private static bool Mainnet = false;
        private static string Secret = "";
        private static string Host = "";
        private static string User = "";
        private static string Pass = "";
        private static int Confirm = 999;
        private static string MergeAddress = "";

        private static ConcurrentQueue<string> ListTxQueue = new ConcurrentQueue<string>();
        private static ConcurrentQueue<JArray> ListRxQueue = new ConcurrentQueue<JArray>();

        private static Thread ThreadAddress;
        private static Thread ThreadList;
        private static Thread ThreadCheck;
        private static Thread ThreadSend;
        private static Thread ThreadSendRaw;
        private static Thread[] ThreadsListDetail;

        public static void Init()
        {
            Mainnet = Common.Settings["NodeBTC"]["Mode"].Value<string>() == "MAINNET";
            Secret = Common.Settings["NodeBTC"]["Secret"].Value<string>();
            Host = Common.Settings["NodeBTC"]["Host"].Value<string>();
            User = Common.Settings["NodeBTC"]["User"].Value<string>();
            Pass = Common.Settings["NodeBTC"]["Pass"].Value<string>();
            Confirm = Common.Settings["NodeBTC"]["Confirm"].Value<int>();
            MergeAddress = Common.Settings["NodeBTC"]["MergeAddress"].Value<string>();

            ThreadsListDetail = new Thread[10];
            for (int i = 0; i < ThreadsListDetail.Length; i++)
            {
                ThreadsListDetail[i] = new Thread(new ParameterizedThreadStart(ListTransactionsGet));
                ThreadsListDetail[i].Start(i);
            }

            ThreadAddress = new Thread(new ThreadStart(GenerateAddress));
            ThreadAddress.Start();

            ThreadList = new Thread(new ThreadStart(ListTransactions));
            ThreadList.Start();

            ThreadCheck = new Thread(new ThreadStart(CheckTransactions));
            ThreadCheck.Start();

            ThreadSend = new Thread(new ThreadStart(SendTransactions));
            ThreadSend.Start();

            ThreadSendRaw = new Thread(new ThreadStart(SendRawTransactions));
            ThreadSendRaw.Start();
        }

        #region GenerateAddress
        private static void GenerateAddress()
        {
            DateTime _last = DateTime.UtcNow;
            while (Common.Running)
            {
                DateTime _now = DateTime.UtcNow;
                if ((_now - _last).TotalMinutes < 1) { Thread.Sleep(100); continue; }
                _last = _now;

                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_address");
                        _tsql.Fields.Add("COUNT(*)");
                        _tsql.Wheres.And("chain", "=", Chain);
                        _tsql.Wheres.And("status", "=", 0);

                        long _count = (long)_db.GetDataScalar(_tsql.ToSqlCommand());
                        if (_count > 50) { continue; }

                        for (int i = 0; i < 100; i++)
                        {
                            Address _address = Address.GetMultisignAddress(_mainnet: Mainnet);

                            string _source = OpenSSLAes.Encode($"{_address.Private}", Secret);

                            _tsql = new TSQL(TSQLType.Insert, "wallet_address");
                            _tsql.Fields.Add("chain", "", Chain);
                            _tsql.Fields.Add("address", "", _address.Text);
                            _tsql.Fields.Add("source", "", _source);
                            _tsql.Fields.Add("create_at", "", _now);
                            _tsql.Fields.Add("status", "", 0);
                            _db.Execute(_tsql.ToSqlCommand());

                            Common.Log("GenerateAddress", $"{Chain},{_address.Text},{_source}");
                        }
                    }
                }
                catch (Exception _ex)
                {
                    Common.Log("GenerateAddress", _ex.ToString(), LogLevel.ERROR);
                }
            }
        }
        #endregion

        #region ListTransactions
        private static void ListTransactions()
        {
            DateTime _last = DateTime.UtcNow;
            while (Common.Running)
            {
                DateTime _now = DateTime.UtcNow;
                if ((_now - _last).TotalMinutes < 1) { Thread.Sleep(100); continue; }
                _last = _now;

                long _local = -1;
                long _block = -1;
                long _height = -1;

                #region Step 0: 获取补扫区块
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_block");
                        _tsql.Limit = 1;
                        _tsql.Wheres.And("chain", "=", Chain);
                        _tsql.Wheres.And("status", "=", 0);
                        _tsql.Orders.AscField("id");
                        DataTable _table = _db.GetDataTable(_tsql.ToSqlCommand());
                        if (_table.Rows.Count == 1)
                        {
                            _block = (long)_table.Rows[0]["block"];
                            Common.Log("ListTransactions", $"Step 0: {_block}");
                        }
                    }
                }
                catch (Exception _ex)
                {
                    _block = -1;
                    Common.Log("ListTransactions", $"Step 0: {_ex}", LogLevel.ERROR);
                }
                #endregion

                #region Step 1: 获取本地区块
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_chain");
                        _tsql.Fields.Add("block");
                        _tsql.Wheres.And("code", "=", Chain);
                        _local = (long)_db.GetDataScalar(_tsql.ToSqlCommand());
                        Common.Log("ListTransactions", $"Step 1: {_local}");
                    }
                }
                catch (Exception _ex)
                {
                    _local = -1;
                    Common.Log("ListTransactions", $"Step 1: {_ex}", LogLevel.ERROR);
                }
                #endregion

                #region Step 2: 获取节点区块
                if (_block == -1)
                {
                    try
                    {
                        JObject _result = new JObject
                        {
                            ["jsonrpc"] = "1.0",
                            ["id"] = "1",
                            ["method"] = "getblockcount",
                            ["params"] = new JArray()
                        };
                        _result = Call(_result);
                        _height = _result["result"].Value<int>();
                        Common.Log("ListTransactions", $"Step 2: {_height}");
                    }
                    catch (Exception _ex)
                    {
                        _height = -1;
                        Common.Log("ListTransactions", $"Step 2: {_ex}", LogLevel.ERROR);
                    }
                    if (_local == -1 || _height == -1) { Common.Log("ListTransactions", $"Block count failed. {_local}/{_height}", LogLevel.ERROR); continue; }
                    if (_local >= _height) { continue; }

                    _block = _local + 1;
                }
                #endregion

                #region Step 3: 获取区块HASH
                string _blockHash = "";
                try
                {
                    JObject _result = new JObject
                    {
                        ["jsonrpc"] = "1.0",
                        ["id"] = "1",
                        ["method"] = "getblockhash",
                        ["params"] = new JArray(_block)
                    };
                    _result = Call(_result);
                    _blockHash = _result["result"].Value<string>();
                    Common.Log("ListTransactions", $"Step 3: {_blockHash}");
                }
                catch (Exception _ex)
                {
                    Common.Log("ListTransactions", $"Step 3: {_ex}", LogLevel.ERROR);
                    continue;
                }
                #endregion

                #region Step 4: 获取区块数据
                JArray _txIds;
                string _blockAt;
                try
                {
                    JObject _result = new JObject
                    {
                        ["jsonrpc"] = "1.0",
                        ["id"] = "1",
                        ["method"] = "getblock",
                        ["params"] = new JArray(_blockHash)
                    };
                    _result = Call(_result);
                    _result = (JObject)_result["result"];
                    _blockAt = DateTimePlus.JSTime2DateTime(_result["time"].Value<long>()).ToString("yyyy-MM-dd HH:mm:ss");
                    _txIds = (JArray)_result["tx"];
                    Common.Log("ListTransactions", $"Step 4: {_txIds.Count}");
                }
                catch (Exception _ex)
                {
                    Common.Log("ListTransactions", $"Step 4: {_ex}", LogLevel.ERROR);
                    return;
                }
                #endregion

                #region Step 5: 逐个获取详情
                try
                {
                    for (int i = 0; i < _txIds.Count; i++)
                    {
                        string _txid = _txIds[i].Value<string>();
                        ListTxQueue.Enqueue(_txid);
                    }
                    int _null = 0, _find = 0, _save = 0;
                    Common.Log("ListTransactions", $"Step 5: {Chain} {_block} {_txIds.Count}");

                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();
                        int _txCount = 0;
                        TSQL _tsql;
                        while (_txCount < _txIds.Count)
                        {
                            Thread.Sleep(10);

                            if (ListRxQueue.Count == 0) { continue; }
                            if (!ListRxQueue.TryDequeue(out JArray _txs)) { continue; }
                            _txCount++;

                            if (_txCount % 100 == 0) { Common.Log("ListTransactions", $"Step 5: {Chain} {_block} {_txCount}/{_txIds.Count}"); }

                            if (_txs == null) { _null++; continue; }

                            #region 保存交易
                            foreach (JObject _tx in _txs)
                            {
                                string _txid = _tx["txid"].Value<string>().ToLower();
                                int _txindex = _tx["txindex"].Value<int>();
                                string _address = _tx["address"].Value<string>();
                                decimal _amount = decimal.Parse(_tx["amount"].Value<string>());

                                _tsql = new TSQL(TSQLType.Select, "wallet_address");
                                _tsql.Fields.Add("member_id");
                                _tsql.Wheres.And("address", "=", _address);
                                _tsql.Wheres.And("chain", "=", Chain);
                                DataTable _table = _db.GetDataTable(_tsql.ToSqlCommand());
                                if (_table.Rows.Count == 0) { continue; }

                                _find++;
                                string _member_id = _table.Rows[0]["member_id"].ToString();

                                _tsql = new TSQL(TSQLType.Select, "wallet_transaction");
                                _tsql.Fields.Add("COUNT(*)");
                                _tsql.Wheres.And("txid", "=", _txid);
                                _tsql.Wheres.And("txindex", "=", _txindex);
                                _tsql.Wheres.And("chain", "=", Chain);
                                long _count = (long)_db.GetDataScalar(_tsql.ToSqlCommand());
                                if (_count != 0) { Common.Log("ListTransactions", $"Step 5: {Chain} {_txid} {_txindex} exist", LogLevel.WARN); continue; };

                                _tsql = new TSQL(TSQLType.Insert, "wallet_transaction");
                                _tsql.Fields.Add("chain", "", Chain);
                                _tsql.Fields.Add("token", "", "");
                                _tsql.Fields.Add("txid", "", _txid);
                                _tsql.Fields.Add("txindex", "", _txindex);
                                _tsql.Fields.Add("tx_at", "", _blockAt);
                                _tsql.Fields.Add("block", "", _block);
                                _tsql.Fields.Add("address", "", _address);
                                _tsql.Fields.Add("member_id", "", _member_id);
                                _tsql.Fields.Add("amount", "", _amount);
                                _tsql.Fields.Add("status", "", 0);
                                _tsql.Fields.Add("list_at", "", DateTime.UtcNow);
                                _db.Execute(_tsql.ToSqlCommand());
                                _save++;
                                Common.Log("ListTransactions", $"Step 5: {Chain} {_txid} {_txindex} {_address} {_amount}");
                            }
                            #endregion
                        }

                        #region 保存区块
                        _tsql = new TSQL(TSQLType.Select, "wallet_block");
                        _tsql.Wheres.And("block", "=", _block);
                        _tsql.Wheres.And("chain", "=", Chain);
                        DataTable _tableBlock = _db.GetDataTable(_tsql.ToSqlCommand());
                        if (_tableBlock.Rows.Count == 0)
                        {
                            _tsql = new TSQL(TSQLType.Insert, "wallet_block");
                            _tsql.Fields.Add("chain", "", Chain);
                            _tsql.Fields.Add("block", "", _block);
                            _tsql.Fields.Add("block_at", "", _blockAt);
                            _tsql.Fields.Add("hash", "", _blockHash.ToLower());
                            _tsql.Fields.Add("tx", "", _txIds.Count);
                            _tsql.Fields.Add("null", "", _null);
                            _tsql.Fields.Add("find", "", _find);
                            _tsql.Fields.Add("save", "", _save);
                            _tsql.Fields.Add("status", "", 1);
                            _db.Execute(_tsql.ToSqlCommand());
                        }
                        else
                        {
                            _tsql = new TSQL(TSQLType.Update, "wallet_block");
                            _tsql.Fields.Add("tx", "", _txIds.Count);
                            _tsql.Fields.Add("null", "", _null);
                            _tsql.Fields.Add("find", "", _find);
                            _tsql.Fields.Add("save", "", _save);
                            _tsql.Fields.Add("status", "", 1);
                            _tsql.Wheres.And("id", "=", _tableBlock.Rows[0]["id"]);
                            _db.Execute(_tsql.ToSqlCommand());
                        }
                        #endregion

                        #region 保存高度
                        if (_height != -1)
                        {
                            _tsql = new TSQL(TSQLType.Update, "wallet_chain");
                            _tsql.Fields.Add("block", "", _block);
                            _tsql.Wheres.And("code", "=", Chain);
                            _db.Execute(_tsql.ToSqlCommand());
                        }
                        #endregion
                        Common.Log("ListTransactions", $"Step 5: {Chain} {_block}/{_height} {_txCount}/{_txIds.Count}");
                    }
                }
                catch (Exception _ex)
                {
                    Common.Log("ListTransactions", $"Step 5: {_ex}", LogLevel.ERROR);
                    return;
                }
                #endregion

                if (_height == -1 || _height > _block) { _last = _last.AddDays(-1); }
            }
        }
        #endregion

        #region CheckTransactions
        private static void CheckTransactions()
        {
            DateTime _last = DateTime.UtcNow;
            while (Common.Running)
            {
                DateTime _now = DateTime.UtcNow;
                if ((_now - _last).TotalMinutes < 1) { Thread.Sleep(100); continue; }
                _last = _now;

                #region Step 1: 获取入账交易
                DataTable _transactionList = null;
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_transaction");
                        _tsql.Wheres.And("status", "=", 0);
                        _tsql.Wheres.And("chain", "=", Chain);
                        _transactionList = _db.GetDataTable(_tsql.ToSqlCommand());
                        Common.Log("CheckTransactions", $"Step 1: {_transactionList.Rows.Count}");
                    }
                }
                catch (Exception _ex)
                {
                    _transactionList = null;
                    Common.Log("CheckTransactions", $"Step 1: {_ex}", LogLevel.ERROR);
                }
                #endregion

                #region Step 2: 检查入账交易
                if (_transactionList != null)
                {
                    try
                    {
                        using (DbCommon _db = Common.DbCommonMain)
                        {
                            _db.Open();
                            foreach (DataRow _row in _transactionList.Rows)
                            {
                                string _txid = _row["txid"].ToString();
                                JArray _txs = GetTransaction(_txid, out string _blockhash, out long _blocktime, out int _confirmations);

                                int _confirm = -1;
                                foreach (JObject _tx in _txs)
                                {
                                    if (_tx["txindex"].Value<string>() != _row["txindex"].ToString()) { continue; }
                                    if (_tx["address"].Value<string>() != _row["address"].ToString()) { _confirm = -1; break; }
                                    if (_tx["amount"].Value<decimal>() != (decimal)_row["amount"]) { _confirm = -1; break; }

                                    _confirm = _confirmations;
                                    break;
                                }

                                TSQL _tsql = new TSQL(TSQLType.Update, "wallet_transaction");
                                if (_confirm > Confirm)
                                {
                                    _tsql.Fields.Add("status", "", 1);
                                    _tsql.Fields.Add("confirm_at", "", DateTime.UtcNow);
                                }
                                else if (_confirm == -1)
                                {
                                    _tsql.Fields.Add("status", "", -1);
                                }
                                _tsql.Fields.Add("confirm", "", _confirm);
                                _tsql.Wheres.And("id", "=", _row["id"]);
                                _db.Execute(_tsql.ToSqlCommand());
                                Common.Log("CheckTransactions", $"Step 2: {_txid} {_confirm}");
                            }
                        }
                    }
                    catch (Exception _ex)
                    {
                        Common.Log("CheckTransactions", $"Step 2: {_ex}", LogLevel.ERROR);
                    }
                }
                #endregion

                #region Step 3: 获取出账交易
                DataTable _withdrawList = null;
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_withdraw");
                        _tsql.Wheres.And("status", "=", 2);
                        _tsql.Wheres.And("tx_at", "<", DateTime.UtcNow.AddMinutes(-10));
                        _tsql.Wheres.And("chain", "=", Chain);
                        _withdrawList = _db.GetDataTable(_tsql.ToSqlCommand());
                        Common.Log("CheckTransactions", $"Step 3: {_withdrawList.Rows.Count}");
                    }
                }
                catch (Exception _ex)
                {
                    _withdrawList = null;
                    Common.Log("CheckTransactions", $"Step 3: {_ex}", LogLevel.ERROR);
                }
                #endregion

                #region Step 4: 检查出账交易
                if (_withdrawList != null)
                {
                    try
                    {
                        using (DbCommon _db = Common.DbCommonMain)
                        {
                            _db.Open();
                            foreach (DataRow _row in _withdrawList.Rows)
                            {
                                string _txid = _row["txid"].ToString();
                                JArray _txs = GetTransaction(_txid, out string _blockhash,out long _blocktime, out int _confirm);
                                if (_confirm <= (int)_row["confirm"]) { continue; }

                                TSQL _tsql = new TSQL(TSQLType.Update, "wallet_block");
                                _tsql.Wheres.And("hash", "=", _blockhash);
                                _tsql.Wheres.And("chain", "=", Chain);
                                DataTable _table = _db.GetDataTable(_tsql.ToSqlCommand());
                                if (_table.Rows.Count == 0) { continue; }

                                string _block = _table.Rows[0]["block"].ToString();

                                _tsql = new TSQL(TSQLType.Update, "wallet_withdraw");
                                _tsql.Fields.Add("block", "", _block);
                                _tsql.Fields.Add("block_at", "", DateTimePlus.JSTime2DateTime(_blocktime));
                                _tsql.Fields.Add("confirm", "", _confirm);
                                _tsql.Fields.Add("confirm_at", "", DateTime.UtcNow);
                                if (_confirm > Confirm) { _tsql.Fields.Add("status", "", 5); }
                                _tsql.Wheres.And("id", "=", _row["id"]);
                                _db.Execute(_tsql.ToSqlCommand());
                                Common.Log("CheckTransactions", $"Step 4: {_txid} {_confirm}");
                            }
                        }
                    }
                    catch (Exception _ex)
                    {
                        Common.Log("CheckTransactions", $"Step 4: {_ex}", LogLevel.ERROR);
                    }
                }
                #endregion

                #region Step 5: 获取RAW 交易
                DataTable _rawList = null;
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        TSQL _tsql = new TSQL(TSQLType.Select, "wallet_sendraw");
                        _tsql.Wheres.And("status", "=", 2);
                        _tsql.Wheres.And("tx_at", "<", DateTime.UtcNow.AddMinutes(-10));
                        _tsql.Wheres.And("chain", "=", Chain);
                        _rawList = _db.GetDataTable(_tsql.ToSqlCommand());
                        Common.Log("CheckTransactions", $"Step 5: {_rawList.Rows.Count}");
                    }
                }
                catch (Exception _ex)
                {
                    _withdrawList = null;
                    Common.Log("CheckTransactions", $"Step 5: {_ex}", LogLevel.ERROR);
                }
                #endregion

                #region Step 6: 检查RAW 交易
                if (_rawList != null)
                {
                    try
                    {
                        using (DbCommon _db = Common.DbCommonMain)
                        {
                            _db.Open();
                            foreach (DataRow _row in _rawList.Rows)
                            {
                                string _txid = _row["txid"].ToString();
                                JArray _txs = GetTransaction(_txid, out string _blockhash, out long _blocktime, out int _confirm);
                                if (_confirm <= (int)_row["confirm"]) { continue; }

                                TSQL _tsql = new TSQL(TSQLType.Update, "wallet_block");
                                _tsql.Wheres.And("hash", "=", _blockhash);
                                _tsql.Wheres.And("chain", "=", Chain);
                                DataTable _table = _db.GetDataTable(_tsql.ToSqlCommand());
                                if (_table.Rows.Count == 0) { continue; }

                                string _block = _table.Rows[0]["block"].ToString();

                                _tsql = new TSQL(TSQLType.Update, "wallet_sendraw");
                                _tsql.Fields.Add("block", "", _block);
                                _tsql.Fields.Add("block_at", "", DateTimePlus.JSTime2DateTime(_blocktime));
                                _tsql.Fields.Add("confirm", "", _confirm);
                                _tsql.Fields.Add("confirm_at", "", DateTime.UtcNow);
                                if (_confirm > Confirm) { _tsql.Fields.Add("status", "", 5); }
                                _tsql.Wheres.And("id", "=", _row["id"]);
                                _db.Execute(_tsql.ToSqlCommand());
                                Common.Log("CheckTransactions", $"Step 4: {_txid} {_confirm}");
                            }
                        }
                    }
                    catch (Exception _ex)
                    {
                        Common.Log("CheckTransactions", $"Step 4: {_ex}", LogLevel.ERROR);
                    }
                }
                #endregion
            }
        }
        #endregion

        #region SendTransactions
        private static void SendTransactions()
        {
            DateTime _last = DateTime.UtcNow;
            while (Common.Running)
            {
                DateTime _now = DateTime.UtcNow;
                if ((_now - _last).TotalMinutes < 1) { Thread.Sleep(100); continue; }
                _last = _now;

                #region Step 1: 获取出账交易
                TSQL _tsql;
                DataTable _withdrawList = null;
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        _tsql = new TSQL(TSQLType.Select, "wallet_withdraw");
                        _tsql.Wheres.And("status", "=", 0);
                        _tsql.Wheres.And("chain", "=", Chain);
                        _withdrawList = _db.GetDataTable(_tsql.ToSqlCommand());
                        Common.Log("SendTransactions", $"Step 1: {_withdrawList.Rows.Count}");
                    }
                }
                catch (Exception _ex)
                {
                    _withdrawList = null;
                    Common.Log("SendTransactions", $"Step 1: {_ex}", LogLevel.ERROR);
                }
                if (_withdrawList == null || _withdrawList.Rows.Count == 0) { continue; }
                #endregion

                #region Step 2: 计算出账总额
                decimal _amount = 0M;
                try
                {
                    foreach (DataRow _row in _withdrawList.Rows)
                    {
                        _amount += (decimal)_row["Amount"];
                    }
                    _amount += 0.01M;
                    Common.Log("SendTransactions", $"Step 2: {_amount}");
                }
                catch (Exception _ex)
                {
                    Common.Log("SendTransactions", $"Step 2: {_ex}", LogLevel.ERROR);
                    continue;
                }
                #endregion

                #region Step 3: 选择出账交易
                decimal _amountLeft = _amount;
                decimal _amountTotal = 0M;
                JArray _spents = new JArray();
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        _tsql = new TSQL(TSQLType.Select, "wallet_transaction");
                        _tsql.Wheres.And("chain", "=", Chain);
                        _tsql.Wheres.And("spent", "=", 0);
                        _tsql.Wheres.And("confirm", ">", 0);
                        _tsql.Orders.Add("id", OrderMode.Asc);
                        MySqlDataReader _reader = (MySqlDataReader)_db.GetDataReader(_tsql.ToSqlCommand());
                        while (_reader.Read() && _amountLeft > 0)
                        {
                            _spents.Add(new JArray(
                                _reader["id"].ToString(),
                                _reader["txid"].ToString(),
                                _reader["txindex"].ToString(),
                                _reader["address"].ToString(),
                                _reader["amount"].ToString(),
                                ""));
                            _amountTotal += (decimal)_reader["amount"];
                            _amountLeft -= (decimal)_reader["amount"];
                        }
                        _reader.Close();
                    }
                    if (_amountLeft > 0) { Common.Log("SendTransactions", $"Step 3: Not enough amount. {_amountTotal}/{_amount}"); continue; }
                    Common.Log("SendTransactions", $"Step 3: {_spents.Count}");
                }
                catch (Exception _ex)
                {
                    Common.Log("SendTransactions", $"Step 3: {_ex}", LogLevel.ERROR);
                    continue;
                }
                #endregion

                #region Step 4: 附加地址详情
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        for (int i = 0; i < _spents.Count; i++)
                        {
                            _tsql = new TSQL(TSQLType.Select, "wallet_address");
                            _tsql.Fields.Add("source");
                            _tsql.Wheres.And("address", "=", _spents[i][3]);
                            _tsql.Wheres.And("chain", "=", Chain);
                            DataTable _table = _db.GetDataTable(_tsql.ToSqlCommand());

                            if (_table.Rows.Count != 1) { throw new Exception($"Address not found. {_spents[i][3]}"); }
                            _spents[i][5] = OpenSSLAes.Decode(_table.Rows[0]["source"].ToString(), Secret);
                        }
                    }
                    Common.Log("SendTransactions", $"Step 4: {_spents.Count}");
                }
                catch (Exception _ex)
                {
                    Common.Log("SendTransactions", $"Step 4: {_ex}", LogLevel.ERROR);
                    continue;
                }
                #endregion

                #region Step 5: 出账交易锁定
                using (DbCommon _db = Common.DbCommonMain)
                {
                    try
                    {
                        _db.Open();
                        _db.BeginTransaction();
                        foreach (DataRow _row in _withdrawList.Rows)
                        {
                            _tsql = new TSQL(TSQLType.Update, "wallet_withdraw");
                            _tsql.Fields.Add("status", "", 1);
                            _tsql.Wheres.And("id", "=", _row["id"]);
                            _tsql.Wheres.And("status", "=", 0);
                            _tsql.Wheres.And("chain", "=", Chain);
                            if (_db.Execute(_tsql.ToSqlCommand()) != 1) { throw new Exception($"Update withdraw failed. ({_row["id"]})"); }
                        }

                        for (int i = 0; i < _spents.Count; i++)
                        {
                            _tsql = new TSQL(TSQLType.Update, "wallet_transaction");
                            _tsql.Fields.Add("spent", "", "1");
                            _tsql.Wheres.And("id", "=", _spents[i][0]);
                            _tsql.Wheres.And("spent", "=", 0);
                            if (_db.Execute(_tsql.ToSqlCommand()) != 1) { throw new Exception($"Update spent failed. ({_spents[i][0]})"); }
                        }
                        _db.CommitTransaction();
                        Common.Log("SendTransactions", $"Step 5: {_spents.Count}");
                    }
                    catch (Exception _ex)
                    {
                        _db.RollbackTransaction();
                        Common.Log("SendTransactions", $"Step 5: {_ex}", LogLevel.ERROR);
                        continue;
                    }
                }
                #endregion

                #region Step 6: 组合出账交易
                Transaction _transaction = new Transaction();
                foreach (DataRow _row in _withdrawList.Rows)
                {
                    _transaction.Vins.Add(new TransactionVin(_row["address"].ToString(), (decimal)_row["amount"]));
                }
                foreach (JArray _spent in _spents)
                {
                    _transaction.Vouts.Add(new TransactionVout(_spent[1].Value<string>(), _spent[2].Value<int>(), _spent[4].Value<decimal>(), Private.FromHex(_spent[3].Value<string>(), _spent[5].Value<string>())));
                }
                Common.Log("SendTransactions", $"Step 5: {_transaction.Vouts.Count} {_transaction.Vins.Count}");
                #endregion

                #region Step 7: 估算出账费用
                decimal _estimatefee = 0M;
                JObject _toValue = new JObject();
                try
                {
                    JObject _result = new JObject
                    {
                        ["jsonrpc"] = "1.0",
                        ["id"] = "1",
                        ["method"] = "estimatesmartfee",
                        ["params"] = new JArray(10)
                    };
                    _result = Call(_result);
                    _result = _result["result"].Value<JObject>();

                    if (!_result.ContainsKey("errors"))
                    {
                        _estimatefee = _result["feerate"].Value<decimal>();
                    }
                    else
                    {
                        _estimatefee = 0.00002M;
                    }

                    decimal _leftAmount = _amountTotal - _transaction.Vins.Sum(t => t.Amount);
                    decimal _fee = _transaction.EstimateFee(_estimatefee, _leftAmount > 0M);

                    _transaction.Vins.Add(new TransactionVin(MergeAddress, _leftAmount - _fee));

                    Common.Log("SendTransactions", $"Step 7: {_estimatefee} {_amountTotal} {_transaction.Vins.Sum(t => t.Amount)} {_transaction.Vouts.Count}/{_transaction.Vins.Count} {_fee}");
                }
                catch (Exception _ex)
                {
                    Common.Log("SendTransactions", $"Step 7: {_ex}", LogLevel.ERROR);
                    continue;
                }
                #endregion

                #region Step 8: 组合发送交易
                string _txid = "";
                try
                {
                    JObject _result = new JObject
                    {
                        ["jsonrpc"] = "1.0",
                        ["id"] = "1",
                        ["method"] = "sendrawtransaction",
                        ["params"] = new JArray(_transaction.ToSignedHex())
                    };
                    _result = Call(_result);
                    if (_result["error"].Type != JTokenType.Null) { throw new Exception(_result["error"]["message"].Value<string>()); }
                    _txid = _result["result"].Value<string>();

                    Common.Log("SendTransactions", $"Step 8: {_txid}");
                }
                catch (Exception _ex)
                {
                    Common.Log("SendTransactions", $"Step 8: {_ex}", LogLevel.ERROR);
                    continue;
                }
                #endregion

                #region Step 9: 出账完成更新
                using (DbCommon _db = Common.DbCommonMain)
                {
                    try
                    {
                        _db.Open();
                        _db.BeginTransaction();
                        int _index = 0;
                        foreach (DataRow _row in _withdrawList.Rows)
                        {
                            _tsql = new TSQL(TSQLType.Update, "wallet_withdraw");
                            _tsql.Fields.Add("status", "", 2);
                            _tsql.Fields.Add("txid", "", _txid);
                            _tsql.Fields.Add("txindex", "", _index);
                            _tsql.Fields.Add("tx_at", "", DateTime.UtcNow);
                            _tsql.Wheres.And("id", "=", _row["id"]);
                            _tsql.Wheres.And("status", "=", 1);
                            _tsql.Wheres.And("chain", "=", Chain);
                            if (_db.Execute(_tsql.ToSqlCommand()) != 1) { throw new Exception($"Update withdraw failed. ({_row["id"]})"); }
                            _index++;
                        }

                        for (int i = 0; i < _spents.Count; i++)
                        {
                            _tsql = new TSQL(TSQLType.Update, "wallet_transaction");
                            _tsql.Fields.Add("spent", "", "2");
                            _tsql.Fields.Add("spent_txid", "", _txid);
                            _tsql.Fields.Add("spent_address", "", "");
                            _tsql.Fields.Add("spent_at", "", DateTime.UtcNow);
                            _tsql.Wheres.And("id", "=", _spents[i][0]);
                            _tsql.Wheres.And("spent", "=", 1);
                            if (_db.Execute(_tsql.ToSqlCommand()) != 1) { throw new Exception($"Update spent failed. ({_spents[i][0]})"); }
                        }
                        _db.CommitTransaction();
                        Common.Log("SendTransactions", $"Step 9: {_spents.Count}");
                    }
                    catch (Exception _ex)
                    {
                        _db.RollbackTransaction();
                        Common.Log("SendTransactions", $"Step 9: {_ex}", LogLevel.ERROR);
                        continue;
                    }
                }
                #endregion
            }
        }
        #endregion

        #region SendRawTransactions
        private static void SendRawTransactions()
        {
            DateTime _last = DateTime.UtcNow;
            while (Common.Running)
            {
                DateTime _now = DateTime.UtcNow;
                if ((_now - _last).TotalMinutes < 1) { Thread.Sleep(100); continue; }
                _last = _now;

                #region Step 1: 获取出账交易
                TSQL _tsql;
                DataTable _rawList = null;
                try
                {
                    using (DbCommon _db = Common.DbCommonMain)
                    {
                        _db.Open();

                        _tsql = new TSQL(TSQLType.Select, "wallet_sendraw");
                        _tsql.Wheres.And("status", "=", 0);
                        _tsql.Wheres.And("chain", "=", Chain);
                        _rawList = _db.GetDataTable(_tsql.ToSqlCommand());
                        Common.Log("SendRawTransactions", $"Step 1: {_rawList.Rows.Count}");
                    }
                }
                catch (Exception _ex)
                {
                    _rawList = null;
                    Common.Log("SendRawTransactions", $"Step 1: {_ex}", LogLevel.ERROR);
                }
                if (_rawList == null || _rawList.Rows.Count == 0) { continue; }
                #endregion

                #region Step 2: 出账交易锁定
                foreach (DataRow _row in _rawList.Rows)
                {
                    try
                    {
                        using (DbCommon _db = Common.DbCommonMain)
                        {
                            _db.Open();
                            _tsql = new TSQL(TSQLType.Update, "wallet_sendraw");
                            _tsql.Fields.Add("status", "", 1);
                            _tsql.Wheres.And("id", "=", _row["id"]);
                            _tsql.Wheres.And("status", "=", 0);
                            _tsql.Wheres.And("chain", "=", Chain);
                            if (_db.Execute(_tsql.ToSqlCommand()) != 1) { throw new Exception($"Update sendraw failed. ({_row["id"]})"); }

                            JObject _result = new JObject {
                                ["jsonrpc"] = "1.0",
                                ["id"] = "1",
                                ["method"] = "sendrawtransaction",
                                ["params"] = new JArray(_row["raw"].ToString())
                            };
                            _result = Call(_result);
                            if (_result["error"].Type != JTokenType.Null) { throw new Exception(_result["error"]["message"].Value<string>()); }

                            string _txid = _result["result"].Value<string>();

                            _tsql = new TSQL(TSQLType.Update, "wallet_sendraw");
                            _tsql.Fields.Add("status", "", 2);
                            _tsql.Fields.Add("txid", "", _txid);
                            _tsql.Fields.Add("tx_at", "", DateTime.UtcNow);
                            _tsql.Wheres.And("id", "=", _row["id"]);
                            _tsql.Wheres.And("status", "=", 1);
                            _tsql.Wheres.And("chain", "=", Chain);
                            if (_db.Execute(_tsql.ToSqlCommand()) != 1) { throw new Exception($"Update sendraw failed. ({_row["id"]})"); }

                            Common.Log("SendRawTransactions", $"Step 2: {_row["id"]} {_txid}");
                        }
                    }
                    catch (Exception _ex)
                    {
                        Common.Log("SendRawTransactions", $"Step 2: {_ex}", LogLevel.ERROR);
                        continue;
                    }
                }
                #endregion
            }
        }
        #endregion

        #region ListTransactionsGet
        private static void ListTransactionsGet(object _state)
        {
            int _thread = (int)_state;
            while (Common.Running)
            {
                Thread.Sleep(10);
                if (ListTxQueue.Count <= 0) { continue; }
                if (!ListTxQueue.TryDequeue(out string _txid)) { continue; }
                if (_txid == null || _txid == "") { Common.Log("ListTransactionsGet", $"{_thread} - {_txid}", LogLevel.ERROR); continue; }

                JArray _result = null;
                try
                {
                    _result = GetTransaction(_txid,out string _blockhash,out long _blocktime,out int _confirm);
                }
                catch (Exception _ex)
                {
                    Common.Log("ListTransactionsGet", $"{_thread} - {_txid} - {_ex}", LogLevel.ERROR);
                }
                ListRxQueue.Enqueue(_result);
            }
        }
        #endregion

        #region GetTransaction
        private static JArray GetTransaction(string _txid, out string _blockhash, out long _blocktime, out int _confirmations)
        {
            _blockhash = "";
            _blocktime = 0;
            _confirmations = 0;

            JObject _result = new JObject
            {
                ["jsonrpc"] = "1.0",
                ["id"] = "1",
                ["method"] = "getrawtransaction",
                ["params"] = new JArray(_txid, 1)
            };

            try
            {
                _result = Call(_result);
                _result = (JObject)_result["result"];
            }
            catch (JsonReaderException)
            {
                Common.Log("GetTransaction", $"{_txid} {_result}", LogLevel.WARN);
                return null;
            }

            int _lockTime = _result.ContainsKey("locktime") ? _result["locktime"].Value<int>() : 0;
            _confirmations = _result.ContainsKey("confirmations") ? _result["confirmations"].Value<int>() : 0;
            if (_lockTime != 0) { _confirmations = 0; }

            _blockhash = _result["blockhash"].Value<string>();
            _blocktime = _result["blocktime"].Value<long>();

            JArray _txList = new();
            JArray _outs = (JArray)_result["vout"];
            foreach (JObject _out in _outs)
            {
                decimal _value = _out["value"].Value<decimal>();
                if (_value <= 0M) { continue; }

                string _address = "";
                if (_out["scriptPubKey"].Value<JObject>().ContainsKey("address")) { _address = _out["scriptPubKey"]["address"].Value<string>(); }
                if (_out["scriptPubKey"].Value<JObject>().ContainsKey("addresses") && _out["scriptPubKey"]["addresses"].Value<JArray>().Count == 1) { _address = _out["scriptPubKey"]["addresses"][0].Value<string>(); }
                if (_address == "") { continue; }

                JObject _item = new JObject();
                _item["txid"] = _txid.ToLower();
                _item["txindex"] = _out["n"].Value<int>();
                _item["address"] = _address;
                _item["amount"] = _value.ToString();
                _txList.Add(_item);
            }

            return _txList;
        }
        #endregion

        #region Call
        private static JObject Call(JObject _json)
        {
            HttpClient _http = new HttpClient(10000);
            _http.BeginResponse("POST", Host, "");
            _http.Request.Credentials = new NetworkCredential(User, Pass);
            _http.Request.ContentType = "application/json";
            _http.EndResponse(Encoding.UTF8.GetBytes(_json.ToString(Formatting.None)));
            string _result = _http.GetResponseString(Encoding.UTF8);
            return JObject.Parse(_result);
        }
        #endregion
    }
}
