using System;
using System.Threading;
using Lion.Data;
using Lion.Data.MySqlClient;
using CCW.Core;

namespace CCW.Service
{
    // Centralized Coin Wallet

    class Program
    {
        static void Main(string[] _args)
        {
            ThreadPool.SetMinThreads(300, 300);
            Common.Init(_args);
            Common.Log("Sys", "Starting");
            Console.CancelKeyPress += Console_CancelKeyPress;

            if (Common.Settings.ContainsKey("NodeBTC")) { NodeBTC.Init(); Thread.Sleep(500); }
            if (Common.Settings.ContainsKey("NodeETH")) { NodeETH.Init(); Thread.Sleep(500); }

            Common.Log("Sys", "Started");
            while (Common.Running) { Thread.Sleep(100); }
        }

        #region Exit
        private static void Exit() { Common.Running = false; }
        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e) { Program.Exit(); }
        #endregion  
    }
}
