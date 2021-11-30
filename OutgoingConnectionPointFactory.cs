using ESB_ConnectionPoints.PluginsInterfaces;
using System;
using System.Collections.Generic;

namespace VSMPO.ESB_Plugins.tst_02
{
    public sealed class OutgoingConnectionPointFactory : IOutgoingConnectionPointFactory
    {
        public const string TIME_DELAY_PARAMETER = "Time delay";
        public IOutgoingConnectionPoint Create(Dictionary<string, string> parameters, IServiceLocator serviceLocator)
        {
            if (!parameters.ContainsKey(TIME_DELAY_PARAMETER))
            {
                throw new ArgumentException(String.Format("Не задан параметр <{0}>", TIME_DELAY_PARAMETER));
            }

            int time_delay = Convert.ToInt32(parameters[TIME_DELAY_PARAMETER]);
            if (time_delay == 0)
            {
                time_delay = 5;
            }

            return new OutgoingConnectionPointBatch(time_delay, serviceLocator);
        }
    }
}
