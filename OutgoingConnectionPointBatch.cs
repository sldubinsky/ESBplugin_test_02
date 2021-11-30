using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using ESB_ConnectionPoints.PluginsInterfaces;
using System.Xml.Linq;

namespace VSMPO.ESB_Plugins.tst_02
{
    public class OutgoingConnectionPointBatch : IStandartOutgoingConnectionPoint
    {
        private ILogger _logger;
        private int _timeDelay;
        private DateTime _oldTime;
        private BatchMessage _batchMessage;
        private readonly HashSet<Guid> _processedMessages = new HashSet<Guid>();
        private TimeSpan _timeSpan = TimeSpan.FromMinutes(1);

        public OutgoingConnectionPointBatch(int time_delay, IServiceLocator serviceLocator)
        {
            _timeDelay = time_delay;
            _logger = serviceLocator.GetLogger(GetType());
            _batchMessage = new BatchMessage();
        }

        public void Dispose()
        {
        }

        void IStandartOutgoingConnectionPoint.Cleanup()
        {
        }

        void IStandartOutgoingConnectionPoint.Initialize()
        {
            _oldTime = DateTime.Now;
            _logger.Info(String.Format("Initialize; delay={0} minutes", _timeDelay));
        }

        void IStandartOutgoingConnectionPoint.Run(IMessageSource messageSource, IMessageReplyHandler replyHandler, CancellationToken ct)
        {
            _logger.Info(String.Format("Start running {0}", DateTime.Now.ToString()));
            while (!ct.IsCancellationRequested)
            {
                if (_oldTime > DateTime.Now)
                {
                    ct.WaitHandle.WaitOne(_timeSpan);
                    continue;
                }

                // Сработал тик, обновим метку времени
                _logger.Debug(string.Format("Tick {0}", _oldTime.ToString()));
                _oldTime = DateTime.Now.AddMinutes(_timeDelay);
                _logger.Debug(string.Format("Next tick {0}", _oldTime.ToString()));

                _batchMessage.Timestamp = DateTime.Now;
                Dictionary<Guid, BatchMessageLine> BatchMessageLines = new Dictionary<Guid, BatchMessageLine>();

                // выберем всё из очереди и закинем в массив батча
                while (true)
                {
                    Message message = messageSource.PeekLockMessage(ct, 1000);
                    if (message == null)
                    {
                        // выбрали всё из очереди
                        break;
                    }
                    if (_processedMessages.Contains(message.Id))
                    {
                        _logger.Debug(string.Format("Повторная блокировка сообщения <{0}>", message.Id));
                        continue;
                    }
                    var newLine = new BatchMessageLine();
                    newLine.Id = message.Id;
                    newLine.Body = message.Body;

                    BatchMessageLines.Add(message.Id, newLine);
                }
                _batchMessage.lines = BatchMessageLines.Values.ToArray();

                if (BatchMessageLines.Count == 0)
                {
                    _logger.Debug(string.Format("Нет новых сообщений на момент {0}", DateTime.Now.ToString()));
                    ct.WaitHandle.WaitOne(_timeSpan);
                    continue;
                }

                _logger.Debug(string.Format("Количества: lines.Count()={0}; BatchMessageLines.Count={1}", _batchMessage.lines.Count(), BatchMessageLines.Count));

                Message newMessage = new Message
                {
                    Id = Guid.NewGuid(),
                    Type = "batch message",
                    Body = Encoding.UTF8.GetBytes(_batchMessage.DoSerialize()),
                    Source = "none",
                    Receiver = null
                };

                try
                {
                    while (!ct.IsCancellationRequested)
                    {
                        if (replyHandler.HandleReplyMessage(newMessage))
                        {
                            break;
                        }
                        ct.WaitHandle.WaitOne(TimeSpan.FromSeconds(1));
                    }
                }
                catch (MessageHandlingException ex)
                {
                    _logger.Error(string.Format("Возникла ошибка при обработке ответного сообщения {0}", newMessage), ex);
                }

                // подтвердим сообщения в очереди
                foreach (Guid _id in BatchMessageLines.Keys)
                {
                    messageSource.CompletePeekLock(_id);
                }

                // зачистим
                BatchMessageLines.Clear();

                ct.WaitHandle.WaitOne(_timeSpan);
            }
        }
    }

    public class BatchMessage
    {
        public DateTime Timestamp;
        public string Description;
        public BatchMessageLine[] lines;

        public BatchMessage()
        {
            this.Timestamp = DateTime.Now;
            this.Description = "Новый батч, по таймеру";
            this.lines = new BatchMessageLine[0];
        }

        public string DoSerialize()
        {
            string Result;

            //Создание новой xml
            XDocument xDocument = new XDocument();
            //Корневой элемент новой xml
            XElement root = new XElement("BatchMessage");
            //Создаем элементы дерева
            root.Add(new XElement("Type"));
            root.Add(new XElement("MessagesQty"));
            root.Add(new XElement("Messages"));
            root.Element("MessagesQty").Value = lines.Length.ToString();

            foreach(BatchMessageLine line in lines)
            {
                XElement xMessage = new XElement("Message", new XAttribute("originalId", line.Id));
                xMessage.Add(XElement.Parse(Encoding.UTF8.GetString(line.Body)));

                root.Element("Messages").Add(xMessage);
            }

            xDocument.Add(root);
            Result = xDocument.ToString();

            return Result;
        }
    }

    public class BatchMessageLine
    {
        public Guid Id;
        public byte[] Body;
    }
}
