using System.Threading.Tasks;

namespace Usf.Core.Messaging;

public delegate Task MessageDelegate(IncomingMessageContext context);
