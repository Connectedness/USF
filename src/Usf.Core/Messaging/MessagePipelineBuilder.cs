using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace Usf.Core.Messaging;

public sealed class MessagePipelineBuilder
{
    private readonly List<Func<MessageDelegate, MessageDelegate>> _components = [];

    public MessagePipelineBuilder Use(Func<MessageDelegate, MessageDelegate> middleware)
    {
        _components.Add(middleware ?? throw new ArgumentNullException(nameof(middleware)));
        return this;
    }

    public MessagePipelineBuilder Use(
        Func<IncomingMessageContext, MessageDelegate, Task> middleware
    )
    {
        if (middleware is null)
        {
            throw new ArgumentNullException(nameof(middleware));
        }

        return Use(next => context => middleware(context, next));
    }

    public MessagePipelineBuilder UseMiddleware<TMiddleware>()
        where TMiddleware : class, IMessageMiddleware
    {
        return Use(
            next => async context =>
            {
                var middleware = context.Services.GetRequiredService<TMiddleware>();
                await middleware.InvokeAsync(context, next).ConfigureAwait(false);
            }
        );
    }

    public MessageDelegate Build(MessageDelegate terminal)
    {
        if (terminal is null)
        {
            throw new ArgumentNullException(nameof(terminal));
        }

        var app = terminal;

        for (var i = _components.Count - 1; i >= 0; i--)
        {
            app = _components[i](app);
        }

        return app;
    }
}
